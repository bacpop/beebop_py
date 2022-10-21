from importlib.resources import path
from flask import Flask, jsonify, request, abort, send_file
from flask_expects_json import expects_json
from waitress import serve
from redis import Redis
import redis.exceptions as redis_exceptions
from rq import Queue
from rq.job import Job
import os
from io import BytesIO
import zipfile
import json
import requests
import pickle

from beebop import versions, assignClusters, visualise
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from beebop.utils import get_args
import beebop.schemas
schemas = beebop.schemas.Schema()

redis_host = os.environ.get("REDIS_HOST")
if not redis_host:
    redis_host = "127.0.0.1"
app = Flask(__name__)
redis = Redis(host=redis_host)
job_timeout = 600

storage_location = os.environ.get('STORAGE_LOCATION')
database_location = os.environ.get('DB_LOCATION')


def response_success(data):
    response = {
        "status": "success",
        "errors": [],
        "data": data
    }
    return response


def response_failure(error):
    response = {
        "status": "failure",
        "errors": [error],
        "data": []
    }
    return response


def check_connection(redis):
    try:
        redis.ping()
    except (redis_exceptions.ConnectionError, ConnectionRefusedError):
        abort(500, description="Redis not found")


def generate_zip(path_folder, type, cluster):
    """
    This generates a .zip folder with results data.

    Arguments:
    path_folder - folder to be zipped
    type - can be either 'microreact' or 'network'
    cluster - only relevant for 'network', since there are multiple
    component files stored in the folder, but only the right one should
    be included in the zip folder
    """
    memory_file = BytesIO()
    if type == 'microreact':
        add_files(memory_file, path_folder)
    elif type == 'network':
        file_list = (f'network_component_{cluster}.graphml',
                     'network_cytoscape.csv',
                     'network_cytoscape.graphml')
        add_files(memory_file, path_folder, file_list)
    memory_file.seek(0)
    return memory_file


def add_files(memory_file, path_folder, file_list=None):
    """
    Add files in specified folder to a memory_file.
    If filelist is provided, only files in this list are added,
    otherwise all files in the folder will be included

    Arguments:
    memory_file - BytesIO object to add files to
    path_folder - path to folder with files to include
    file_list - optional, if only specific files in folder should be included
    """
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path_folder):
            for file in files:
                if file_list:
                    if file in file_list:
                        zipf.write(os.path.join(root, file), arcname=file)
                else:
                    zipf.write(os.path.join(root, file), arcname=file)
    return memory_file


@app.errorhandler(500)
def internal_server_error(e):
    return jsonify(error=response_failure({"error": "Internal Server Error",
                                           "detail": str(e)})), 500


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=response_failure({"error": "Resource not found",
                                           "detail": str(e)})), 404


@app.route('/version')
def report_version():
    """
    report version of beebop and poppunk (and ska in the future)
    wrapped in response object
    """
    vers = versions.get_version()
    return jsonify(response_success(vers))


@app.route('/poppunk', methods=['POST'])
@expects_json(schemas.sketches)
def run_poppunk():
    """
    run poppunks assing_query() and generate_visualisations().
    input: multiple sketches in json format together with project hash
    and filename mapping, schema can be found in spec/sketches.schema.json
    """
    sketches = request.json['sketches'].items()
    project_hash = request.json['projectHash']
    name_mapping = request.json['names']
    q = Queue(connection=redis)
    return run_poppunk_internal(sketches, project_hash, name_mapping,
                                storage_location, redis, q)


def run_poppunk_internal(sketches,
                         project_hash,
                         name_mapping,
                         storage_location,
                         redis,
                         q):
    """
    Runs all poppunk functions we are interested in on the provided sketches.
    These are clustering with poppunk_assign, and creating visualisations
    (microreact and network) with poppunk_visualise. In future, also lineage
    assignment and QC should be triggered from this endpoint.

    Arguments:
    sketches - all sketches in json format
    project_hash
    name_mapping - maps filehashes to filenames for all query samples
    storage_location
    redis - Redis instance
    q - redis queue
    
    """
    # create FS
    fs = PoppunkFileStore(storage_location)
    # read arguments
    args = get_args()
    # set database paths
    db_paths = DatabaseFileStore(database_location)
    # store json sketches in storage
    hashes_list = []
    for key, value in sketches:
        hashes_list.append(key)
        fs.input.put(key, value)
    # check connection to redis
    check_connection(redis)
    # submit list of hashes to redis worker
    job_assign = q.enqueue(assignClusters.get_clusters,
                           hashes_list,
                           project_hash,
                           fs,
                           db_paths,
                           args,
                           job_timeout=job_timeout)
    # save p-hash with job.id in redis server
    redis.hset("beebop:hash:job:assign", project_hash, job_assign.id)
    # create visualisations
    # microreact
    job_microreact = q.enqueue(visualise.microreact,
                               args=(project_hash,
                                     fs,
                                     db_paths,
                                     args,
                                     name_mapping),
                               depends_on=job_assign, job_timeout=job_timeout)
    redis.hset("beebop:hash:job:microreact", project_hash,
               job_microreact.id)
    # network
    job_network = q.enqueue(visualise.network,
                            args=(project_hash,
                                  fs,
                                  db_paths,
                                  args,
                                  name_mapping),
                            depends_on=job_assign, job_timeout=job_timeout)
    redis.hset("beebop:hash:job:network", project_hash, job_network.id)
    return jsonify(response_success({"assign": job_assign.id,
                                     "microreact": job_microreact.id,
                                     "network": job_network.id}))


# get job status
@app.route("/status/<hash>")
def get_status(hash):
    """
    Returns job statuses for jobs with given project hash.
    Possible values are: queued, started, deferred, finished, stopped,
    scheduled, canceled and failed
    """
    return get_status_internal(hash, redis)


def get_status_internal(hash, redis):
    """
    Returns statuses of all jobs from given project (cluster assignment,
    microreact and network visualisations).

    Arguments:
    hash - project hash
    redis - Redis instance
    """
    check_connection(redis)

    def get_status_job(job, hash, redis):
        id = redis.hget(f"beebop:hash:job:{job}", hash).decode("utf-8")
        return Job.fetch(id, connection=redis).get_status()
    try:
        status_assign = get_status_job('assign', hash, redis)
        if status_assign == "finished":
            status_microreact = get_status_job('microreact', hash, redis)
            status_network = get_status_job('network', hash, redis)
        else:
            status_microreact = "waiting"
            status_network = "waiting"
        return jsonify(response_success({"assign": status_assign,
                                         "microreact": status_microreact,
                                         "network": status_network}))
    except AttributeError:
        return jsonify(error=response_failure({
            "error": "Unknown project hash"})), 500


# get job result
@app.route("/results/<type>", methods=['POST'])
def get_results(type):
    """
    Route to get results for the specified type of analysis. These can be
    - 'assign' for clusters
    - 'zip' for visualisation results as zip folders (with the json property
      'type' specifying whether 'microreact' or 'network' results are required)
    - 'microreact' for the microreact URL for a given cluster
    - 'graphml' for the content of the .graphml file for the specified cluster

    Input: json with the following properties:
    project_hash
    type - only for 'zip' results, can be either 'microreact' or 'network'
    cluster - for 'zip', 'microreact' and 'graphml' results
    api_token - only required for  'microreact' URL generation. This must be
    provided by the user in the frontend
    """
    if type == 'assign':
        project_hash = request.json['projectHash']
        return get_clusters_internal(project_hash, redis)
    elif type == 'zip':
        project_hash = request.json['projectHash']
        type = request.json['type']
        cluster = str(request.json['cluster'])
        return send_zip_internal(project_hash, type, cluster, storage_location)
    elif type == 'microreact':
        microreact_api_new_url = "https://microreact.org/api/projects/create"
        project_hash = request.json['projectHash']
        cluster = str(request.json['cluster'])
        api_token = str(request.json['apiToken'])
        return generate_microreact_url_internal(microreact_api_new_url,
                                                project_hash,
                                                cluster,
                                                api_token,
                                                storage_location)
    elif type == 'graphml':
        project_hash = request.json['projectHash']
        cluster = str(request.json['cluster'])
        return download_graphml_internal(project_hash,
                                         cluster,
                                         storage_location)


def get_clusters_internal(hash, redis):
    """
    returns cluster assignment results

    Arguments:
    hash - project hash
    redis - Redis instance
    """
    check_connection(redis)
    try:
        id = redis.hget("beebop:hash:job:assign", hash).decode("utf-8")
        job = Job.fetch(id, connection=redis)
        if job.result is None:
            return jsonify(error=response_failure({
                "error": "Result not ready yet"})), 500
        else:
            return jsonify(response_success(job.result))
    except AttributeError:
        return jsonify(error=response_failure({
            "error": "Unknown project hash"})), 500


def send_zip_internal(project_hash, type, cluster, storage_location):
    """
    Generates a zipfile with visualisation results and returns zipfile

    Arguments:
    project_hash
    type - either 'microreact' or 'network'
    cluster
    storage_location
    """
    fs = PoppunkFileStore(storage_location)
    if type == 'microreact':
        path_folder = fs.output_microreact(project_hash, cluster)
    elif type == 'network':
        path_folder = fs.output_network(project_hash)
    # generate zipfile
    memory_file = generate_zip(path_folder, type, cluster)
    return send_file(memory_file,
                     download_name=type + '.zip',
                     as_attachment=True)


def generate_microreact_url_internal(microreact_api_new_url,
                                     project_hash,
                                     cluster,
                                     api_token,
                                     storage_location):
    """
    Generates Microreact URL to a microreact project with the users data
    already being uploaded. 

    Arguments:
    microreact_api_new_url - URL where the microreact API can be accessed
    project_hash
    cluster
    api_token - this ust be provided by the user. The new API does not allow
    generating a URL without a token. 
    storage_location
    """
    fs = PoppunkFileStore(storage_location)

    path_json = fs.microreact_json(project_hash, cluster)

    with open(path_json, 'rb') as microreact_file:
        json_microreact = json.load(microreact_file)

    # generate URL from microreact API
    headers = {"Content-type": "application/json; charset=UTF-8",
               "Access-Token": api_token}
    r = requests.post(microreact_api_new_url,
                      data=json.dumps(json_microreact),
                      headers=headers)
    if r.status_code == 200:
        url = r.json()['url']
        return jsonify(response_success({"cluster": cluster, "url": url}))
    elif r.status_code == 500:
        return jsonify(error=response_failure({
            "error": "Wrong Token",
            "detail": """
            Microreact reported Internal Server Error.
            Most likely Token is invalid!"""
            })), 500
    elif r.status_code == 404:
        return jsonify(error=response_failure({
            "error": "Resource not found",
            "detail": "Cannot reach Microreact API"
            })), 404
    else:
        return jsonify(error=response_failure({
            "error": "Unknown error",
            "detail": f"""Microreact API returned status code {r.status_code}.
                Response text: {r.text}."""
            })), 500


def download_graphml_internal(project_hash, cluster, storage_location):
    """
    Sends the content of the .graphml file for a specified cluster to the backend
    to be used to draw a network graph. Since ,component numbers are not matching
    with cluster numbers, we must first infer the component number from cluster number
    to locate and send the right .graphml file.

    Arguments:
    project_hash
    cluster
    storage_location
    """
    fs = PoppunkFileStore(storage_location)
    try:
        with open(fs.network_mapping(project_hash), 'rb') as dict:
            cluster_component_mapping = pickle.load(dict)
        component = cluster_component_mapping[str(cluster)]
        path = fs.network_output_component(project_hash, component)
        with open(path, 'r') as graphml_file:
            graph = graphml_file.read()
        f = jsonify(response_success({
            "cluster": cluster,
            "graph": graph}))
    except (FileNotFoundError):
        f = jsonify(error=response_failure({
                "error": "File not found",
                "detail": "GraphML file not found"
                })), 500
    return f


if __name__ == "__main__":
    serve(app)  # pragma: no cover
