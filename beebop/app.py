from flask import Flask, jsonify, request, abort, send_file
from flask_expects_json import expects_json
from waitress import serve
from redis import Redis
import redis.exceptions as redis_exceptions
from rq import Queue
from rq.job import Job, Dependency
import os
from io import BytesIO
import zipfile
import json
import requests
import pickle

from beebop import versions, assignClusters, visualise
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from beebop.utils import get_args, get_cluster_num
from PopPUNK.sketchlib import getKmersFromReferenceDatabase
import beebop.schemas
schemas = beebop.schemas.Schema()

redis_host = os.environ.get("REDIS_HOST")
if not redis_host:
    redis_host = "127.0.0.1"
app = Flask(__name__)
redis = Redis(host=redis_host)
job_timeout = 1200

storage_location = os.environ.get('STORAGE_LOCATION')
dbs_location = os.environ.get('DBS_LOCATION')


def response_success(data) -> dict:
    """
    :param data: [data to be stored in response object]
    :return dict: [response object for successful response holding data]
    """
    response = {
        "status": "success",
        "errors": [],
        "data": data
    }
    return response


def response_failure(error) -> dict:
    """
    :param error: [error message]
    :return dict: [response object for error response holding error message]
    """
    response = {
        "status": "failure",
        "errors": [error],
        "data": []
    }
    return response


def check_connection(redis) -> None:
    """
    :param redis: [Redis instance]
    """
    try:
        redis.ping()
    except (redis_exceptions.ConnectionError, ConnectionRefusedError):
        abort(500, description="Redis not found")


def generate_zip(fs: PoppunkFileStore,
                 p_hash: str,
                 type: str,
                 cluster: str) -> BytesIO:
    """
    [This generates a .zip folder with results data.]

    :param fs: [PoppunkFileStore with path to folder to be zipped]
    :param p_hash: [project hash]
    :param type: [can be either 'microreact' or 'network']
    :param cluster: [only relevant for 'network', since there are multiple
        component files stored in the folder, but only the right one should
        be included in the zip folder. For 'microreact' this can be None, as
        the cluster information is already included in the path]
    :return BytesIO: [memory file]
    """
    memory_file = BytesIO()
    cluster_num = get_cluster_num(cluster)
    if type == 'microreact':
        path_folder = fs.output_microreact(p_hash, cluster_num)
        add_files(memory_file, path_folder)
    elif type == 'network':
        path_folder = fs.output_network(p_hash)
        file_list = (f'network_component_{cluster_num}.graphml',
                     'network_cytoscape.csv')
        add_files(memory_file, path_folder, file_list)
    memory_file.seek(0)
    return memory_file


def add_files(memory_file: BytesIO,
              path_folder: str,
              file_list: list = None) -> BytesIO:
    """
    [Add files in specified folder to a memory_file.
    If filelist is provided, only files in this list are added,
    otherwise all files in the folder will be included]

    :param memory_file: [empty memory file to add files to]
    :param path_folder: [path to folder with files to include]
    :param file_list: [optional, if only specific files in folder should
        be included]
    :return BytesIO: [memory file with added files]
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
def internal_server_error(e) -> json:
    """
    :param e: [error]
    :return json: [error response object]
    """
    return jsonify(error=response_failure({"error": "Internal Server Error",
                                           "detail": str(e)})), 500


@app.errorhandler(404)
def resource_not_found(e) -> json:
    """
    :param e: [error]
    :return json: [error response object]
    """
    return jsonify(error=response_failure({"error": "Resource not found",
                                           "detail": str(e)})), 404


@app.route('/version')
def report_version() -> json:
    """
    [report version of beebop and poppunk (and ska in the future)
    wrapped in response object]

    :return json: [response that stores version infos in 'data']
    """
    vers = versions.get_version()
    return jsonify(response_success(vers))


@app.route("/speciesConfig", methods=['GET'])
def get_species_config() -> json:
    """
    Retrieves k-mer lists for all species specified in the arguments.
    This function extracts species arguments,
    fetches k-mers from the reference database for each species,
    and constructs a configuration dictionary
    containing the k-mers for each species.The result is then
    returned as a JSON response.

    :return json: [JSON response containing a dictionary
        where each key is a species and the value is another
        dictionary with a list of k-mers for that species.]
    """
    all_species_args = vars(get_args().species)
    species_config = {
        species: get_species_kmers(args.refdb)
        for species, args in all_species_args.items()
    }
    return jsonify(response_success(species_config))


def get_species_kmers(species_db_name: str) -> dict:
    """
    Retrieve k-mer information from database for a given species.

    :param species_db_name: [The name of the species database.]
    :return dict: [A dictionary containing the maximum, minimum, and step
        k-mer values.]
    """
    kmers = getKmersFromReferenceDatabase(f"{dbs_location}/{species_db_name}")
    return {
        "kmerMax": int(kmers[-1]),
        "kmerMin": int(kmers[0]),
        "kmerStep": int(kmers[1] - kmers[0]),
    }


@app.route('/poppunk', methods=['POST'])
@expects_json(schemas.sketches)
def run_poppunk() -> json:
    """
    [run poppunks assing_query() and generate_visualisations().
    input: multiple sketches in json format together with project hash
    and filename mapping, schema can be found in spec/sketches.schema.json]

    :return json: [response object with all job IDs stored in 'data']
    """
    sketches = request.json['sketches'].items()
    p_hash = request.json['projectHash']
    name_mapping = request.json['names']
    species = request.json["species"]
    q = Queue(connection=redis)
    return run_poppunk_internal(sketches, p_hash, name_mapping,
                                storage_location, redis, q, species)


def run_poppunk_internal(sketches: dict,
                         p_hash: str,
                         name_mapping: dict,
                         storage_location: str,
                         redis: Redis,
                         q: Queue, species: str) -> json:
    """
    [Runs all poppunk functions we are interested in on the provided sketches.
    These are clustering with poppunk_assign, and creating visualisations
    (microreact and network) with poppunk_visualise. In future, also lineage
    assignment and QC should be triggered from this endpoint.]

    :param sketches: [all sketches in json format]
    :param p_hash: [project hash]
    :param name_mapping: [maps filehashes to filenames for all query
        samples]
    :param storage_location: [path to storage location]
    :param redis: [Redis instance]
    :param q: [redis queue]
    :param species: [type of species to be analyzed]
    :return json: [response object with all job IDs stored in 'data']
    """
    fs = PoppunkFileStore(storage_location)
    args = get_args()
    species_args = getattr(args.species, species, None)
    if not species_args:
        return (
            jsonify(
                error=response_failure(
                    {
                        "error": "Species not found",
                        "detail": f"No database found for species: {species}",
                    }
                )
            ),
            400,
        )

    # pass in both full and refs to assign
    ref_db_fs = DatabaseFileStore(
        f"{dbs_location}/{species_args.refdb}",
        species_args.external_clusters_file,
    )
    full_db_fs = DatabaseFileStore(
        f"{dbs_location}/{species_args.fulldb}",
        species_args.external_clusters_file,
        species_args.db_metadata_file,
    )

    # store json sketches in storage, and store an initial output_cluster file
    # to record sample hashes for the project
    hashes_list = []
    initial_output = {}
    for i, (key, value) in enumerate(sketches):
        hashes_list.append(key)
        fs.input.put(key, value)
        initial_output[i] = {
            "hash": key
        }
    fs.ensure_output_dir_exists(p_hash)
    with open(fs.output_cluster(p_hash), 'wb') as f:
        pickle.dump(initial_output, f)
    # check connection to redis
    check_connection(redis)
    # keep results forever
    queue_kwargs = {
          "job_timeout": job_timeout,
          "result_ttl": -1,
          "failure_ttl": -1
        }
    # submit list of hashes to redis worker
    job_assign = q.enqueue(assignClusters.get_clusters,
                           hashes_list,
                           p_hash,
                           fs,
                           ref_db_fs,
                           full_db_fs,
                           args,
                           species,
                           **queue_kwargs)
    # save p-hash with job.id in redis server
    redis.hset("beebop:hash:job:assign", p_hash, job_assign.id)
    # create visualisations
    # network
    job_network = q.enqueue(visualise.network,
                            args=(p_hash,
                                  fs,
                                  full_db_fs,
                                  args,
                                  name_mapping,
                                  species),
                            depends_on=job_assign, **queue_kwargs)
    redis.hset("beebop:hash:job:network", p_hash, job_network.id)
    # microreact
    # delete all previous microreact cluster job results for this project
    redis.delete(f"beebop:hash:job:microreact:{p_hash}")
    job_microreact = q.enqueue(
        visualise.microreact,
        args=(
            p_hash,
            fs,
            full_db_fs,
            args,
            name_mapping,
            species,
            redis_host,
            queue_kwargs,
        ),
        depends_on=Dependency([job_assign, job_network], allow_failure=True),
        **queue_kwargs,
    )
    redis.hset("beebop:hash:job:microreact", p_hash, job_microreact.id)
    return jsonify(
        response_success(
            {
                "assign": job_assign.id,
                "microreact": job_microreact.id,
                "network": job_network.id,
            }
        )
    )


# get job status
@app.route("/status/<p_hash>")
def get_status(p_hash) -> json:
    """
    [returns job statuses for all jobs with given project hash. Possible
    values are: queued, started, deferred, finished, stopped, canceled,
    scheduled and failed]

    :param p_hash: [project hash]
    :return json: [response object with job statuses]
    """
    return get_status_response(p_hash, redis)


def get_status_response(p_hash: str, redis: Redis) -> json:
    """
    [returns jsonified response of all job statuses for a project]

    :param p_hash: [project hash]
    :param redis: [Redis instance]
    :return json: [response object with job statuses]
    """
    response = get_status_internal(p_hash, redis)
    if "error" in response:
        return jsonify(error=response_failure(response)), 500
    else:
        return jsonify(response_success(response))


def get_status_internal(p_hash: str, redis: Redis) -> dict:
    """
    [returns statuses of all jobs from a given project (cluster assignment,
    microreact and network visualisations)]

    :param p_hash: [project hash]
    :param redis: [Redis instance]
    :return: [dict with job statuses]
    """
    check_connection(redis)

    def get_status_job(job, p_hash, redis):
        id = redis.hget(f"beebop:hash:job:{job}", p_hash).decode("utf-8")
        return Job.fetch(id, connection=redis).get_status()
    try:
        status_assign = get_status_job('assign', p_hash, redis)
        if status_assign == "finished":
            status_microreact = get_status_job('microreact', p_hash, redis)
            status_network = get_status_job('network', p_hash, redis)
            microreact_cluster_statuses = {
                cluster.decode("utf-8"): Job.fetch(
                    status.decode("utf-8"), connection=redis
                ).get_status()
                for cluster, status in redis.hgetall(
                    f"beebop:hash:job:microreact:{p_hash}"
                ).items()
            }
        else:
            status_microreact = "waiting"
            status_network = "waiting"
            microreact_cluster_statuses = {}
        return {"assign": status_assign,
                "microreact": status_microreact,
                "network": status_network,
                "microreactClusters": microreact_cluster_statuses}
    except AttributeError:
        return {"error": "Unknown project hash"}


@app.route("/results/networkGraphs/<p_hash>", methods=['GET'])
def get_network_graphs(p_hash) -> json:
    """
    [returns all network graphml files for a given project hash]

    :param p_hash: [project hash]
    :return json: [response object with all graphml files stored in 'data']
    """
    fs = PoppunkFileStore(storage_location)
    try:
        cluster_result = get_cluster_assignments(p_hash, storage_location)
        graphmls = {}
        for cluster_info in cluster_result.values():
            cluster = cluster_info["cluster"]
            path = fs.pruned_network_output_component(
                p_hash, get_cluster_num(cluster)
            )
            with open(path, "r") as graphml_file:
                graph = graphml_file.read()
            graphmls[cluster] = graph
        return jsonify(response_success(graphmls))

    except KeyError:
        f = (
            jsonify(
                error=response_failure(
                    {
                        "error": "Cluster not found",
                        "detail": "Cluster not found",
                    }
                )
            ),
            500,
        )

    except FileNotFoundError:
        return (
            jsonify(
                error=response_failure(
                    {
                        "error": "File not found",
                        "detail": "GraphML files not found",
                    }
                )
            ),
            404,
        )


# get job result
@app.route("/results/<result_type>", methods=['POST'])
def get_results(result_type) -> json:
    """
    [Route to get results for the specified type of analysis.
    Request object includes:
        project_hash
        type - only for 'zip' results, can be 'microreact' or 'network'
        cluster - for 'zip', 'microreact' and 'graphml' results
        api_token - only required for  'microreact' URL generation. This
        must be provided by the user in the frontend]

    :param result_type: [can be
        - 'assign' for clusters
        - 'zip' for visualisation results as zip folders (with the json
            property 'type' specifying whether 'microreact' or 'network'
            results are required)
        - 'microreact' for the microreact URL for a given cluster
        - 'graphml' for the content of the .graphml file for the specified
            cluster]
    :return json: [response object with result stored in 'data']
    """
    if result_type == 'assign':
        p_hash = request.json['projectHash']
        return get_clusters_json(p_hash, storage_location)
    elif result_type == 'zip':
        p_hash = request.json['projectHash']
        visualisation_type = request.json['type']
        cluster = str(request.json['cluster'])
        return send_zip_internal(p_hash,
                                 visualisation_type,
                                 cluster,
                                 storage_location)
    elif result_type == 'microreact':
        microreact_api_new_url = "https://microreact.org/api/projects/create"
        p_hash = request.json['projectHash']
        cluster = str(request.json['cluster'])
        api_token = str(request.json['apiToken'])
        return generate_microreact_url_internal(microreact_api_new_url,
                                                p_hash,
                                                cluster,
                                                api_token,
                                                storage_location)


def get_cluster_assignments(p_hash: str, storage_location: str) -> dict:
    """
    [returns cluster assignment results]

    :param p_hash: [project hash]
    :param storage_location: [storage location]
    :return dict: [cluster results]
    """
    fs = PoppunkFileStore(storage_location)
    with open(fs.output_cluster(p_hash), 'rb') as f:
        cluster_result = pickle.load(f)
        return cluster_result


def get_clusters_json(p_hash: str, storage_location: str) -> json:
    """
    [returns cluster assignment results as json response]

    :param p_hash: [project hash]
    :param storage_location: [storage location]
    :return json: [response object with cluster results stored in 'data']
    """
    cluster_result = get_cluster_assignments(p_hash, storage_location)
    cluster_dict = {value['hash']: value for value in cluster_result.values()}
    failed_samples = get_failed_samples_internal(p_hash, storage_location)

    return jsonify(response_success({**cluster_dict, **failed_samples}))


def send_zip_internal(p_hash: str,
                      type: str,
                      cluster: str,
                      storage_location: str) -> any:
    """
    [Generates a zipfile with visualisation results and returns zipfile]

    :param p_hash: [project hash]
    :param type: [either 'microreact' or 'network']
    :param cluster: [cluster number]
    :param storage_location: [storage location]
    :return any: [zipfile]
    """
    fs = PoppunkFileStore(storage_location)
    # generate zipfile
    memory_file = generate_zip(fs, p_hash, type, cluster)
    return send_file(memory_file,
                     download_name=type + '.zip',
                     as_attachment=True)


def generate_microreact_url_internal(microreact_api_new_url: str,
                                     p_hash: str,
                                     cluster: str,
                                     api_token: str,
                                     storage_location: str) -> json:
    """
    [Generates Microreact URL to a microreact project with the users data
    already being uploaded.]

    :param microreact_api_new_url: [URL where the microreact API can be
        accessed]
    :param p_hash: [project hash]
    :param cluster: [cluster number]
    :param api_token: [this ust be provided by the user. The new API does
        not allow generating a URL without a token.]
    :param storage_location: [storage location]
    :return json: [response object with URL stored in 'data']
    """
    fs = PoppunkFileStore(storage_location)

    cluster_num = get_cluster_num(cluster)
    path_json = fs.microreact_json(p_hash, cluster_num)

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


@app.route("/project/<p_hash>", methods=['GET'])
def get_project(p_hash) -> json:
    """
    [Loads all project data for a given project hash so the project can be
    re-opened in beebop.]

    :param p_hash: [identifying hash for the project]
    :return: [project data]
    """
    job_id = redis.hget("beebop:hash:job:assign", p_hash)
    if job_id is None:
        return jsonify(error=response_failure({
            "error": "Project hash not found",
            "detail": "Project hash does not have an associated job"
        })), 404

    status = get_status_internal(p_hash, redis)

    if "error" in status:
        return jsonify(error=response_failure(status)), 500
    else:
        clusters_result = get_cluster_assignments(p_hash, storage_location)
        failed_samples = get_failed_samples_internal(p_hash, storage_location)

        fs = PoppunkFileStore(storage_location)
        passed_samples = {}
        for value in clusters_result.values():
            sample_hash = value["hash"]
            sketch = fs.input.get(sample_hash)
            passed_samples[sample_hash] = {
                       "hash": sample_hash,
                       "sketch": sketch
                     }
            # Cluster may not have been assigned yet
            passed_samples[sample_hash]["cluster"] = value.get("cluster")

        return jsonify(response_success({
            "hash": p_hash,
            "samples": {**passed_samples, **failed_samples},
            "status": status
        }))


def get_failed_samples_internal(p_hash: str,
                                storage_location: str
                                ) -> dict[str, dict]:
    """
    [Returns a dictionary of failed samples for a given project hash]

    :param p_hash (str): The hash of the samples to retrieve.
    :param storage_location (str): The location of the storage.

    :return dict[str, dict]: failed samples
    containing hash and reasons for failure.
    """
    fs = PoppunkFileStore(storage_location)
    qc_report_file_path = fs.output_qc_report(p_hash)
    failed_samples = {}
    if os.path.exists(qc_report_file_path):
        with open(fs.output_qc_report(p_hash), "r") as f:
            for line in f:
                hash, reasons = line.strip().split("\t")
                failed_samples[hash] \
                    = {"failReasons": reasons.split(","), "hash": hash}
    return failed_samples


if __name__ == "__main__":
    serve(app)  # pragma: no cover
