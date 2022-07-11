from flask import Flask, jsonify, request, abort
from flask_expects_json import expects_json
from waitress import serve
from redis import Redis
import redis.exceptions as redis_exceptions
from rq import Queue
from rq.job import Job
import os
import json
from types import SimpleNamespace

from beebop import versions, assignClusters, visualise
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
import beebop.schemas
schemas = beebop.schemas.Schema()


app = Flask(__name__)
redis = Redis()

if os.environ.get('TESTING') == 'True':
    storageLocation = './tests/files'
else:
    storageLocation = './storage'


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


@app.errorhandler(500)
def internal_server_error(e):
    return jsonify(error=response_failure(str(e))), 500


@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=response_failure(str(e))), 404


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
    input: multiple sketches in json format
    """
    sketches = request.json['sketches'].items()
    project_hash = request.json['projectHash']
    return run_poppunk_internal(sketches, project_hash, storageLocation, redis)


def run_poppunk_internal(sketches, project_hash, storageLocation, redis):
    # create FS
    fs = PoppunkFileStore(storageLocation)
    # set output directory
    outdir = fs.output(project_hash)
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    # read arguments
    with open("./beebop/resources/args.json") as a:
        args_json = a.read()
    args = json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))
    # set database paths
    db_paths = DatabaseFileStore('./storage/GPS_v4_references')
    # store json sketches in storage
    hashes_list = []
    for key, value in sketches:
        hashes_list.append(key)
        fs.input.put(key, value)
    # check connection to redis
    check_connection(redis)
    # submit list of hashes to redis worker
    q = Queue(connection=redis)
    job_assign = q.enqueue(assignClusters.get_clusters,
                           hashes_list, fs, outdir, db_paths, args)
    # save p-hash with job.id in redis server
    redis.hset("beebop:hash:job", project_hash+'_assign', job_assign.id)
    # create visualisations
    # microreact
    job_microreact = q.enqueue(visualise.microreact,
                               args=(project_hash, outdir, db_paths, args),
                               depends_on=job_assign)
    redis.hset("beebop:hash:job", project_hash+'_microreact',
               job_microreact.id)
    # network
    job_network = q.enqueue(visualise.network,
                            args=(project_hash, outdir, db_paths, args),
                            depends_on=job_assign)
    redis.hset("beebop:hash:job", project_hash+'_network', job_network.id)
    return {"assign": job_assign.id,
            "microreact": job_microreact.id,
            "network": job_network.id}


# get job status
@app.route("/status/<hash>")
def get_status(hash):
    return get_status_internal(hash, redis)


def get_status_internal(hash, redis):
    check_connection(redis)
    try:
        id_assign = redis.hget("beebop:hash:job",
                               hash+'_assign').decode("utf-8")
        job_assign = Job.fetch(id_assign, connection=redis)
        status_assign = job_assign.get_status()
        if status_assign == "finished":
            id_microreact = redis.hget("beebop:hash:job",
                                       hash+'_microreact').decode("utf-8")
            job_microreact = Job.fetch(id_microreact, connection=redis)
            status_microreact = job_microreact.get_status()
            id_network = redis.hget("beebop:hash:job",
                                    hash+'_network').decode("utf-8")
            job_network = Job.fetch(id_network, connection=redis)
            status_network = job_network.get_status()
        else:
            status_microreact = "waiting"
            status_network = "waiting"
        return jsonify(response_success({"assign": status_assign,
                                         "microreact": status_microreact,
                                         "network": status_network}))
    except AttributeError:
        return jsonify(response_failure("Unknown project hash"))


# get job result
@app.route("/result/<hash>")
def get_result(hash):
    return get_result_internal(hash, redis)


def get_result_internal(hash, redis):
    check_connection(redis)
    try:
        id = redis.hget("beebop:hash:job", hash+'_assign').decode("utf-8")
        job = Job.fetch(id, connection=redis)
        if job.result is None:
            return jsonify(response_failure("Result not ready yet"))
        else:
            return jsonify(response_success(job.result))
    except AttributeError:
        return jsonify(response_failure("Unknown project hash"))


if __name__ == "__main__":
    serve(app)  # pragma: no cover
