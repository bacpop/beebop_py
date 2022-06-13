from importlib.metadata import files
from flask import Flask, jsonify, request
from flask_expects_json import expects_json
from waitress import serve
import json
from redis import Redis
import redis.exceptions as redis_exceptions
from rq import Queue
from rq.job import Job
import os

from beebop import versions
from beebop import assignClusters
from beebop.filestore import PoppunkFileStore
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
        return True
    except (redis_exceptions.ConnectionError, ConnectionRefusedError):
        return jsonify(response_failure("Connection to redis failed"))


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
    run poppunks assing_query()
    input: multiple sketches in json format
    output: clusters assigned to sketches
    """
    sketches = request.json['sketches'].items()
    project_hash = request.json['projectHash']
    return run_poppunk_internal(sketches, project_hash, storageLocation, redis)


def run_poppunk_internal(sketches, project_hash, storageLocation, redis):
    # create FS
    fs = PoppunkFileStore(storageLocation)
    # store json sketches in storage
    hashes_list = []
    for key, value in sketches:
        hashes_list.append(key)
        fs.input.put(key, value)
    # check connection to redis
    check = check_connection(redis)
    if check:
        # submit list of hashes to redis worker
        q = Queue(connection=redis)
        job = q.enqueue(assignClusters.get_clusters,
                        hashes_list, project_hash, fs)
        # save p-hash with job.id in redis server
        redis.hset("beebop:hash:job", project_hash, job.id)
        return job.id
    else:
        return check


# get job status
@app.route("/status/<hash>")
def get_status(hash):
    return get_status_internal(hash, redis)


def get_status_internal(hash, redis):
    check = check_connection(redis)
    if check:
        try:
            id = redis.hget("beebop:hash:job", hash).decode("utf-8")
            job = Job.fetch(id, connection=redis)
            return jsonify(response_success(job.get_status()))
        except AttributeError:
            return jsonify(response_failure("Unknown project hash"))
    else:
        return check


# get job result
@app.route("/result/<hash>")
def get_result(hash):
    return get_result_internal(hash, redis)


def get_result_internal(hash, redis):
    check = check_connection(redis)
    if check:
        try:
            id = redis.hget("beebop:hash:job", hash).decode("utf-8")
            job = Job.fetch(id, connection=redis)
            if job.result is None:
                return jsonify(response_failure("Result not ready yet"))
            else:
                return jsonify(response_success(job.result))
        except AttributeError:
            return jsonify(response_failure("Unknown project hash"))
    else:
        return check


if __name__ == "__main__":
    serve(app)
