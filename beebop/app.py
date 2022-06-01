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
from beebop.filestore import FileStore
import beebop.schemas
schemas = beebop.schemas.Schema()


app = Flask(__name__)
redis = Redis()

storageLocation = './storage'
fs_json = FileStore(storageLocation+'/json')


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


@app.route('/version')
def report_version():
    """
    report version of beebop and poppunk (and ska in the future)
    wrapped in response object
    """
    vers = versions.get_version()
    response = response_success(vers)
    response_json = jsonify(response)
    return response_json


@app.route('/poppunk', methods=['POST'])
@expects_json(schemas.sketches)
def run_poppunk():
    """
    run poppunks assing_query()
    input: multiple sketches in json format
    output: clusters assigned to sketches
    """
    # store json sketches in storage
    hashes_list = []
    for key, value in request.json['sketches'].items():
        hashes_list.append(key)
        fs_json.put(key, value)
    # submit list of hashes to redis worker
    q = Queue(connection=redis)
    job = q.enqueue(assignClusters.get_clusters,
                    hashes_list, request.json['projectHash'])
    # save p-hash with job.id in redis server
    redis.hset("beebop:hash:job", request.json['projectHash'], job.id)
    return job.id


# get job status
@app.route("/status/<hash>")
def get_status(hash):
    try:
        redis.ping()
        id = redis.hget("beebop:hash:job", hash).decode("utf-8")
        job = Job.fetch(id, connection=redis)
        return job.get_status()
    except AttributeError:
        return jsonify(response_failure("Unknown project hash"))
    except (redis_exceptions.ConnectionError, ConnectionRefusedError):
        return jsonify(response_failure("Connection to redis failed"))


# get job result
@app.route("/result/<hash>")
def get_result(hash):
    try:
        redis.ping()
        id = redis.hget("beebop:hash:job", hash).decode("utf-8")
        job = Job.fetch(id, connection=redis)
        if job.result is None:
            return jsonify(response_failure("Result not ready yet"))
        else:
            return jsonify(response_success(job.result))
    except AttributeError:
        return jsonify(response_failure("Unknown project hash"))
    except (redis_exceptions.ConnectionError, ConnectionRefusedError):
        return jsonify(response_failure("Connection to redis failed"))


if __name__ == "__main__":
    serve(app)
