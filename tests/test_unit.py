import jsonschema
import json
from PopPUNK import __version__ as poppunk_version
from redis import Redis
from rq import SimpleWorker, Queue
from rq.job import Job
import time
import pytest
from werkzeug.exceptions import InternalServerError
import string
import random
import os
import shutil

from beebop import __version__ as beebop_version
from beebop import app
from beebop import versions
from beebop import assignClusters
from beebop import visualise
from beebop.filestore import PoppunkFileStore, FileStore, DatabaseFileStore
from beebop.utils import get_args
import beebop.schemas


schemas = beebop.schemas.Schema()
storageLocation = './tests/results'
fs = PoppunkFileStore(storageLocation)
db_paths = DatabaseFileStore('./storage/GPS_v4_references')
args = get_args()


def dummy_fct(duration):
    time.sleep(duration)
    return "Result"


def read_data(response):
    return json.loads(response.get_data().decode("utf-8"))


def read_redis(name, key, redis):
    return redis.hget(name, key).decode("utf-8")


def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version},
        {"name": "poppunk", "version": poppunk_version}]
    assert jsonschema.validate(versions.get_version(),
                               schemas.version) is None


def test_assign_clusters():
    hashes_list = [
            '02ff334f17f17d775b9ecd69046ed296',
            '9c00583e2f24fed5e3c6baa87a4bfa4c',
            '99965c83b1839b25c3c27bd2910da00a']
    assert assignClusters.get_clusters(
        hashes_list,
        'unit_test_poppunk_assign',
        fs,
        db_paths,
        args) == {
            0: {'cluster': 9, 'hash': '02ff334f17f17d775b9ecd69046ed296'},
            1: {'cluster': 10, 'hash': '99965c83b1839b25c3c27bd2910da00a'},
            2: {'cluster': 41, 'hash': '9c00583e2f24fed5e3c6baa87a4bfa4c'}}


def test_microreact(mocker):
    class mock_job():
        def __init__(self, result):
            self.dependency = {"result": result}
    assign_result = {0: {'cluster': 5, 'hash': 'some_hash'},
                     1: {'cluster': 59, 'hash': 'another_hash'}}
    this_job = mock_job(assign_result)
    mocker.patch(
        'rq.get_current_job',
        return_value=this_job
    )
    p_hash = 'unit_test_visualisations'
    visualise.microreact_internal(assign_result, p_hash,
                                  fs, db_paths, args)
    assert os.path.exists(fs.output_microreact(p_hash, 5) +
                          "/microreact_5_core_NJ.nwk")


def test_microreact_internal():
    assign_result = {0: {'cluster': 5, 'hash': 'some_hash'},
                     1: {'cluster': 59, 'hash': 'another_hash'}}
    p_hash = 'unit_test_visualisations'
    visualise.microreact_internal(assign_result, p_hash,
                                  fs, db_paths, args)
    assert os.path.exists(fs.output_microreact(p_hash, 5) +
                          "/microreact_5_core_NJ.nwk")


def test_network():
    p_hash = 'unit_test_visualisations'
    visualise.network(p_hash, fs, db_paths, args)
    assert os.path.exists(fs.output_network(p_hash) +
                          "/network_cytoscape.graphml")


def test_run_poppunk_internal(qtbot):
    fs_json = FileStore('./tests/files/json')
    sketches = {
        'e868c76fec83ee1f69a95bd27b8d5e76':
        fs_json.get('e868c76fec83ee1f69a95bd27b8d5e76'),
        'f3d9b387e311d5ab59a8c08eb3545dbb':
        fs_json.get('f3d9b387e311d5ab59a8c08eb3545dbb')
    }.items()
    project_hash = 'unit_test_run_poppunk_internal'
    storage_location = storageLocation + '/results'
    redis = Redis()
    queue = Queue(connection=Redis())
    job_ids = app.run_poppunk_internal(sketches,
                                       project_hash,
                                       storage_location,
                                       redis,
                                       queue)
    # stores sketches in storage
    assert fs.input.exists('e868c76fec83ee1f69a95bd27b8d5e76')
    assert fs.input.exists('f3d9b387e311d5ab59a8c08eb3545dbb')
    # submits assign job to queue
    worker = SimpleWorker([queue], connection=queue.connection)
    worker.work(burst=True)  # Runs enqueued job
    job_assign = Job.fetch(job_ids["assign"], connection=redis)
    status_options = ['queued', 'started', 'finished', 'scheduled']
    assert job_assign.get_status() in status_options
    # saves p-hash with job id in redis
    assert read_redis("beebop:hash:job:assign",
                      project_hash, redis) == job_ids["assign"]

    # wait for assign job to be finished
    def assign_status_finished():
        job = Job.fetch(job_ids["assign"], connection=redis)
        assert job.get_status() == 'finished'
    qtbot.waitUntil(assign_status_finished, timeout=20000)
    # submits visualisation jobs to queue
    job_microreact = Job.fetch(job_ids["microreact"], connection=redis)
    assert job_microreact.get_status() in status_options
    assert read_redis("beebop:hash:job:microreact",
                      project_hash,
                      redis) == job_ids["microreact"]
    job_network = Job.fetch(job_ids["network"], connection=redis)
    assert job_network.get_status() in status_options
    assert read_redis("beebop:hash:job:network",
                      project_hash, redis) == job_ids["network"]


def test_get_result_internal(client):
    # queue example job
    redis = Redis()
    q = Queue(connection=redis)
    job = q.enqueue(dummy_fct, 5)
    hash = "unit_test_get_result_internal"
    redis.hset("beebop:hash:job:assign", hash, job.id)
    result1 = app.get_result_internal(hash, redis)
    assert read_data(result1) == {
        "status": "failure",
        "errors": ["Result not ready yet"],
        "data": []
    }
    worker = SimpleWorker([q], connection=q.connection)
    worker.work(burst=True)
    finished = False
    # wait until results are available
    while finished is False:
        time.sleep(1)
        result2 = app.get_result_internal(hash, redis)
        if read_data(result2)['status'] == 'success':
            finished = True
    assert read_data(result2) == {
        "status": "success",
        "errors": [],
        "data": "Result"
    }
    assert read_data(app.get_result_internal("wrong-hash", redis)) == {
        "status": "failure",
        "errors": ["Unknown project hash"],
        "data": []
    }


def test_get_status_internal(client):
    # queue example job
    redis = Redis()
    q = Queue(connection=Redis())
    job_assign = q.enqueue(dummy_fct, 1)
    job_microreact = q.enqueue(dummy_fct, 1)
    job_network = q.enqueue(dummy_fct, 1)
    worker = SimpleWorker([q], connection=q.connection)
    worker.work(burst=True)
    hash = "unit_test_get_status_internal"
    redis.hset("beebop:hash:job:assign", hash, job_assign.id)
    redis.hset("beebop:hash:job:microreact", hash, job_microreact.id)
    redis.hset("beebop:hash:job:network", hash, job_network.id)
    result = app.get_status_internal(hash, redis)
    assert read_data(result)['status'] == 'success'
    status_options = ['queued', 'started', 'finished', 'scheduled', 'waiting']
    assert read_data(result)['data']['assign'] in status_options
    assert read_data(result)['data']['microreact'] in status_options
    assert read_data(result)['data']['network'] in status_options
    assert read_data(app.get_status_internal("wrong-hash", redis)) == {
        "status": "failure",
        "errors": ["Unknown project hash"],
        "data": []
    }


def test_hex_to_decimal():
    dummy_sketch = {
        "sample1": {
            "14": ["0x2964619C7"],
            "17": ["0x52C8C338E"],
            "20": ["0x7C2D24D55"],
            "23": ["0xA5918671C"],
            "26": ["0xCEF5E80E3"],
            "29": ["0xF85A49AAA"]
        }
    }
    dummy_converted = {
        "sample1": {
            "14": [11111111111],
            "17": [22222222222],
            "20": [33333333333],
            "23": [44444444444],
            "26": [55555555555],
            "29": [66666666666]
        }
    }
    assignClusters.hex_to_decimal(dummy_sketch)
    assert dummy_sketch == dummy_converted


def test_filestore():
    fs_test = FileStore('./tests/results/json')
    # check for existing file
    assert fs_test.exists('e868c76fec83ee1f69a95bd27b8d5e76') is True
    # get existing sketch
    fs_test.get('e868c76fec83ee1f69a95bd27b8d5e76')
    # raises exception when trying to get non-existent sketch
    with pytest.raises(Exception):
        fs_test.get('random_non_existent_hash')
    # stores new hash
    characters = string.ascii_letters + string.digits
    new_hash = ''.join(random.choice(characters) for i in range(32))
    new_sketch = {
        'random': 'input'
    }
    assert fs_test.exists(new_hash) is False
    fs_test.put(new_hash, new_sketch)
    assert fs_test.exists(new_hash) is True


class RedisMock:
    def ping(self):
        raise ConnectionRefusedError


def test_check_connection():
    redis_mock = RedisMock()
    with pytest.raises(InternalServerError):
        app.check_connection(redis_mock)
    redis = Redis()
    app.check_connection(redis)
