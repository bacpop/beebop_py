import jsonschema
import json
from PopPUNK import __version__ as poppunk_version
from redis import Redis
from rq import Queue
from rq.job import Job
import time

from beebop import __version__ as beebop_version
from beebop import app
from beebop import versions
from beebop import assignClusters
from beebop.filestore import PoppunkFileStore, FileStore
import beebop.schemas
from tests import setup


schemas = beebop.schemas.Schema()
storageLocation = './tests/files'
fs = PoppunkFileStore(storageLocation)


def dummy_fct():
    time.sleep(5)
    return "Result"


def read_data(response):
    return json.loads(response.get_data().decode("utf-8"))


def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version},
        {"name": "poppunk", "version": poppunk_version}]
    assert jsonschema.validate(versions.get_version(),
                               schemas.version) is None


def test_setup():
    assert jsonschema.validate(json.loads(
        setup.generate_json()), schemas.sketches) is None


def test_assign_clusters():
    assert assignClusters.get_clusters(
        [
            '02ff334f17f17d775b9ecd69046ed296',
            '9c00583e2f24fed5e3c6baa87a4bfa4c',
            '99965c83b1839b25c3c27bd2910da00a'
        ], 'unit_test_poppunk_assign', fs) == {
            0: {'cluster': 9, 'hash': '02ff334f17f17d775b9ecd69046ed296'},
            1: {'cluster': 10, 'hash': '99965c83b1839b25c3c27bd2910da00a'},
            2: {'cluster': 41, 'hash': '9c00583e2f24fed5e3c6baa87a4bfa4c'}}


def test_run_poppunk_internal():
    fs_test = FileStore('./tests/files')
    sketches = {
        'e868c76fec83ee1f69a95bd27b8d5e76':
        fs_test.get('e868c76fec83ee1f69a95bd27b8d5e76'),
        'f3d9b387e311d5ab59a8c08eb3545dbb':
        fs_test.get('f3d9b387e311d5ab59a8c08eb3545dbb')
    }.items()
    project_hash = 'unit_test_run_poppunk_internal'
    storage_location = storageLocation
    redis = Redis()
    job_id = app.run_poppunk_internal(sketches,
                                      project_hash,
                                      storage_location,
                                      redis)
    # stores sketches in storage
    assert fs.input.exists('e868c76fec83ee1f69a95bd27b8d5e76')
    assert fs.input.exists('f3d9b387e311d5ab59a8c08eb3545dbb')
    # submits job to queue
    job = Job.fetch(job_id, connection=redis)
    assert job.get_status() in ['queued', 'started', 'finished', 'scheduled']
    # saves p-hash with job id in redis
    assert redis.hget("beebop:hash:job",
                      project_hash).decode("utf-8") == job_id


def test_get_result_internal():
    # queue example job
    redis = Redis()
    q = Queue(connection=redis)
    job = q.enqueue(dummy_fct)
    hash = "unit_test_get_result_internal"
    redis.hset("beebop:hash:job", hash, job.id)
    result1 = app.get_result_internal(hash, redis)
    assert read_data(result1) == {
        "status": "failure",
        "errors": ["Result not ready yet"],
        "data": []
    }
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


def test_get_status_internal():
    # queue example job
    redis = Redis()
    q = Queue(connection=redis)
    job = q.enqueue(dummy_fct)
    hash = "unit_test_get_status_internal"
    redis.hset("beebop:hash:job", hash, job.id)
    result = app.get_status_internal(hash, redis)
    assert read_data(result)['status'] == 'success'
    assert read_data(result)['data'] in ['queued',
                                         'started',
                                         'finished',
                                         'scheduled']
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
