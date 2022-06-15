import jsonschema
import json
from PopPUNK import __version__ as poppunk_version
from redis import Redis
from rq import Queue
from rq.job import Job
import time
import pytest
from werkzeug.exceptions import InternalServerError
import string
import random
import numpy as np

from beebop import __version__ as beebop_version
from beebop import app
from beebop import versions
from beebop import assignClusters
from beebop.filestore import PoppunkFileStore, FileStore
import beebop.schemas
from tests import setup
from tests import hdf5_to_json


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


def test_filestore():
    fs_test = FileStore('./tests/files')
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


def test_encoder():
    sketch_dec = {
        # these attributes should not be converted
        "codon_phased": False,
        "sketchsize64": np.uint64(156),
        "version": "c42cd0e22f6ef6d5c9a2900fa16367e096519170",
        "bases": np.array([
            0.300634,
            0.207509,
            0.188758,
            0.303098
        ]),
        # these arrays should be converted
        "14": np.array([
            np.uint64(1235257003518336777),
            np.uint64(14030650254064228360),
            np.uint64(488260933813288744),
            np.uint64(13271593282997563722)
        ]),
        "26": np.array([
            np.uint64(8801399391270942979),
            np.uint64(3035910811177971819),
            np.uint64(5773981271656401042),
            np.uint64(6376812912998502619)
        ])
    }
    sketch_hex_expected = {
        "codon_phased": False,
        "sketchsize64": 156,
        "version": "c42cd0e22f6ef6d5c9a2900fa16367e096519170",
        "bases": [
            0.300634,
            0.207509,
            0.188758,
            0.303098
        ],
        "14": [
            "0x112483b335061f09",
            "0xc2b6e211893e1808",
            "0x6c6a6bb7da43f28",
            "0xb82e2bc66487b14a"
        ],
        "26": [
            "0x7a24da1d530b3903",
            "0x2a21b8cc3e07f06b",
            "0x50214d5fecd68892",
            "0x587efd8efe6650db"
        ]
    }
    sketch_hex = json.dumps(sketch_dec, cls=hdf5_to_json.NpEncoder)
    assert json.loads(sketch_hex) == sketch_hex_expected
