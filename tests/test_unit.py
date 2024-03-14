import jsonschema
import json
from PopPUNK import __version__ as poppunk_version
from redis import Redis
from rq import SimpleWorker, Queue
from rq.job import Job
from rq.job import Job
import time
import pytest
from pytest_unordered import unordered
from werkzeug.exceptions import InternalServerError
import string
import random
import os
from flask import Flask
from unittest.mock import Mock, patch
from io import BytesIO
from pathlib import Path
import xml.etree.ElementTree as ET
import pickle
import shutil

from beebop import __version__ as beebop_version
from beebop import app
from beebop import versions
from beebop import assignClusters
from beebop import visualise
from beebop import utils
from beebop.filestore import PoppunkFileStore, FileStore, DatabaseFileStore
from beebop.utils import get_args
import beebop.schemas


schemas = beebop.schemas.Schema()
schema_path = Path(os.getcwd() + "/spec")
resolver = jsonschema.validators.RefResolver(
    base_uri=f"{schema_path.as_uri()}/",
    referrer=True,
)
storage_location = './tests/results'
fs = PoppunkFileStore(storage_location)
db_paths = DatabaseFileStore('./storage/GPS_v6_references')
args = get_args()

status_options = ['queued',
                  'started',
                  'finished',
                  'scheduled',
                  'waiting',
                  'deferred']

external_to_poppunk_clusters = {
    "GPSC16": "9",
    "GPSC29": "41",
    "GPSC8": "10"
}

expected_assign_result = {
     0: {'cluster': 'GPSC16', 'hash': '02ff334f17f17d775b9ecd69046ed296'},
     1: {'cluster': 'GPSC29', 'hash': '9c00583e2f24fed5e3c6baa87a4bfa4c'},
     2: {'cluster': 'GPSC8', 'hash': '99965c83b1839b25c3c27bd2910da00a'}
}

name_mapping = {
    "02ff334f17f17d775b9ecd69046ed296": "name1.fa",
    "9c00583e2f24fed5e3c6baa87a4bfa4c": "name2.fa"
}

def dummy_fct(duration):
    time.sleep(duration)
    return "Result"


def read_data(response):
    return json.loads(response.get_data().decode("utf-8"))


def read_redis(name, key, redis):
    return redis.hget(name, key).decode("utf-8")


def run_test_job(p_hash):
    # queue example job
    redis = Redis()
    q = Queue(connection=Redis())
    job_assign = q.enqueue(dummy_fct, 1)
    job_microreact = q.enqueue(dummy_fct, 1)
    job_network = q.enqueue(dummy_fct, 1)
    worker = SimpleWorker([q], connection=q.connection)
    worker.work(burst=True)
    redis.hset("beebop:hash:job:assign", p_hash, job_assign.id)
    redis.hset("beebop:hash:job:microreact", p_hash, job_microreact.id)
    redis.hset("beebop:hash:job:network", p_hash, job_network.id)

def do_assign_clusters(p_hash: str):
    hashes_list = [
            '02ff334f17f17d775b9ecd69046ed296',
            '9c00583e2f24fed5e3c6baa87a4bfa4c',
            '99965c83b1839b25c3c27bd2910da00a']

    return assignClusters.get_clusters(
        hashes_list,
        p_hash,
        fs,
        db_paths,
        args)

def do_network(p_hash: str):
     def mock_get_current_job(Redis):
            assign_result = expected_assign_result

            class mock_dependency:
                def __init__(self, result):
                    self.result = result

            class mock_job:
                def __init__(self, result):
                    self.dependency = mock_dependency(result)
            return mock_job(assign_result)
    mocker.patch(
        'beebop.visualise.get_current_job',
        new=mock_get_current_job
    )
    from beebop import visualise

    do_assign_clusters(p_hash)
    visualise.network(p_hash, fs, db_paths, args, name_mapping)

def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version},
        {"name": "poppunk", "version": poppunk_version}]
    assert jsonschema.validate(versions.get_version(),
                               schemas.version) is None


def test_assign_clusters():
    result = do_assign_clusters('unit_test_poppunk_assign')
    expected = {
            0: {'cluster': 'GPSC16', 'hash': '02ff334f17f17d775b9ecd69046ed296'},
            1: {'cluster': 'GPSC29', 'hash': '9c00583e2f24fed5e3c6baa87a4bfa4c'},
            2: {'cluster': 'GPSC8', 'hash': '99965c83b1839b25c3c27bd2910da00a'}}
    assert list(result.values()) == unordered(list(expected_assign_result.values()))


def test_microreact(mocker):
    def mock_get_current_job(Redis):
        assign_result = expected_assign_result

        class mock_dependency:
            def __init__(self, result):
                self.result = result

        class mock_job:
            def __init__(self, result):
                self.dependency = mock_dependency(result)
        return mock_job(assign_result)
    mocker.patch(
        'beebop.visualise.get_current_job',
        new=mock_get_current_job
    )
    p_hash = 'unit_test_microreact'
    do_network(p_hash)

    visualise.microreact(p_hash, fs, db_paths, args, name_mapping)
    assert os.path.exists(fs.output_microreact(p_hash, 16) +
                          "/microreact_16_core_NJ.nwk")


def test_microreact_internal():
    p_hash = 'unit_test_microreact_internal'
    do_network(p_hash)
    visualise.microreact_internal(expected_assign_result, p_hash,
                                  fs, db_paths, args, name_mapping,
                                  external_to_poppunk_clusters)
    assert os.path.exists(fs.output_microreact(p_hash, 16) +
                          "/microreact_16_core_NJ.nwk")



def test_network(mocker):
    p_hash = 'unit_test_network'
    do_network(p_hash)
    assert os.path.exists(fs.output_network(p_hash) +
                          "/network_cytoscape.graphml")


def test_network_internal():
    assign_result = expected_assign_result
    p_hash = 'unit_test_network_internal'
    do_assign_clusters(p_hash)
    visualise.network_internal(assign_result,
                               p_hash,
                               fs,
                               db_paths,
                               args,
                               name_mapping)
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
    name_mapping = {
        "hash1": "name1.fa",
        "hash2": "name2.fa"
        }
    project_hash = 'unit_test_run_poppunk_internal'
    results_storage_location = storage_location + '/results'
    redis = Redis()
    queue = Queue(connection=Redis())
    response = app.run_poppunk_internal(sketches,
                                        project_hash,
                                        name_mapping,
                                        results_storage_location,
                                        redis,
                                        queue)
    job_ids = read_data(response)['data']
    # stores sketches in storage
    assert fs.input.exists('e868c76fec83ee1f69a95bd27b8d5e76')
    assert fs.input.exists('f3d9b387e311d5ab59a8c08eb3545dbb')
    # submits assign job to queue
    worker = SimpleWorker([queue], connection=queue.connection)
    worker.work(burst=True)  # Runs enqueued job
    job_assign = Job.fetch(job_ids["assign"], connection=redis)
    status_options = ['queued', 'started', 'finished', 'scheduled', 'deferred']
    assert job_assign.get_status() in status_options
    # saves p-hash with job id in redis
    assert read_redis("beebop:hash:job:assign",
                      project_hash, redis) == job_ids["assign"]
    # writes initial output file linking project hash with sample hashes
    results_fs = PoppunkFileStore(results_storage_location)
    with open(results_fs.output_cluster(project_hash), 'rb') as f:
        initial_output = pickle.load(f)
        assert initial_output[0]["hash"] == 'e868c76fec83ee1f69a95bd27b8d5e76'
        assert initial_output[1]["hash"] == 'f3d9b387e311d5ab59a8c08eb3545dbb'

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


def test_get_clusters_json(client):
    hash = "unit_test_get_clusters_internal"
    result = app.get_clusters_json(hash, storage_location)
    expected_result = {'0': {'hash': '24280624a730ada7b5bccea16306765c',
                             'cluster': 3},
                       '1': {'hash': '7e5ddeb048075ac23ab3672769bda17d',
                             'cluster': 53},
                       '2': {'hash': 'f3d9b387e311d5ab59a8c08eb3545dbb',
                             'cluster': 24}}
    assert read_data(result) == {
        "status": "success",
        "errors": [],
        "data": expected_result
    }


def test_get_project(client):
    hash = "unit_test_get_clusters_internal"
    run_test_job(hash)
    result = app.get_project("unit_test_get_clusters_internal")
    assert result.status == "200 OK"
    data = read_data(result)["data"]
    assert data["hash"] == "unit_test_get_clusters_internal"
    samples = data["samples"]
    assert len(samples) == 3
    assert samples[0]["hash"] == "24280624a730ada7b5bccea16306765c"
    assert samples[0]["cluster"] == 3
    assert samples[0]["sketch"]["bbits"] == 3
    assert samples[1]["hash"] == "7e5ddeb048075ac23ab3672769bda17d"
    assert samples[1]["cluster"] == 53
    assert samples[1]["sketch"]["bbits"] == 53
    assert samples[2]["hash"] == "f3d9b387e311d5ab59a8c08eb3545dbb"
    assert samples[2]["cluster"] == 24
    assert samples[2]["sketch"]["bbits"] == 14
    assert data["status"]["assign"] in status_options
    assert data["status"]["microreact"] in status_options
    assert data["status"]["network"] in status_options
    schema = schemas.project
    assert jsonschema.validate(data, schema, resolver=resolver) is None


def test_get_project_returns_404_if_unknown_project_hash(client):
    hash = "unit_test_not_known"
    result = app.get_project(hash)
    assert result[1] == 404
    response = read_data(result[0])["error"]
    data = response["data"]
    errors = response["errors"]
    assert errors[0] == {
        "error": "Project hash not found",
        "detail": "Project hash does not have an associated job"
    }


@patch('rq.job.Job.fetch')
def test_get_project_returns_500_if_status_error(mock_fetch):
    hash = "unit_test_get_clusters_internal"

    def side_effect(id, connection):
        raise AttributeError("test")
    mock_fetch.side_effect = side_effect
    result = app.get_project(hash)
    assert result[1] == 500
    response = read_data(result[0])["error"]
    assert response["status"] == "failure"
    assert response["errors"] == [{"error": "Unknown project hash"}]


@patch('rq.job.Job.fetch')
def test_get_project_returns_samples_before_clusters_assigned(mock_fetch):
    # Fake a project hash that doesn't have clusters yet by adding it to redis,
    # mocking the rq job, and writing out initial output file without cluster
    # assignments.
    hash = "unit_test_no_clusters_yet"
    redis = Redis()
    redis.hset("beebop:hash:job:assign", hash, "9991")
    redis.hset("beebop:hash:job:microreact", hash, "9992")
    redis.hset("beebop:hash:job:network", hash, "9993")
    mock_fetch.return_value = Mock(ok=True)
    mock_get_status = Mock()
    mock_get_status.return_value = "waiting"
    mock_fetch.return_value.get_status = mock_get_status
    fs.ensure_output_dir_exists(hash)
    sample_hash_1 = "24280624a730ada7b5bccea16306765c"
    sample_hash_2 = "7e5ddeb048075ac23ab3672769bda17d"
    initial_output = {
        0: {"hash": sample_hash_1},
        1: {"hash": sample_hash_2}
    }
    with open(fs.output_cluster(hash), 'wb') as f:
        pickle.dump(initial_output, f)
    result = app.get_project(hash)
    assert result.status == "200 OK"
    data = read_data(result)["data"]
    assert data["hash"] == hash
    samples = data["samples"]
    assert len(samples) == 2
    sample_1 = samples[0]
    assert sample_1["hash"] == sample_hash_1
    assert "cluster" not in sample_1
    sample_2 = samples[1]
    assert sample_2["hash"] == sample_hash_2
    assert "cluster" not in sample_2


def test_get_status_internal(client):
    # queue example job
    redis = Redis()
    q = Queue(connection=Redis())
    job_assign = q.enqueue(dummy_fct, 1)
    job_microreact = q.enqueue(dummy_fct, 1)
    job_network = q.enqueue(dummy_fct, 1)
    worker = SimpleWorker([q], connection=q.connection)
    worker.work(burst=True)


def test_get_status_response(client):
    hash = "unit_test_get_status_internal"
    run_test_job(hash)
    redis = Redis()
    result = app.get_status_response(hash, redis)
    assert read_data(result)['status'] == 'success'
    assert read_data(result)['data']['assign'] in status_options
    assert read_data(result)['data']['microreact'] in status_options
    assert read_data(result)['data']['network'] in status_options
    assert read_data(app.get_status_response("wrong-hash",
                                             redis)[0])['error'] == {
        "status": "failure",
        "errors": [{"error": "Unknown project hash"}],
        "data": []
    }


@patch('requests.post')
def test_generate_microreact_url_internal(mock_post):
    dummy_url = 'https://microreact.org/project/12345-testmicroreactapi'
    mock_post.return_value = Mock(ok=True)
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {
        'url': dummy_url}

    microreact_api_new_url = "https://dummy.url"
    project_hash = 'test_microreact_api'
    api_token = os.environ['MICROREACT_TOKEN']
    # for a cluster without tree file
    cluster = '24'

    result = app.generate_microreact_url_internal(microreact_api_new_url,
                                                  project_hash,
                                                  cluster,
                                                  api_token,
                                                  storage_location)
    assert read_data(result)['data'] == {'cluster': cluster, 'url': dummy_url}
    # for a cluster with tree file
    cluster = '7'
    result2 = app.generate_microreact_url_internal(microreact_api_new_url,
                                                   project_hash,
                                                   cluster,
                                                   api_token,
                                                   storage_location)
    assert read_data(result2)['data'] == {'cluster': cluster, 'url': dummy_url}


@patch('requests.post')
def test_generate_microreact_url_internal_API_error_404(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 404
    mock_post.return_value.json.return_value = {
        'error': 'Resource not found'}

    microreact_api_new_url = "https://dummy.url"
    project_hash = 'test_microreact_api'
    api_token = os.environ['MICROREACT_TOKEN']
    cluster = '24'

    result = app.generate_microreact_url_internal(microreact_api_new_url,
                                                  project_hash,
                                                  cluster,
                                                  api_token,
                                                  storage_location)
    error = read_data(result[0])['error']
    assert error['errors'][0]['error'] == 'Resource not found'


@patch('requests.post')
def test_generate_microreact_url_internal_API_error_500(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 500
    mock_post.return_value.json.return_value = {
        'error': 'Internal Server Error'}

    microreact_api_new_url = "https://dummy.url"
    project_hash = 'test_microreact_api'
    api_token = os.environ['MICROREACT_TOKEN']
    cluster = '24'

    result = app.generate_microreact_url_internal(microreact_api_new_url,
                                                  project_hash,
                                                  cluster,
                                                  api_token,
                                                  storage_location)
    error = read_data(result[0])['error']
    assert error['errors'][0]['error'] == 'Wrong Token'


@patch('requests.post')
def test_generate_microreact_url_internal_API_other_error(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 456
    mock_post.return_value.json.return_value = {
        'error': 'Unexpected error'}

    microreact_api_new_url = "https://dummy.url"
    project_hash = 'test_microreact_api'
    api_token = os.environ['MICROREACT_TOKEN']
    cluster = '24'

    result = app.generate_microreact_url_internal(microreact_api_new_url,
                                                  project_hash,
                                                  cluster,
                                                  api_token,
                                                  storage_location)
    error = read_data(result[0])['error']['errors'][0]
    assert error['error'] == 'Unknown error'
    assert 'Microreact API returned status code 456.' in error['detail']


def test_send_zip_internal(client):
    app_app = Flask(__name__)
    with app_app.test_request_context():
        project_hash = 'test_microreact_api'
        cluster = '24'
        type = 'microreact'
        response = app.send_zip_internal(project_hash,
                                         type,
                                         cluster,
                                         storage_location)
        response.direct_passthrough = False
        filename1 = 'microreact_24_microreact_clusters.csv'
        filename2 = 'microreact_24_perplexity20.0_accessory_mandrake.dot'
        assert filename1.encode('utf-8') in response.data
        assert filename2.encode('utf-8') in response.data
        project_hash = 'test_network_zip'
        cluster = "GPSC1"
        type = 'network'
        response = app.send_zip_internal(project_hash,
                                         type,
                                         cluster,
                                         storage_location)
        response.direct_passthrough = False
        assert 'network_cytoscape.csv'.encode('utf-8') in response.data
        assert 'network_cytoscape.graphml'.encode('utf-8') in response.data
        assert 'network_component_38.graphml'.encode('utf-8') in response.data


def test_download_graphml_internal():
    project_hash = 'unit_test_graphml'
    cluster = "GPSC16"
    do_assign_clusters(project_hash)
    response = app.download_graphml_internal(project_hash,
                                             cluster,
                                             storage_location)
    graph_string = read_data(response)['data']['graph']
    assert all(x in graph_string for x in ['</graph>',
                                           '</graphml>',
                                           '</node>',
                                           '</edge>'])
    cluster_no_network_file = "not a cluster"
    response_error2 = app.download_graphml_internal(project_hash,
                                                    cluster_no_network_file,
                                                    storage_location)
    error2 = read_data(response_error2[0])['error']['errors'][0]
    assert error2['error'] == 'File not found'


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


def test_add_files():
    memory_file1 = BytesIO()
    app.add_files(memory_file1, './tests/files/sketchlib_input')
    memory_file1.seek(0)
    contents1 = memory_file1.read()
    assert 'rfile.txt'.encode('utf-8') in contents1
    assert '6930_8_9.fa'.encode('utf-8') in contents1
    assert '7622_5_91.fa'.encode('utf-8') in contents1
    memory_file2 = BytesIO()
    app.add_files(memory_file2, './tests/files/sketchlib_input', ('rfile.txt'))
    memory_file2.seek(0)
    contents2 = memory_file2.read()
    assert 'rfile.txt'.encode('utf-8') in contents2
    assert '6930_8_9.fa'.encode('utf-8') not in contents2
    assert '7622_5_91.fa'.encode('utf-8') not in contents2


def test_generate_mapping():
    cluster_nos_to_map = ['5', '7', '9', '13', '14', '31', '32']
    result = utils.generate_mapping('results_modifications', cluster_nos_to_map, fs)
    print(result)
    exp_cluster_component_dict = {
        '13': '2',
        '31': '1',
        '32': '3',
        '7': '4',
        '9': '6',
        '14': '7',
        '5': '5'
    }
    assert result == exp_cluster_component_dict


def test_delete_component_files():
    # should remove all component files apart from components 7 and 5
    cluster_component_dict = {
        '4': '7',
        '5': '5',
        '23': '12',
        '1': '2',
        '12': '3'
    }
    assign_result = {
        0: {'cluster': '4'},
        1: {'cluster': '5'}
    }
    p_hash = 'results_modifications'
    utils.delete_component_files(cluster_component_dict,
                                 fs,
                                 assign_result,
                                 p_hash)
    assert not os.path.exists(fs.output_network(p_hash) +
                              "/network_component_1.graphml")
    assert not os.path.exists(fs.output_network(p_hash) +
                              "/network_component_2.graphml")
    assert not os.path.exists(fs.output_network(p_hash) +
                              "/network_component_3.graphml")
    assert not os.path.exists(fs.output_network(p_hash) +
                              "/network_component_4.graphml")
    assert os.path.exists(fs.output_network(p_hash) +
                          "/network_component_5.graphml")
    assert not os.path.exists(fs.output_network(p_hash) +
                              "/network_component_6.graphml")
    assert os.path.exists(fs.output_network(p_hash) +
                          "/network_component_7.graphml")


def test_replace_filehashes():
    p_hash = 'results_modifications'
    folder = fs.output_network(p_hash)
    filename_dict = {
        'filehash1': 'filename1',
        'filehash2': 'filename2',
        'filehash3': 'filename3',
    }
    utils.replace_filehashes(folder, filename_dict)
    with open(fs.network_output_component(p_hash, 5), 'r') as comp5:
        comp5_text = comp5.read()
        assert 'filename1' in comp5_text
        assert 'filename3' in comp5_text
        assert 'filehash1' not in comp5_text
        assert 'filehash3' not in comp5_text
    with open(fs.network_output_component(p_hash, 7), 'r') as comp7:
        comp7_text = comp7.read()
        assert 'filename2' in comp7_text
        assert 'filehash2' not in comp7_text


def test_add_query_ref_status():
    p_hash = 'results_modifications'
    filename_dict = {
        'filehash1': 'filename1',
        'filehash2': 'filename2',
        'filehash3': 'filename3',
    }
    utils.add_query_ref_status(fs, p_hash, filename_dict)
    path = fs.network_output_component(p_hash, 5)
    print(path)
    xml = ET.parse(path)
    graph = xml.getroot()

    def get_node_status(node_no):
        node = graph.find(
            f".//{{http://graphml.graphdrawing.org/xmlns}}"
            f"node[@id='n{node_no}']"
        )
        return node.find(
            "./{http://graphml.graphdrawing.org/xmlns}data[@key='ref_query']"
        ).text
    assert get_node_status(21) == 'query'
    assert get_node_status(22) == 'query'
    assert get_node_status(20) == 'ref'
