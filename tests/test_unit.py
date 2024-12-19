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
from unittest.mock import Mock, patch, call
from io import BytesIO
from pathlib import Path
import beebop.visualise
from tests import setup
import xml.etree.ElementTree as ET
import pickle
from pathlib import PurePath
import pandas as pd

from beebop import __version__ as beebop_version
from beebop import app
from beebop import versions
from beebop import assignClusters
from beebop import visualise
from beebop import utils
from beebop.poppunkWrapper import PoppunkWrapper

import beebop.schemas
from beebop.filestore import PoppunkFileStore, FileStore, DatabaseFileStore
import networkx as nx

fs = setup.fs
args = setup.args
storage_location = setup.storage_location

schemas = beebop.schemas.Schema()
schema_path = Path(os.getcwd() + "/spec")
resolver = jsonschema.validators.RefResolver(
    base_uri=f"{schema_path.as_uri()}/",
    referrer=True,
)

status_options = [
    "queued",
    "started",
    "finished",
    "scheduled",
    "waiting",
    "deferred",
]

external_to_poppunk_clusters = {"GPSC16": "9", "GPSC29": "41", "GPSC8": "10"}


@pytest.fixture
def sample_clustering_csv(tmp_path):
    # Create data as dictionary
    data = {
        "sample": ["sample1", "sample2", "sample3", "sample4", "sample5"],
        "Cluster": ["10", "309;20;101", "30", "40", None],  # Using None for NA
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Define path and save CSV
    csv_path = tmp_path / "samples.csv"
    df.to_csv(csv_path, index=False)

    return str(csv_path)


@pytest.fixture
def config():
    return assignClusters.ClusteringConfig(
        "species",
        "p_hash",
        {},
        "prefix",
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        "outdir",
    )


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


def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version},
        {"name": "poppunk", "version": poppunk_version},
    ]
    assert jsonschema.validate(versions.get_version(), schemas.version) is None


def test_assign_clusters():
    result = setup.do_assign_clusters("unit_test_poppunk_assign")
    expected = unordered(list(setup.expected_assign_result.values()))
    assert list(result.values()) == expected


def test_microreact(mocker):
    def mock_get_current_job(Redis):
        assign_result = setup.expected_assign_result

        class mock_dependency:
            def __init__(self, result):
                self.result = result

        class mock_job:
            def __init__(self, result):
                self.dependency = mock_dependency(result)

        return mock_job(assign_result)

    mocker.patch("beebop.visualise.get_current_job", new=mock_get_current_job)
    p_hash = "unit_test_microreact"

    setup.do_network_internal(p_hash)

    visualise.microreact(
        p_hash,
        fs,
        setup.full_db_fs,
        args,
        setup.name_mapping,
        setup.species,
        "localhost",
        {},
    )

    time.sleep(60)  # wait for jobs to finish

    assert os.path.exists(
        fs.output_microreact(p_hash, 16) + "/microreact_16_core_NJ.nwk"
    )
    assert os.path.exists(
        fs.output_microreact(p_hash, 8) + "/microreact_8_core_NJ.nwk"
    )
    assert os.path.exists(
        fs.output_microreact(p_hash, 29) + "/microreact_29_core_NJ.nwk"
    )


@patch("beebop.visualise.replace_filehashes")
def test_microreact_per_cluster(mock_replace_filehashes, mocker):
    p_hash = "unit_test_microreact_internal"
    cluster = "GPSC16"
    wrapper = Mock()
    mocker.patch.object(wrapper, "create_microreact")

    visualise.microreact_per_cluster(
        cluster,
        p_hash,
        fs,
        wrapper,
        setup.name_mapping,
        external_to_poppunk_clusters,
    )

    wrapper.create_microreact.assert_called_with("16", "9")
    mock_replace_filehashes.assert_called_with(
        fs.output_microreact(p_hash, 16), setup.name_mapping
    )


def test_queue_microreact_jobs(mocker):
    p_hash = "unit_test_microreact_internal"
    wrapper = Mock()
    redis = Mock()
    mocker.patch.object(redis, "hset")
    mockQueue = Mock()
    mockJob = Mock()
    mockJob.id.return_value = "1234"
    mockQueue.enqueue.return_value = mockJob
    mocker.patch("beebop.visualise.Queue", return_value=mockQueue)
    mocker.patch("beebop.visualise.Dependency")
    expected_hset_calls = [
        call(
            f"beebop:hash:job:microreact:{p_hash}", item["cluster"], mockJob.id
        )
        for item in setup.expected_assign_result.values()
    ]
    expected_enqueue_calls = [
        call(
            visualise.microreact_per_cluster,
            args=(
                item["cluster"],
                p_hash,
                fs,
                wrapper,
                setup.name_mapping,
                external_to_poppunk_clusters,
            ),
            job_timeout=60,
            depends_on=mocker.ANY,
        )
        for i, item in enumerate(setup.expected_assign_result.values())
    ]

    visualise.queue_microreact_jobs(
        setup.expected_assign_result,
        p_hash,
        fs,
        wrapper,
        setup.name_mapping,
        external_to_poppunk_clusters,
        redis,
        queue_kwargs={"job_timeout": 60},
    )

    redis.hset.assert_has_calls(expected_hset_calls, any_order=True)
    mockQueue.enqueue.assert_has_calls(expected_enqueue_calls, any_order=True)


def test_network(mocker):
    p_hash = "unit_test_network"

    def mock_get_current_job(Redis):
        assign_result = setup.expected_assign_result

        class mock_dependency:
            def __init__(self, result):
                self.result = result

        class mock_job:
            def __init__(self, result):
                self.dependency = mock_dependency(result)

        return mock_job(assign_result)

    mocker.patch("beebop.visualise.get_current_job", new=mock_get_current_job)
    from beebop import visualise

    setup.do_assign_clusters(p_hash)
    visualise.network(
        p_hash, fs, setup.full_db_fs, args, setup.name_mapping, setup.species
    )

    for cluster in external_to_poppunk_clusters.keys():
        cluster_num = utils.get_cluster_num(cluster)
        assert os.path.exists(
            fs.output_network(p_hash)
            + f"/network_component_{cluster_num}.graphml"
        )


def test_network_internal():
    p_hash = "unit_test_network_internal"
    setup.do_network_internal(p_hash)
    for cluster in external_to_poppunk_clusters.keys():
        cluster_num = utils.get_cluster_num(cluster)
        assert os.path.exists(
            fs.output_network(p_hash)
            + f"/network_component_{cluster_num}.graphml"
        )


def test_run_poppunk_internal(qtbot):
    fs_json = FileStore("./tests/files/json")
    sketches = {
        "e868c76fec83ee1f69a95bd27b8d5e76": fs_json.get(
            "e868c76fec83ee1f69a95bd27b8d5e76"
        ),
        "f3d9b387e311d5ab59a8c08eb3545dbb": fs_json.get(
            "f3d9b387e311d5ab59a8c08eb3545dbb"
        ),
    }.items()
    name_mapping = {"hash1": "name1.fa", "hash2": "name2.fa"}
    project_hash = "unit_test_run_poppunk_internal"
    results_storage_location = storage_location + "/results"
    redis = Redis()
    queue = Queue(connection=Redis())
    response = app.run_poppunk_internal(
        sketches,
        project_hash,
        name_mapping,
        results_storage_location,
        redis,
        queue,
        setup.species,
    )
    job_ids = read_data(response)["data"]
    # stores sketches in storage
    assert fs.input.exists("e868c76fec83ee1f69a95bd27b8d5e76")
    assert fs.input.exists("f3d9b387e311d5ab59a8c08eb3545dbb")
    # submits assign job to queue
    worker = SimpleWorker([queue], connection=queue.connection)
    worker.work(burst=True)  # Runs enqueued job
    job_assign = Job.fetch(job_ids["assign"], connection=redis)
    status_options = ["queued", "started", "finished", "scheduled", "deferred"]
    assert job_assign.get_status() in status_options
    # saves p-hash with job id in redis
    assert (
        read_redis("beebop:hash:job:assign", project_hash, redis)
        == job_ids["assign"]
    )
    # writes initial output file linking project hash with sample hashes
    results_fs = PoppunkFileStore(results_storage_location)
    with open(results_fs.output_cluster(project_hash), "rb") as f:
        initial_output = pickle.load(f)
        assert initial_output[0]["hash"] == "e868c76fec83ee1f69a95bd27b8d5e76"
        assert initial_output[1]["hash"] == "f3d9b387e311d5ab59a8c08eb3545dbb"

    # wait for assign job to be finished
    def assign_status_finished():
        job = Job.fetch(job_ids["assign"], connection=redis)
        assert job.get_status() == "finished"

    qtbot.waitUntil(assign_status_finished, timeout=20000)
    # submits visualisation jobs to queue
    job_microreact = Job.fetch(job_ids["microreact"], connection=redis)
    assert job_microreact.get_status() in status_options
    assert (
        read_redis("beebop:hash:job:microreact", project_hash, redis)
        == job_ids["microreact"]
    )
    job_network = Job.fetch(job_ids["network"], connection=redis)
    assert job_network.get_status() in status_options
    assert (
        read_redis("beebop:hash:job:network", project_hash, redis)
        == job_ids["network"]
    )


def test_get_clusters_json(client):
    hash = "unit_test_get_clusters_internal"
    result = app.get_clusters_json(hash, storage_location)
    expected_result = {
        "24280624a730ada7b5bccea16306765c": {
            "hash": "24280624a730ada7b5bccea16306765c",
            "cluster": 3,
        },
        "7e5ddeb048075ac23ab3672769bda17d": {
            "hash": "7e5ddeb048075ac23ab3672769bda17d",
            "cluster": 53,
        },
        "f3d9b387e311d5ab59a8c08eb3545dbb": {
            "hash": "f3d9b387e311d5ab59a8c08eb3545dbb",
            "cluster": 24,
        },
    }
    assert read_data(result) == {
        "status": "success",
        "errors": [],
        "data": expected_result,
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
    assert (
        samples["24280624a730ada7b5bccea16306765c"]["hash"]
        == "24280624a730ada7b5bccea16306765c"
    )
    assert samples["24280624a730ada7b5bccea16306765c"]["cluster"] == 3
    assert samples["24280624a730ada7b5bccea16306765c"]["sketch"]["bbits"] == 3
    assert (
        samples["7e5ddeb048075ac23ab3672769bda17d"]["hash"]
        == "7e5ddeb048075ac23ab3672769bda17d"
    )
    assert samples["7e5ddeb048075ac23ab3672769bda17d"]["cluster"] == 53
    assert samples["7e5ddeb048075ac23ab3672769bda17d"]["sketch"]["bbits"] == 53
    assert (
        samples["f3d9b387e311d5ab59a8c08eb3545dbb"]["hash"]
        == "f3d9b387e311d5ab59a8c08eb3545dbb"
    )
    assert samples["f3d9b387e311d5ab59a8c08eb3545dbb"]["cluster"] == 24
    assert samples["f3d9b387e311d5ab59a8c08eb3545dbb"]["sketch"]["bbits"] == 14
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
        "detail": "Project hash does not have an associated job",
    }


@patch("rq.job.Job.fetch")
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


@patch("rq.job.Job.fetch")
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
        sample_hash_1: {"hash": sample_hash_1},
        sample_hash_2: {"hash": sample_hash_2},
    }
    with open(fs.output_cluster(hash), "wb") as f:
        pickle.dump(initial_output, f)
    result = app.get_project(hash)
    assert result.status == "200 OK"
    data = read_data(result)["data"]
    assert data["hash"] == hash
    samples = data["samples"]
    assert len(samples) == 2
    sample_1 = samples["24280624a730ada7b5bccea16306765c"]
    assert sample_1["hash"] == sample_hash_1
    assert sample_1["cluster"] is None
    sample_2 = samples["7e5ddeb048075ac23ab3672769bda17d"]
    assert sample_2["hash"] == sample_hash_2
    assert sample_2["cluster"] is None


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
    assert read_data(result)["status"] == "success"
    assert read_data(result)["data"]["assign"] in status_options
    assert read_data(result)["data"]["microreact"] in status_options
    assert read_data(result)["data"]["network"] in status_options
    assert read_data(app.get_status_response("wrong-hash", redis)[0])[
        "error"
    ] == {
        "status": "failure",
        "errors": [{"error": "Unknown project hash"}],
        "data": [],
    }


@patch("requests.post")
def test_generate_microreact_url_internal(mock_post):
    dummy_url = "https://microreact.org/project/12345-testmicroreactapi"
    mock_post.return_value = Mock(ok=True)
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"url": dummy_url}

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    # for a cluster without tree file
    cluster = "24"

    result = app.generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        storage_location,
    )
    assert read_data(result)["data"] == {"cluster": cluster, "url": dummy_url}
    # for a cluster with tree file
    cluster = "7"
    result2 = app.generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        storage_location,
    )
    assert read_data(result2)["data"] == {"cluster": cluster, "url": dummy_url}


@patch("requests.post")
def test_generate_microreact_url_internal_API_error_404(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 404
    mock_post.return_value.json.return_value = {"error": "Resource not found"}

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    cluster = "24"

    result = app.generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        storage_location,
    )
    error = read_data(result[0])["error"]
    assert error["errors"][0]["error"] == "Resource not found"


@patch("requests.post")
def test_generate_microreact_url_internal_API_error_500(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 500
    mock_post.return_value.json.return_value = {
        "error": "Internal Server Error"
    }

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    cluster = "24"

    result = app.generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        storage_location,
    )
    error = read_data(result[0])["error"]
    assert error["errors"][0]["error"] == "Wrong Token"


@patch("requests.post")
def test_generate_microreact_url_internal_API_other_error(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 456
    mock_post.return_value.json.return_value = {"error": "Unexpected error"}

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    cluster = "24"

    result = app.generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        storage_location,
    )
    error = read_data(result[0])["error"]["errors"][0]
    assert error["error"] == "Unknown error"
    assert "Microreact API returned status code 456." in error["detail"]


def test_send_zip_internal(client):
    app_app = Flask(__name__)
    with app_app.test_request_context():
        project_hash = "test_microreact_api"
        cluster = "24"
        type = "microreact"
        response = app.send_zip_internal(
            project_hash, type, cluster, storage_location
        )
        response.direct_passthrough = False
        filename1 = "microreact_24_microreact_clusters.csv"
        filename2 = "microreact_24_perplexity20.0_accessory_mandrake.dot"
        assert filename1.encode("utf-8") in response.data
        assert filename2.encode("utf-8") in response.data
        
        project_hash = "test_network_zip"
        cluster = "GPSC38"
        type = "network"
        response = app.send_zip_internal(
            project_hash, type, cluster, storage_location
        )
        response.direct_passthrough = False
        assert "network_cytoscape.csv".encode("utf-8") in response.data
        assert "network_component_38.graphml".encode("utf-8") in response.data


def test_hex_to_decimal():
    dummy_sketch = {
        "sample1": {
            "14": ["0x2964619C7"],
            "17": ["0x52C8C338E"],
            "20": ["0x7C2D24D55"],
            "23": ["0xA5918671C"],
            "26": ["0xCEF5E80E3"],
            "29": ["0xF85A49AAA"],
        }
    }
    dummy_converted = {
        "sample1": {
            "14": [11111111111],
            "17": [22222222222],
            "20": [33333333333],
            "23": [44444444444],
            "26": [55555555555],
            "29": [66666666666],
        }
    }
    assignClusters.hex_to_decimal(dummy_sketch)
    assert dummy_sketch == dummy_converted


def test_filestore():
    fs_test = FileStore("./tests/results/json")
    # check for existing file
    assert fs_test.exists("e868c76fec83ee1f69a95bd27b8d5e76") is True
    # get existing sketch
    fs_test.get("e868c76fec83ee1f69a95bd27b8d5e76")
    # raises exception when trying to get non-existent sketch
    with pytest.raises(Exception):
        fs_test.get("random_non_existent_hash")
    # stores new hash
    characters = string.ascii_letters + string.digits
    new_hash = "".join(random.choice(characters) for i in range(32))
    new_sketch = {"random": "input"}
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
    app.add_files(memory_file1, "./tests/files/sketchlib_input")
    memory_file1.seek(0)
    contents1 = memory_file1.read()
    assert "rfile.txt".encode("utf-8") in contents1
    assert "6930_8_9.fa".encode("utf-8") in contents1
    assert "7622_5_91.fa".encode("utf-8") in contents1
    memory_file2 = BytesIO()
    app.add_files(memory_file2, "./tests/files/sketchlib_input", ("rfile.txt"))
    memory_file2.seek(0)
    contents2 = memory_file2.read()
    assert "rfile.txt".encode("utf-8") in contents2
    assert "6930_8_9.fa".encode("utf-8") not in contents2
    assert "7622_5_91.fa".encode("utf-8") not in contents2


def test_replace_filehashes():
    p_hash = "results_modifications"
    folder = fs.output_network(p_hash)
    filename_dict = {
        "filehash1": "filename1",
        "filehash2": "filename2",
        "filehash3": "filename3",
    }
    utils.replace_filehashes(folder, filename_dict)
    with open(fs.network_output_component(p_hash, 5), "r") as comp5:
        comp5_text = comp5.read()
        assert "filename1" in comp5_text
        assert "filename3" in comp5_text
        assert "filehash1" not in comp5_text
        assert "filehash3" not in comp5_text
    with open(fs.network_output_component(p_hash, 7), "r") as comp7:
        comp7_text = comp7.read()
        assert "filename2" in comp7_text
        assert "filehash2" not in comp7_text


@patch("beebop.poppunkWrapper.assign_query_hdf5")
def test_poppunk_wrapper_assign_cluster(mock_assign):
    db_fs = DatabaseFileStore(
        "./storage/GPS_v9_ref", "GPS_v9_external_clusters.csv"
    )
    p_hash = "random hash"
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, setup.species)

    wrapper.assign_clusters(db_fs, ["ms1", "ms2"], fs.output(p_hash))

    mock_assign.assert_called_with(
        dbFuncs=db_fs,
        ref_db=db_fs.db,
        qNames=["ms1", "ms2"],
        output=fs.output(wrapper.p_hash),
        qc_dict=vars(getattr(args.species, setup.species).qc_dict),
        update_db=args.assign.update_db,
        write_references=args.assign.write_references,
        distances=db_fs.distances,
        serial=args.assign.serial,
        threads=args.assign.threads,
        overwrite=args.assign.overwrite,
        plot_fit=args.assign.plot_fit,
        graph_weights=args.assign.graph_weights,
        model_dir=db_fs.db,
        strand_preserved=args.assign.strand_preserved,
        previous_clustering=db_fs.db,
        external_clustering=db_fs.external_clustering,
        core=args.assign.core_only,
        accessory=args.assign.accessory_only,
        gpu_dist=args.assign.gpu_dist,
        gpu_graph=args.assign.gpu_graph,
        save_partial_query_graph=args.assign.save_partial_query_graph,
        stable=args.assign.stable,
        use_full_network=args.assign.use_full_network,
    )


def test_get_failed_samples_internal_no_file():
    p_hash = "unit_test_get_clusters_internal"
    result = app.get_failed_samples_internal(p_hash, storage_location)
    assert result == {}


def test_get_failed_samples_internal_file_exists():
    p_hash = "unit_test_get_failed_samples_internal"
    result = app.get_failed_samples_internal(p_hash, storage_location)
    assert result == {
        "3eaf3ff220d15f8b7ce9ee47aaa9b4a9": {
            "hash": "3eaf3ff220d15f8b7ce9ee47aaa9b4a9",
            "failReasons": [
                "Failed distance QC (too high)",
                "Failed distance QC (too many zeros)",
            ],
        }
    }


def test_get_clusters_json_with_failed_samples(client):
    p_hash = "unit_test_get_failed_samples_internal"

    result = app.get_clusters_json(p_hash, storage_location)

    expected_result = {
        "3eaf3ff220d15f8b7ce9ee47aaa9b4a9": {
            "hash": "3eaf3ff220d15f8b7ce9ee47aaa9b4a9",
            "failReasons": [
                "Failed distance QC (too high)",
                "Failed distance QC (too many zeros)",
            ],
        },
        "c448c13f7efd6a5e7e520a7495f3f40f": {
            "hash": "c448c13f7efd6a5e7e520a7495f3f40f",
            "cluster": "GPSC3",
        },
    }
    assert read_data(result) == {
        "status": "success",
        "errors": [],
        "data": expected_result,
    }


def test_get_project_with_failed_samples(client):
    p_hash = "unit_test_get_failed_samples_internal"
    run_test_job(p_hash)

    result = app.get_project(p_hash)

    assert result.status == "200 OK"
    samples = read_data(result)["data"]["samples"]
    assert len(samples) == 2
    assert (
        samples["3eaf3ff220d15f8b7ce9ee47aaa9b4a9"]["hash"]
        == "3eaf3ff220d15f8b7ce9ee47aaa9b4a9"
    )
    assert (
        samples["3eaf3ff220d15f8b7ce9ee47aaa9b4a9"]["failReasons"][0]
        == "Failed distance QC (too high)"
    )
    assert (
        samples["3eaf3ff220d15f8b7ce9ee47aaa9b4a9"]["failReasons"][1]
        == "Failed distance QC (too many zeros)"
    )
    assert (
        samples["c448c13f7efd6a5e7e520a7495f3f40f"]["hash"]
        == "c448c13f7efd6a5e7e520a7495f3f40f"
    )
    assert samples["c448c13f7efd6a5e7e520a7495f3f40f"]["cluster"] == "GPSC3"


def test_get_cluster_num_with_numeric_part():
    assert utils.get_cluster_num("cluster123") == "123"


def test_get_cluster_num_with_no_numeric_part():
    assert utils.get_cluster_num("cluster") == "cluster"


def test_get_cluster_num_with_multiple_numeric_parts():
    assert utils.get_cluster_num("cluster123abc456") == "123"


def test_get_cluster_num_with_empty_string():

    assert utils.get_cluster_num("") == ""


def test_get_cluster_num_with_special_characters():
    assert utils.get_cluster_num("cluster@#123") == "123"


def test_cluster_nums_from_assign_multiple_clusters():
    assign_result = {
        "sample1": {"cluster": "GPSC3"},
        "sample2": {"cluster": "GPSC60"},
        "sample3": {"cluster": "GPSC3"},
    }
    expected = ["3", "60"]
    assert sorted(utils.cluster_nums_from_assign(assign_result)) == sorted(
        expected
    )


@patch("beebop.app.getKmersFromReferenceDatabase")
def test_get_species_kmers(mock_getKmersFromReferenceDatabase):
    mock_getKmersFromReferenceDatabase.return_value = [14, 17, 20, 23]

    species_db_name = "valid_species_db"
    expected_result = {"kmerMax": 23, "kmerMin": 14, "kmerStep": 3}

    result = app.get_species_kmers(species_db_name)
    assert result == expected_result


@patch("beebop.app.get_species_kmers")
def test_get_species_config_valid(mock_get_species_kmers, client):
    mock_get_species_kmers.return_value = {
        "kmerMax": 31,
        "kmerMin": 15,
        "kmerStep": 2,
    }

    response = app.get_species_config()
    data = response.get_json()

    assert response.status_code == 200
    assert data["status"] == "success"
    assert "Streptococcus pneumoniae" in data["data"]
    assert "Streptococcus agalactiae" in data["data"]
    for species, kmers in data["data"].items():
        assert kmers == {"kmerMax": 31, "kmerMin": 15, "kmerStep": 2}


def test_parital_query_graph():
    p_hash = "test_hash"
    expected_path = str(PurePath(fs.output(p_hash), f"{p_hash}_query.subset"))
    assert fs.partial_query_graph(p_hash) == expected_path


@patch("os.makedirs")
def test_tmp(mock_makedirs):
    p_hash = "test_hash"
    expected_path = PurePath(fs.output(p_hash), "tmp")

    # Call the tmp method
    result_path = fs.tmp(p_hash)

    # Check if the directory is created and the path is correct
    mock_makedirs.assert_called_once_with(expected_path, exist_ok=True)
    assert result_path == str(expected_path)


def test_get_df_sample_mask(sample_clustering_csv):
    """Test getting mask for existing samples"""
    samples = ["sample1", "sample3"]

    df, mask = utils.get_df_sample_mask(sample_clustering_csv, samples)

    # Check DataFrame
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert list(df.columns) == ["sample", "Cluster"]

    # Check column types
    assert df["sample"].dtype == object  # string type in pandas
    assert df["Cluster"].dtype == object  # string type in pandas

    # Check mask
    assert isinstance(mask, pd.Series)
    assert mask.tolist() == [True, False, True, False, False]
    assert sum(mask) == 2


@patch("beebop.utils.get_external_cluster_nums")
def test_update_external_clusters_csv(
    mock_get_external_cluster_nums, sample_clustering_csv
):
    not_found_samples = ["sample1", "sample3"]
    sample_cluster_num_mapping = {"sample1": "11", "sample3": "69;191"}
    source_query_clustering = "tmp_query_clustering.csv"
    mock_get_external_cluster_nums.return_value = sample_cluster_num_mapping
    utils.update_external_clusters_csv(
        sample_clustering_csv,
        source_query_clustering,
        not_found_samples,
    )

    df = pd.read_csv(sample_clustering_csv)

    mock_get_external_cluster_nums.assert_called_once_with(
        source_query_clustering, not_found_samples
    )
    assert df.loc[df["sample"] == "sample1", "Cluster"].values[0] == "11"
    assert (
        df.loc[df["sample"] == "sample2", "Cluster"].values[0] == "309;20;101"
    )  # Unchanged
    assert df.loc[df["sample"] == "sample3", "Cluster"].values[0] == "69;191"
    assert (
        df.loc[df["sample"] == "sample4", "Cluster"].values[0] == "40"
    )  # Unchanged


def test_get_external_clusters_from_file(sample_clustering_csv):
    samples = ["sample1", "sample2", "sample5"]
    prefix = "PRE"

    external_clusters, not_found = utils.get_external_clusters_from_file(
        sample_clustering_csv, samples, prefix
    )

    assert not_found == ["sample5"]
    assert external_clusters == {
        "sample1": "PRE10",
        "sample2": "PRE20",  # lowest cluster number
    }


def test_setup_output_directory():
    hash = "unit_test_get_clusters_internal"

    outdir = assignClusters.setup_output_directory(fs, hash)

    assert outdir == fs.output(hash)


def test_create_sketches_dict():
    sketches = {
        "e868c76fec83ee1f69a95bd27b8d5e76": fs.input.get(
            "e868c76fec83ee1f69a95bd27b8d5e76"
        ),
        "f3d9b387e311d5ab59a8c08eb3545dbb": fs.input.get(
            "f3d9b387e311d5ab59a8c08eb3545dbb"
        ),
    }

    sketches_dict = assignClusters.create_sketches_dict(
        list(sketches.keys()), fs
    )

    assert sketches_dict == sketches


@patch("beebop.assignClusters.hex_to_decimal")
@patch("beebop.assignClusters.sketch_to_hdf5")
def test_preprocess_sketches(mock_sketch_to_hdf5, mock_hex_to_decimal):
    sketches = {"hash1": "sketch sample 1", "hash2": "sketch sample 2"}
    outdir = "outdir"
    mock_sketch_to_hdf5.return_value = list(sketches.keys())

    hashes = assignClusters.preprocess_sketches(sketches, outdir)

    assert hashes == list(sketches.keys())
    mock_hex_to_decimal.assert_called_once_with(sketches)
    mock_sketch_to_hdf5.assert_called_once_with(sketches, outdir)


def test_assign_query_clusters(mocker, config):
    samples = ["sample1", "sample2"]
    wrapper = Mock()
    mocker.patch("beebop.assignClusters.PoppunkWrapper", return_value=wrapper)

    assignClusters.assign_query_clusters(
        config, config.ref_db_fs, samples, config.out_dir
    )

    wrapper.assign_clusters.assert_called_once_with(
        config.db_funcs, samples, config.out_dir
    )


def test_handle_external_clusters_all_found(mocker, config):
    external_clusters, not_found = {
        "sample1": "GPSC69",
        "sample2": "GPSC420",
    }, []
    mocker.patch(
        "beebop.assignClusters.get_external_clusters_from_file",
        return_value=(external_clusters, not_found),
    )
    mock_save_clusters = mocker.patch(
        "beebop.assignClusters.save_external_to_poppunk_clusters"
    )
    config.fs.previous_query_clustering.return_value = (
        "previous_query_clustering"
    )

    res = assignClusters.handle_external_clusters(
        config, {}, ["sample1", "sample2"], [1, 2]
    )

    assert res == {
        0: {"hash": "sample1", "cluster": "GPSC69"},
        1: {"hash": "sample2", "cluster": "GPSC420"},
    }
    mock_save_clusters.assert_called_once_with(
        ["sample1", "sample2"],
        [1, 2],
        external_clusters,
        config.p_hash,
        config.fs,
    )


def test_handle_external_clusters_with_not_found(mocker, config):
    q_names = ["sample1", "sample2", "sample3"]
    q_clusters = [1, 2, 1000]
    not_found_q_clusters = {1234, 6969}
    external_clusters, not_found = {
        "sample1": "GPSC69",
        "sample2": "GPSC420",
    }, ["sample3"]
    mocker.patch(
        "beebop.assignClusters.get_external_clusters_from_file",
        return_value=(external_clusters, not_found),
    )
    mock_save_clusters = mocker.patch(
        "beebop.assignClusters.save_external_to_poppunk_clusters"
    )
    # mock function calls for not found queries
    mock_filter_queries = mocker.patch(
        "beebop.assignClusters.filter_queries",
        return_value=(q_names, q_clusters, not_found_q_clusters),
    )
    mock_handle_not_found = mocker.patch(
        "beebop.assignClusters.handle_not_found_queries",
        return_value=(q_names, q_clusters),
    )
    mock_update_external_clusters = mocker.patch(
        "beebop.assignClusters.update_external_clusters"
    )
    mock_shutil_rmtree = mocker.patch("shutil.rmtree")

    tmp_output = "output_tmp"
    config.fs.previous_query_clustering.return_value = (
        "previous_query_clustering"
    )
    config.fs.output_tmp.return_value = tmp_output

    res = assignClusters.handle_external_clusters(
        config, {}, q_names, q_clusters
    )

    # not found function calls
    mock_filter_queries.assert_called_once_with(q_names, q_clusters, not_found)
    mock_handle_not_found.assert_called_once_with(
        config, {}, not_found, tmp_output, not_found_q_clusters
    )
    mock_update_external_clusters.assert_called_once_with(
        config, not_found, external_clusters, "previous_query_clustering"
    )
    mock_shutil_rmtree.assert_called_once_with(tmp_output)

    # check return calls
    assert res == {
        0: {"hash": "sample1", "cluster": "GPSC69"},
        1: {"hash": "sample2", "cluster": "GPSC420"},
    }
    mock_save_clusters.assert_called_once_with(
        q_names, q_clusters, external_clusters, config.p_hash, config.fs
    )


@patch("beebop.assignClusters.sketch_to_hdf5")
@patch("beebop.assignClusters.assign_query_clusters")
@patch("beebop.assignClusters.summarise_clusters")
@patch("beebop.assignClusters.handle_files_manipulation")
def test_handle_not_found_queries(
    mock_files_manipulation,
    mock_summarise,
    mock_assign,
    mock_sketch_to_hdf5,
    config,
):
    sketches = {"hash1": "sketch sample 1", "hash2": "sketch sample 2"}
    not_found = ["hash2"]
    not_found_query_clusters = {6969}
    output_dir = "output_dir"
    mock_summarise.return_value = ["hash1"], [10], "", "", "", "", ""

    query_names, query_clusters = assignClusters.handle_not_found_queries(
        config, sketches, not_found, output_dir, not_found_query_clusters
    )

    mock_sketch_to_hdf5.assert_called_once_with(
        {"hash2": "sketch sample 2"}, output_dir
    )
    mock_assign.assert_called_once_with(
        config, config.full_db_fs, not_found, output_dir
    )
    mock_files_manipulation.assert_called_once_with(
        config, output_dir, not_found_query_clusters
    )
    assert query_names == ["hash1"]
    assert query_clusters == [10]


@patch("beebop.assignClusters.merge_txt_files")
@patch("beebop.assignClusters.copy_include_files")
@patch("beebop.assignClusters.delete_include_files")
def test_handle_files_manipulation(mock_delete, mock_copy, mock_merge, config):
    outdir_tmp = "outdir_tmp"
    not_found_query_clusters = {1234, 6969}
    config.fs.partial_query_graph.return_value = "partial_query_graph"
    config.fs.partial_query_graph_tmp.return_value = "partial_query_graph_tmp"

    assignClusters.handle_files_manipulation(
        config, outdir_tmp, not_found_query_clusters
    )

    mock_delete.assert_called_once_with(
        config.fs, config.p_hash, not_found_query_clusters
    )
    mock_copy.assert_called_once_with(outdir_tmp, config.out_dir)
    mock_merge.assert_called_once_with(
        "partial_query_graph", "partial_query_graph_tmp"
    )


@patch("beebop.assignClusters.update_external_clusters_csv")
@patch("beebop.assignClusters.get_external_clusters_from_file")
def test_update_external_clusters(
    mock_get_external_clusters, mock_update_external_clusters, config
):
    previous_query_clustering = "previous_query_clustering"
    config.fs.external_previous_query_clustering_tmp.return_value = (
        "tmp_previous_query_clustering"
    )
    not_found = ["sample3", "samples4"]
    external_clusters = {"sample1": "GPSC69", "sample2": "GPSC420"}
    new_external_clusters = {"sample3": "GPSC11", "samples4": "GPSC33"}
    mock_get_external_clusters.return_value = (new_external_clusters, [])

    assignClusters.update_external_clusters(
        config, not_found, external_clusters, previous_query_clustering
    )

    mock_get_external_clusters.assert_called_once_with(
        "tmp_previous_query_clustering",
        not_found,
        config.external_clusters_prefix,
    )
    mock_update_external_clusters.assert_called_once_with(
        previous_query_clustering, "tmp_previous_query_clustering", not_found
    )

    assert external_clusters == {
        "sample1": "GPSC69",
        "sample2": "GPSC420",
        "sample3": "GPSC11",
        "samples4": "GPSC33",
    }


def test_merge_partial_query_graphs(tmp_path, config):
    tmp_file = tmp_path / "tmp_query.subset"
    main_file = tmp_path / "main_query.subset"

    tmp_file.write_text("sample2\nsample10\n")
    main_file.write_text("sample1\nsample2\nsample3\nsample4\n")

    assignClusters.merge_txt_files(main_file, tmp_file)

    main_file_queries = list(main_file.read_text().splitlines())

    assert len(main_file_queries) == 5
    assert sorted(main_file_queries) == sorted(
        ["sample1", "sample2", "sample3", "sample4", "sample10"]
    )


def test_copy_include_files_no_conflict(tmp_path):
    output_full_tmp = tmp_path / "output_full_tmp"
    outdir = tmp_path / "outdir"
    output_full_tmp.mkdir()
    outdir.mkdir()

    # Create some dummy include files in output_full_tmp
    include_files = ["include_1.txt", "include_2.txt", "include_test.txt"]
    other_files = ["other.txt", "data.csv"]

    # Create include files
    for f in include_files:
        (output_full_tmp / f).write_text("test content")

    # Create other non-include files
    for f in other_files:
        (output_full_tmp / f).write_text("other content")

    # Run the function
    assignClusters.copy_include_files(str(output_full_tmp), str(outdir))

    # Check include files were copied
    for f in include_files:
        assert not (output_full_tmp / f).exists()  # Original removed
        assert (outdir / f).exists()  # New location exists
        assert (outdir / f).read_text() == "test content"  # Content preserved

    # Check non-include files were not copied
    for f in other_files:
        assert (output_full_tmp / f).exists()  # Still in original location
        assert not (outdir / f).exists()  # Not in new location


def test_copy_include_file_conflict(tmp_path):
    output_full_tmp = tmp_path / "output_full_tmp"
    outdir = tmp_path / "outdir"
    output_full_tmp.mkdir()
    outdir.mkdir()

    include_files_tmp = [
        "include_1.txt",
    ]
    include_files = ["include_1.txt"]

    # Create include files
    (output_full_tmp / include_files_tmp[0]).write_text("new content")
    (outdir / include_files[0]).write_text("original content")

    assignClusters.copy_include_files(str(output_full_tmp), str(outdir))

    assert not (
        output_full_tmp / include_files_tmp[0]
    ).exists()  # Original removed
    included_file_content = (outdir / include_files[0]).read_text()
    assert "new content" in included_file_content  # New content
    assert "original content" in included_file_content  # Original content


def test_filter_queries():
    q_names = ["sample1", "sample2", "sample3"]
    q_clusters = [1, 2, 3]
    not_found = ["sample2"]

    filtered_names, filtered_clusters, not_found_q_clusters = (
        assignClusters.filter_queries(q_names, q_clusters, not_found)
    )

    assert filtered_names == ["sample1", "sample3"]
    assert filtered_clusters == [1, 3]
    assert not_found_q_clusters


def test_delete_include_files(tmp_path):
    fs = Mock()
    fs.include_files.side_effect = lambda _p_hash, cluster: str(
        tmp_path / f"inlude_{cluster}.txt"
    )
    clusters = [10, 15, 20]

    for cluster in clusters:
        include_file = tmp_path / f"inlude_{cluster}.txt"

    assignClusters.delete_include_files(fs, "test_hash", clusters)

    assert fs.include_files.call_count == len(clusters)
    for cluster in clusters:
        fs.include_files.assert_any_call("test_hash", cluster)
        assert not (tmp_path / f"inlude_{cluster}.txt").exists()


def test_assign_clusters_to_result_dict_items():
    query_cluster_mapping = {
        "sample1": "GPSC69",
        "sample2": "GPSC420",
    }

    result = assignClusters.assign_clusters_to_result(
        query_cluster_mapping.items()
    )

    assert result == {
        0: {"hash": "sample1", "cluster": "GPSC69"},
        1: {"hash": "sample2", "cluster": "GPSC420"},
    }


def test_assign_clusters_to_result_zip():
    assign_qnames = ["sample1", "sample2"]
    assign_clusters = [1, 2]

    result = assignClusters.assign_clusters_to_result(
        zip(assign_qnames, assign_clusters)
    )

    assert result == {
        0: {"hash": "sample1", "cluster": 1},
        1: {"hash": "sample2", "cluster": 2},
    }


def test_save_result(tmp_path, config):
    assign_result = {
        0: {"hash": "sample1", "cluster": 1},
        1: {"hash": "sample2", "cluster": 2},
    }
    result_path = tmp_path / "output.pkl"
    config.fs.output_cluster.return_value = str(result_path)

    assignClusters.save_result(config, assign_result)

    config.fs.output_cluster.assert_called_once_with(config.p_hash)
    assert result_path.exists()
    with open(result_path, "rb") as f:
        assert assign_result == pickle.load(f)


def test_save_external_to_poppunk_clusters(tmp_path):
    q_names = ["sample1", "sample2"]
    q_clusters = [1, 2]
    external_clusters = {"sample1": "GPSC69", "sample2": "GPSC420"}
    fs = Mock()
    external_clusters_path = tmp_path / "external_clusters.pkl"
    fs.external_to_poppunk_clusters.return_value = str(external_clusters_path)

    assignClusters.save_external_to_poppunk_clusters(
        q_names, q_clusters, external_clusters, "test_hash", fs
    )

    fs.external_to_poppunk_clusters.assert_called_once_with("test_hash")
    assert external_clusters_path.exists()
    with open(external_clusters_path, "rb") as f:
        assert pickle.load(f) == {
            "GPSC69": "1",
            "GPSC420": "2",
        }


def test_get_component_filenames(tmp_path):
    network_folder = tmp_path / "network"
    network_folder.mkdir()

    # Create matching files
    expected_files = [
        network_folder / "network_component_1;88.graphml",
        network_folder / "network_component_2.graphml",
    ]
    for f in expected_files:
        f.touch()

    # Create non-matching files
    (network_folder / "other_file.txt").touch()
    (network_folder / "network_other.graphml").touch()

    result = utils.get_component_filenames(str(network_folder))

    assert len(result) == 2
    assert sorted(result) == sorted([str(f) for f in expected_files])


def test_get_df_filtered_by_samples(sample_clustering_csv):
    """Test getting mask for existing samples"""
    samples = ["sample1", "sample3"]

    filtered_df = utils.get_df_filtered_by_samples(
        sample_clustering_csv, samples
    )

    # Check DataFrame
    assert isinstance(filtered_df, pd.DataFrame)
    assert len(filtered_df) == 2
    assert list(filtered_df["sample"]) == ["sample1", "sample3"]


@patch("beebop.utils.build_subgraph")
@patch("beebop.utils.write_graphml")
@patch("beebop.utils.add_query_ref_to_graph")
@patch("beebop.utils.get_component_filenames")
def test_create_subgraphs(
    mock_get_component_filenames,
    mock_add_query_ref_to_graph,
    mock_write_graphml,
    mock_build_subgraph,
):
    mock_get_component_filenames.return_value = [
        "network_component_1.graphml",
    ]
    mock_subgraph = Mock()
    mock_build_subgraph.return_value = mock_subgraph
    filename_dict = {
        "filehash1": "filename1",
        "filehash2": "filename2",
    }
    query_names = list(filename_dict.values())

    utils.create_subgraphs("network_folder", filename_dict)

    mock_build_subgraph.assert_called_once_with(
        "network_component_1.graphml", query_names
    )
    mock_add_query_ref_to_graph.assert_called_once_with(
        mock_subgraph, query_names
    )
    mock_write_graphml.assert_called_once_with(
        mock_subgraph, "pruned_network_component_1.graphml"
    )


@patch("beebop.utils.read_graphml")
def test_build_subgraph(mock_read_graphml):
    graph = nx.complete_graph(50)  # 50 nodes fully conected
    query_names = ["sample1", "sample2", "sample3"]
    graph.nodes[45]["id"] = "sample2"
    mock_read_graphml.return_value = graph

    subgraph = utils.build_subgraph("network_component_1.graphml", query_names)

    assert len(subgraph.nodes) == 30  # max number
    assert subgraph.has_node(45) is True


def test_add_query_ref_to_graph():
    graph = nx.complete_graph(10)  # 10 nodes fully conected
    query_names = ["sample1", "sample2", "sample3"]
    graph.nodes[0]["id"] = "sample2"

    utils.add_query_ref_to_graph(graph, query_names)

    assert graph.nodes[0]["ref_query"] == "query"
    for i in range(1, 10):
        assert graph.nodes[i]["ref_query"] == "ref"


def create_test_files(network_folder, filenames):
    """Helper to create test files in the network folder"""
    for filename in filenames:
        filepath = os.path.join(network_folder, filename)
        with open(filepath, "w") as f:
            f.write("test content")


def test_replace_merged_component_filenames(tmp_path):
    network_dir = tmp_path / "network"
    network_dir.mkdir()
    network_folder = str(network_dir)
    create_test_files(
        network_folder,
        [
            "network_component_10.graphml",
            "network_component_15;5;25.graphml",
            "network_component_6;4;2.graphml",
            "network_component_2.graphml",
        ],
    )

    utils.replace_merged_component_filenames(network_folder)

    assert os.path.exists(
        os.path.join(network_folder, "network_component_10.graphml")
    )
    assert os.path.exists(
        os.path.join(network_folder, "network_component_5.graphml")
    )
    assert os.path.exists(
        os.path.join(network_folder, "network_component_2.graphml")
    )


def test_get_external_cluster_nums(sample_clustering_csv):
    samples = ["sample1", "sample2"]

    result = utils.get_external_cluster_nums(sample_clustering_csv, samples)

    assert result == {
        "sample1": "10",
        "sample2": "309;20;101",
    }
