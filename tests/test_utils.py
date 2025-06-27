import json
import os
import subprocess
import time
from typing import Callable, Literal

import jsonschema
from redis import Redis
from rq import Queue

from beebop.config import Schema
from tests import hdf5_to_json, setup


def wait_until(condition: Callable[[], bool], interval=300, timeout=10000) -> Literal[True]:
    """
    Wait until a condition is met or timeout occurs.

    :param condition: A callable that returns True when the condition is met.
    :param interval: Time in milliseconds to wait between checks.
    :param timeout: Time in milliseconds to wait before timing out.
    :return: True if the condition is met before timeout.
    :raises TimeoutError: If the timeout period is exceeded.
    """
    interval_seconds = interval / 1000
    timeout_seconds = timeout / 1000

    start_time = time.time()
    while not condition():
        if (time.time() - start_time) > timeout_seconds:
            raise TimeoutError("Condition not met within the timeout period.")
        time.sleep(interval_seconds)
    return True


def read_data(response):
    return json.loads(response.data.decode("utf-8"))["data"]


def visualise_status_finished(client, p_hash):
    status = client.get("/status/" + p_hash)
    visualise_clusters_status = read_data(status)["visualiseClusters"]
    return len(visualise_clusters_status) > 0 and all(
        status == "finished" for status in visualise_clusters_status.values()
    )


def assign_status_finished(client, p_hash):
    status = client.get("/status/" + p_hash)
    return read_data(status)["assign"] == "finished"


def assert_status_present(client, p_hash):
    status = client.get("/status/" + p_hash)
    status_options = ["queued", "started", "finished", "waiting", "deferred"]
    assert read_data(status)["assign"] in status_options
    assert read_data(status)["visualise"] in status_options


def assert_all_finished(project_data):
    assert project_data["status"]["assign"] == "finished"
    assert project_data["status"]["visualise"] == "finished"


def run_assign_and_validate(client, p_hash):
    schemas = Schema()
    result = client.post("/results/assign", json={"projectHash": p_hash})
    result_object = json.loads(result.data.decode("utf-8"))
    assert result_object["status"] == "success"
    assert jsonschema.validate(result_object["data"], schemas.cluster) is None


def run_poppunk(
    client,
    p_hash,
    sketches,
    name_mapping,
    species=setup.species,
    amr_for_metadata_csv=setup.amr_for_metadata_csv,
):
    os.makedirs(setup.output_folder, exist_ok=True)
    response = client.post(
        "/poppunk",
        json={
            "projectHash": p_hash,
            "sketches": sketches,
            "names": name_mapping,
            "species": species,
            "amrForMetadataCsv": amr_for_metadata_csv,
        },
    )
    assert response.status_code == 200


def assert_correct_poppunk_results(client, p_hash, cluster_nums):
    # retrieve job status
    assert_status_present(client, p_hash)

    # retrieve cluster result when finished
    wait_until(lambda: assign_status_finished(client, p_hash), timeout=30000)
    run_assign_and_validate(client, p_hash)

    # check if visualisation files are stored
    wait_until(lambda: visualise_status_finished(client, p_hash), timeout=300000)

    for cluster_num in cluster_nums:
        assert os.path.exists(
            setup.output_folder
            + p_hash
            + f"/visualise_{cluster_num}"
            + f"/visualise_{cluster_num}_component_{cluster_num}.graphml"
        )
    for cluster_num in cluster_nums:
        assert os.path.exists(
            setup.output_folder + p_hash + f"/visualise_{cluster_num}/visualise_{cluster_num}.microreact"
        )


def read_redis(name, key, redis):
    return redis.hget(name, key).decode("utf-8")


def dummy_fct():
    return "Result"


def run_test_job(p_hash):
    # queue example job
    redis = Redis()
    q = Queue(connection=Redis(), is_async=False)
    job_assign = q.enqueue(dummy_fct)
    job_visualise = q.enqueue(dummy_fct)
    job_network = q.enqueue(dummy_fct)
    redis.hset("beebop:hash:job:assign", p_hash, job_assign.id)
    redis.hset("beebop:hash:job:visualise", p_hash, job_visualise.id)
    redis.hset("beebop:hash:job:network", p_hash, job_network.id)


def generate_json_pneumo():
    
    # generate hdf5 sketch from fasta file using pp-sketchlib
    subprocess.run(
        "sketchlib sketch -l sketchlib_input/rfile.txt -o pneumo_sample -s 9984 --cpus 4 -k 14,29,3",
        shell=True,
        cwd="tests/results",
        check=True,
    )

    # translate hdf5 into json
    filepath = "tests/results/pneumo_sample.h5"
    sketches_json = json.loads(hdf5_to_json.h5_to_json(filepath))

    return json.dumps(sketches_json)
