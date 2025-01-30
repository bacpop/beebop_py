import json
import os
import jsonschema
from tests import setup
import beebop.schemas


def read_data(response):
    return json.loads(response.data.decode("utf-8"))["data"]


def microreact_status_finished(client, p_hash):
    status = client.get("/status/" + p_hash)
    microreact_clusters_status = read_data(status)["microreactClusters"]
    assert len(microreact_clusters_status) > 0
    assert all(
        status == "finished" for status in microreact_clusters_status.values()
    )


def network_status_finished(client, p_hash):
    status = client.get("/status/" + p_hash)
    assert read_data(status)["network"] == "finished"


def assign_status_finished(client, p_hash):
    status = client.get("/status/" + p_hash)
    assert read_data(status)["assign"] == "finished"


def assert_status_present(client, p_hash):
    status = client.get("/status/" + p_hash)
    status_options = ["queued", "started", "finished", "waiting", "deferred"]
    assert read_data(status)["assign"] in status_options
    assert read_data(status)["microreact"] in status_options
    assert read_data(status)["network"] in status_options


def assert_all_finished(project_data):
    assert project_data["status"]["assign"] == "finished"
    assert project_data["status"]["microreact"] == "finished"
    assert project_data["status"]["network"] == "finished"


def run_assign_and_validate(client, p_hash):
    schemas = beebop.schemas.Schema()
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


def assert_correct_poppunk_results(client, p_hash, qtbot, cluster_nums):
    # retrieve job status
    assert_status_present(client, p_hash)

    # retrieve cluster result when finished
    qtbot.waitUntil(
        lambda: assign_status_finished(client, p_hash), timeout=20000
    )
    run_assign_and_validate(client, p_hash)

    # check if visualisation files are stored
    qtbot.waitUntil(
        lambda: network_status_finished(client, p_hash), timeout=300000
    )

    for cluster_num in cluster_nums:
        assert os.path.exists(
            setup.output_folder
            + p_hash
            + f"/network/network_component_{cluster_num}.graphml"
        )
    qtbot.waitUntil(
        lambda: microreact_status_finished(client, p_hash), timeout=300000
    )
    for cluster_num in cluster_nums:
        assert os.path.exists(
            setup.output_folder
            + p_hash
            + f"/microreact_{cluster_num}/microreact_{cluster_num}.microreact"
        )
