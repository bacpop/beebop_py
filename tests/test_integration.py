import json
import jsonschema
import os
import re
import beebop.schemas
from tests import setup
from tests.test_utils import (
    read_data,
    assert_all_finished,
    run_poppunk,
    assert_correct_poppunk_results,
)

schemas = beebop.schemas.Schema()


def run_pneumo(client):
    # generate sketches
    sketches = json.loads(setup.generate_json_pneumo())
    name_mapping = {"6930_8_9": "6930_8_9.fa", "7622_5_91": "7622_5_91.fa"}
    # submit new job
    p_hash = "integration_test_run_poppunk_pneumo"

    run_poppunk(
        client,
        p_hash,
        sketches,
        name_mapping,
        setup.species,
        setup.amr_for_metadata_csv,
    )
    assert_correct_poppunk_results(client, p_hash, [3, 60])

    return (p_hash, sketches)


def test_request_version(client):
    response = client.get("/version")
    schema = schemas.version
    assert (
        jsonschema.validate(
            json.loads(response.data.decode("utf-8"))["data"], schema
        )
        is None
    )


def test_run_poppunk_pneumo(client):
    p_hash, sketches = run_pneumo(client)

    # check can load project data from client
    project_response = client.get("/project/" + p_hash)
    project_data = read_data(project_response)
    assert project_data["hash"] == p_hash
    assert len(project_data["samples"]) == 2

    # check response data matches the generated data
    assert (
        project_data["samples"]["7622_5_91"]["sketch"] == sketches["7622_5_91"]
    )
    assert project_data["samples"]["7622_5_91"]["cluster"] == "GPSC3"
    assert (
        project_data["samples"]["6930_8_9"]["sketch"] == sketches["6930_8_9"]
    )
    assert project_data["samples"]["6930_8_9"]["cluster"] == "GPSC60"
    assert_all_finished(project_data)


def test_project_not_found(client):
    response = client.get("/project/not_a_hash")
    assert response.status_code == 404
    response = json.loads(response.data)["error"]
    assert response["status"] == "failure"
    err = response["errors"][0]
    assert err["error"] == "Project hash not found"
    assert err["detail"] == "Project hash does not have an associated job"


def test_results_microreact(client):
    p_hash = "test_microreact_api"
    cluster = 7
    api_token = os.environ["MICROREACT_TOKEN"]
    invalid_token = "invalid_token"
    response = client.post(
        "/results/microreact",
        json={
            "projectHash": p_hash,
            "cluster": cluster,
            "apiToken": api_token,
        },
    )
    print(read_data(response)["url"])
    assert re.match(
        "https://microreact.org/project/.*cluster-7*",
        read_data(response)["url"],
    )
    error_response = client.post(
        "/results/microreact",
        json={
            "projectHash": p_hash,
            "cluster": cluster,
            "apiToken": invalid_token,
        },
    )
    error = json.loads(error_response.data)["error"]
    assert error["status"] == "failure"
    assert error["errors"][0]["error"] == "Wrong Token"


def test_network_results_zip(client):
    p_hash = "test_network_zip"
    type = "network"
    response = client.post(
        "/results/zip",
        json={"projectHash": p_hash, "cluster": "GPSC38", "type": type},
    )
    assert "visualise_38_component_38.graphml".encode("utf-8") in response.data
    assert (
        "pruned_visualise_38_component_38.graphml".encode("utf-8")
        in response.data
    )
    assert "visualise_38_cytoscape.csv".encode("utf-8") in response.data


def test_get_network_graphs(client):
    p_hash, _ = run_pneumo(client)

    response = client.get(f"/results/networkGraphs/{p_hash}")
    graph_string_3 = json.loads(response.data.decode("utf-8"))["data"]["GPSC3"]
    graph_string_60 = json.loads(response.data.decode("utf-8"))["data"][
        "GPSC60"
    ]
    assert response.status_code == 200
    for graph_string in [graph_string_3, graph_string_60]:
        assert all(
            x in graph_string
            for x in ["</graph>", "</graphml>", "</node>", "</edge>"]
        )


def test_404(client):
    response = client.get("/random_path")
    assert response.status_code == 404
    response_data = response.data.decode("utf-8")
    assert json.loads(response_data)["error"]["status"] == "failure"


def test_run_poppunk_streptococcus_agalactiae(client):
    p_hash = "integration_test_run_poppunk_streptococcus_agalactiae"
    sketch_hash = "strep_sample"
    name_mapping = {
        sketch_hash: "name1.fa",
    }
    with open("tests/files/sketches/strep_sample.json") as f:
        sketch = json.load(f)

    run_poppunk(
        client,
        p_hash,
        {sketch_hash: sketch},
        name_mapping,
        "Streptococcus agalactiae",
    )

    assert_correct_poppunk_results(client, p_hash, [18])

    # check can load project data from client
    project_response = client.get("/project/" + p_hash)
    project_data = read_data(project_response)
    assert project_data["hash"] == p_hash
    assert_all_finished(project_data)


def test_run_poppunk_streptococcus_pyogenes(client):
    p_hash = "integration_test_run_poppunk_streptococcus_pyogenes"
    sketch_hash = "strep_pyogenes_sample"
    name_mapping = {
        sketch_hash: "name1.fa",
    }
    with open(f"tests/files/sketches/{sketch_hash}.json") as f:
        sketch = json.load(f)

    run_poppunk(
        client,
        p_hash,
        {sketch_hash: sketch},
        name_mapping,
        "Streptococcus pyogenes",
    )

    assert_correct_poppunk_results(client, p_hash, [2])

    # check can load project data from client
    project_response = client.get("/project/" + p_hash)
    project_data = read_data(project_response)
    assert project_data["hash"] == p_hash
    assert_all_finished(project_data)
