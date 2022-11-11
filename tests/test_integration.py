import json
import jsonschema
import os
import beebop.schemas
from tests import setup
import re


schemas = beebop.schemas.Schema()


def read_data(response):
    return json.loads(response.data.decode("utf-8"))['data']


def test_request_version(client):
    response = client.get("/version")
    schema = schemas.version
    assert jsonschema.validate(
        json.loads(response.data.decode("utf-8"))["data"], schema) is None


def test_run_poppunk(client, qtbot):
    # this requires Redis & rqworker to be running
    storage = "./tests/results/poppunk_output/"
    os.makedirs(storage, exist_ok=True)
    # generate sketches
    sketches = json.loads(setup.generate_json())
    name_mapping = {
        "hash1": "name1.fa",
        "hash2": "name2.fa"
        }
    # submit new job
    p_hash = 'integration_test_run_poppunk'
    response = client.post("/poppunk", json={
        'projectHash': p_hash,
        'sketches': sketches,
        'names': name_mapping
        })
    assert response.status_code == 200
    # retrieve job status
    status = client.get("/status/" + p_hash)
    status_options = ['queued', 'started', 'finished', 'waiting']
    assert read_data(status)['assign'] in status_options
    assert read_data(status)['microreact'] in status_options
    assert read_data(status)['network'] in status_options

    # retrieve cluster result when finished
    def assign_status_finished():
        status = client.get("/status/" + p_hash)
        assert read_data(status)['assign'] == 'finished'

    qtbot.waitUntil(assign_status_finished, timeout=200000)
    result = client.post("/results/assign", json={
        'projectHash': p_hash})
    result_object = json.loads(result.data.decode("utf-8"))
    assert result_object["status"] == "success"
    assert jsonschema.validate(result_object["data"], schemas.cluster) is None

    # check if visualisation files are stored
    def microreact_status_finished():
        status = client.get("/status/" + p_hash)
        assert read_data(status)['microreact'] == 'finished'

    qtbot.waitUntil(microreact_status_finished, timeout=100000)
    assert os.path.exists(storage + p_hash +
                          "/microreact_5/microreact_5_core_NJ.nwk")

    def network_status_finished():
        status = client.get("/status/" + p_hash)
        assert read_data(status)['network'] == 'finished'

    qtbot.waitUntil(network_status_finished, timeout=100000)
    assert os.path.exists(storage + p_hash +
                          "/network/network_cytoscape.graphml")
    assert os.path.exists(storage + p_hash +
                          "/network/cluster_component_dict.pickle")


def test_results_microreact(client):
    p_hash = 'test_microreact_api'
    cluster = 7
    api_token = os.environ['MICROREACT_TOKEN']
    invalid_token = 'invalid_token'
    response = client.post("/results/microreact", json={
        'projectHash': p_hash,
        'cluster': cluster,
        'apiToken': api_token})
    assert re.match("https://microreact.org/project/.*-poppunk.*",
                    read_data(response)['url'])
    error_response = client.post("/results/microreact", json={
        'projectHash': p_hash,
        'cluster': cluster,
        'apiToken': invalid_token})
    error = json.loads(error_response.data)["error"]
    assert error["status"] == "failure"
    assert error["errors"][0]["error"] == "Wrong Token"


def test_results_zip(client):
    p_hash = 'test_network_zip'
    type = 'network'
    response = client.post("/results/zip", json={
        'projectHash': p_hash,
        'cluster': 1,
        'type': type})
    assert 'network_component_38.graphml'.encode('utf-8') in response.data
    assert 'network_cytoscape.csv'.encode('utf-8') in response.data
    assert 'network_cytoscape.graphml'.encode('utf-8') in response.data


def test_download_graphml(client):
    p_hash = 'unit_test_graphml'
    cluster = 5
    response = client.post("/results/graphml", json={
        'projectHash': p_hash,
        'cluster': cluster})
    graph_string = json.loads(response.data.decode("utf-8"))['data']['graph']
    assert response.status_code == 200
    assert all(x in graph_string for x in ['</graph>',
                                           '</graphml>',
                                           '</node>',
                                           '</edge>'])


def test_404(client):
    response = client.get("/random_path")
    assert response.status_code == 404
    response_data = response.data.decode("utf-8")
    assert json.loads(response_data)["error"]["status"] == "failure"
