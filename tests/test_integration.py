import json
import jsonschema
import beebop.schemas
from tests import setup


schemas = beebop.schemas.Schema()


def test_request_version(client):
    response = client.get("/version")
    schema = schemas.version
    assert jsonschema.validate(
        json.loads(response.data.decode("utf-8"))["data"], schema) is None


def test_run_poppunk(client, qtbot):
    # this requires Redis & rqworker to be running
    # generate sketches
    sketches = json.loads(setup.generate_json())
    assert jsonschema.validate(sketches, schemas.sketches) is None
    # submit new job
    response = client.post("/poppunk", json={
        'projectHash': 'integration_test_run_poppunk',
        'sketches': sketches})
    assert response.status_code == 200
    # retrieve job status
    status = client.get("/status/integration_test_run_poppunk")
    assert json.loads(status.data.decode("utf-8"))['data'] in ['queued',
                                                               'started',
                                                               'finished']
    # retrieve result when finished

    def status_finished():
        status = client.get("/status/integration_test_run_poppunk")
        assert json.loads(status.data.decode("utf-8"))['data'] == 'finished'

    qtbot.waitUntil(status_finished, timeout=20000)
    result = client.get("/result/integration_test_run_poppunk")
    result_object = json.loads(result.data.decode("utf-8"))
    assert result_object["status"] == "success"
    assert jsonschema.validate(result_object["data"], schemas.cluster) is None
