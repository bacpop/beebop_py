import json
import jsonschema

import beebop.schemas


schemas = beebop.schemas.Schema()


def test_request_version(client):
    response = client.get("/version")
    schema = schemas.version
    assert jsonschema.validate(
        json.loads(response.data.decode("utf-8"))["data"], schema) is None
