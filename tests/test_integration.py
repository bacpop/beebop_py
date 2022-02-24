import json
import jsonschema


def test_request_version(client):
    response = client.get("/version")

    with open('spec/version.schema.json', 'r', encoding="utf-8") as file:
        schema_data = file.read()
    schema = json.loads(schema_data)

    assert jsonschema.validate(
        json.loads(response.data.decode("utf-8"))["data"], schema) is None
