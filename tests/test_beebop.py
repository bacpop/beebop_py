import json
import jsonschema

from beebop import __version__
from beebop import versions


def test_version():
    assert __version__ == '0.1.0'


def test_get_version():
    assert versions.get_version() == [{"name": "beebop", "version": "0.1.0"}]


def test_request_version(client):
    response = client.get("/version")

    with open('spec/version.schema.json', 'r', encoding="utf-8") as file:
        schema_data = file.read()
    schema = json.loads(schema_data)

    assert jsonschema.validate(
        json.loads(response.data.decode("utf-8"))["data"], schema) is None
