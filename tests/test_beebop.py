from beebop import __version__
from beebop import addition
from beebop import versions
import json
import jsonschema


def test_version():
    assert __version__ == '0.1.0'


def test_add():
    assert addition.add(5, 4) == 9
    assert addition.add(5, 10) == 15


def test_get_version():
    assert versions.get_version(["beebop"]) =="[{\"name\": \"beebop\", \"version\": \"0.1.0\"}]"


def test_request_version(client):
    response = client.get("/version")
    
    with open('spec/version.schema.json', 'r') as f:
        schema_data = f.read()
    schema = json.loads(schema_data)

    assert jsonschema.validate(json.loads(response.data.decode("utf-8")) , schema) == None
    assert response.data == b"[{\"name\": \"beebop\", \"version\": \"0.1.0\"}]"
    
