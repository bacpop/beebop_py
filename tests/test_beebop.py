'''test functions for unit and integration tests'''

import json
import jsonschema

from beebop import __version__
from beebop import addition
from beebop import versions


def test_version():
    '''test the version'''
    assert __version__ == '0.1.0'


def test_add():
    '''test addition'''
    assert addition.add(5, 4) == 9
    assert addition.add(5, 10) == 15


def test_get_version():
    '''test function that returns version number'''
    assert versions.get_version() == [{"name": "beebop", "version": "0.1.0"}]


def test_request_version(client):
    '''test /version endpoint'''
    response = client.get("/version")

    with open('spec/version.schema.json', 'r', encoding="utf-8") as file:
        schema_data = file.read()
    schema = json.loads(schema_data)

    assert jsonschema.validate(json.loads(response.data.decode("utf-8")), schema) is None
