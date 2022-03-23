import jsonschema
import json

from beebop import __version__ as beebop_version
from beebop import versions
import beebop.schemas
from tests import setup

schemas = beebop.schemas.Schema()


def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version}]


def test_setup():
    assert jsonschema.validate(json.loads(
        setup.generate_json()), schemas.sketch) is None
