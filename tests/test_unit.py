import jsonschema
import json
from PopPUNK import __version__ as poppunk_version

from beebop import __version__ as beebop_version
from beebop import versions
from beebop import assignClusters
import beebop.schemas
from tests import setup

schemas = beebop.schemas.Schema()
storageLocation = './tests/files'


def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version},
        {"name": "poppunk", "version": poppunk_version}]
    assert jsonschema.validate(versions.get_version(),
                               schemas.version) is None


def test_setup():
    assert jsonschema.validate(json.loads(
        setup.generate_json()), schemas.sketches) is None


def test_poppunk_assign():
    assert assignClusters.get_clusters(
        [
            '02ff334f17f17d775b9ecd69046ed296',
            '9c00583e2f24fed5e3c6baa87a4bfa4c',
            '99965c83b1839b25c3c27bd2910da00a'
        ], 'unit_test_poppunk_assign', storageLocation) == {
            0: {'cluster': 9, 'hash': '02ff334f17f17d775b9ecd69046ed296'},
            1: {'cluster': 10, 'hash': '99965c83b1839b25c3c27bd2910da00a'},
            2: {'cluster': 41, 'hash': '9c00583e2f24fed5e3c6baa87a4bfa4c'}}
