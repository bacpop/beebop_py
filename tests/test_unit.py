from beebop import __version__ as beebop_version
from beebop import versions


def test_get_version():
    assert versions.get_version() == [
        {"name": "beebop", "version": beebop_version}]
