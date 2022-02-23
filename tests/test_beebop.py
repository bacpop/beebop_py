from beebop import __version__
from beebop import addition
from beebop import versions


def test_version():
    assert __version__ == '0.1.0'


def test_add():
    assert addition.add(5, 4) == 9
    assert addition.add(5, 10) == 15


def test_get_version():
    assert versions.get_version(["beebop"]) =="[{\"name\": \"beebop\", \"version\": \"0.1.0\"}]"

