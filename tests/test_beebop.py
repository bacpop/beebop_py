from beebop import __version__
from beebop import addition


def test_version():
    assert __version__ == '0.1.0'


def test_add():
    assert addition.add(5, 4) == 9
    assert addition.add(5, 10) == 15
