'''Fixtures to run a test client for testing'''

import pytest
from beebop.app import app as flask_app


@pytest.fixture()
def app():
    '''App fixture'''
    app = flask_app
    app.config.update({
        "TESTING": True,
    })

    yield app


@pytest.fixture()
def client(app):
    '''Client fixture'''
    return app.test_client()


@pytest.fixture()
def runner(app):
    '''Runner fixture'''
    return app.test_cli_runner()
