import pytest

from beebop.app import app as flask_app


@pytest.fixture()
def app():
    app = flask_app
    app.config.update({
        "TESTING": True,
    })

    yield app


@pytest.fixture()
def client(app):
    # Establish an application context before running the tests.
    ctx = app.app_context()
    ctx.push()

    return app.test_client()
