import logging

from flask import Flask
from waitress import serve

from .api import ConfigRoutes, ProjectRoutes, register_error_handlers
from .config import Config


def create_app() -> Flask:
    """
    [Setups flask app with config context,
    routes, logging and exception handlers]

    :return: [Flask app instance]
    """
    app = Flask(__name__)
    app.config.update(Config().__dict__)
    logging.basicConfig(level=logging.INFO)

    # Register error handlers
    register_error_handlers(app)
    # Register blueprints for routes
    app.register_blueprint(ConfigRoutes(app).get_blueprint())
    app.register_blueprint(ProjectRoutes(app).get_blueprint())

    return app


if __name__ == "__main__":
    app = create_app()
    serve(app)  # pragma: no cover
