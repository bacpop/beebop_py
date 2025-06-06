import logging

from flask import Flask
from waitress import serve

from .api.error_handlers import register_error_handlers
from .api.routes import register_routes
from .config import Config


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.update(Config().__dict__)
    logging.basicConfig(level=logging.INFO)

    register_error_handlers(app)
    register_routes(app)

    return app


if __name__ == "__main__":
    app = create_app()
    serve(app)  # pragma: no cover
