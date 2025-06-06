from waitress import serve
from flask import Flask
from .utils import Config
import logging
from .api.error_handlers import register_error_handlers
from .api.routes import register_routes


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
