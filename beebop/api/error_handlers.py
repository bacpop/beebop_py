import logging
from flask import jsonify, Response
from typing import Literal
from beebop.models.dataclasses import ResponseError
from .helpers import response_failure

logger = logging.getLogger(__name__)


def register_error_handlers(app):
    @app.errorhandler(500)
    def internal_server_error(e) -> tuple[Response, Literal[500]]:
        """
        :param e: [error]
        :return Response: [error response object]
        """
        logger.warning(f"Internal Server Error: {e}")
        return (
            jsonify(
                error=response_failure(
                    ResponseError(
                        error="Internal Server Error",
                        detail=str(e.description),
                    )
                )
            ),
            500,
        )

    @app.errorhandler(400)
    def bad_request(e) -> tuple[Response, Literal[400]]:
        """
        :param e: [error]
        :return Response: [error response object]
        """
        logger.warning(f"Bad Request: {e}")
        return (
            jsonify(
                error=response_failure(
                    ResponseError(
                        error="Bad Request", detail=str(e.description)
                    )
                )
            ),
            400,
        )

    @app.errorhandler(404)
    def not_found(e) -> tuple[Response, Literal[404]]:
        """
        :param e: [error]
        :return Response: [error response object]
        """
        logger.warning(f"Not found: {e}")
        return (
            jsonify(
                error=response_failure(
                    ResponseError(
                        error="Resource not found", detail=str(e.description)
                    )
                )
            ),
            404,
        )
