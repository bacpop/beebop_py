import logging
from typing import Literal

from flask import Response

from beebop.models import ResponseError

from .api_utils import response_failure

logger = logging.getLogger(__name__)


def register_error_handlers(app) -> None:
    """
    [registers error handlers for common HTTP
    error codes with the Flask app.]

    :param app: [Flask application instance]
    """

    @app.errorhandler(500)
    def internal_server_error(e) -> tuple[Response, Literal[500]]:
        """
        :param e: [error]
        :return Response: [error response object]
        """
        logger.exception(f"Internal Server Error: {e}")
        return (
            response_failure(
                error_message="Internal Server Error",
                error_detail=str(e.description),
            ),
            500,
        )

    @app.errorhandler(400)
    def bad_request(e) -> tuple[Response, Literal[400]]:
        """
        :param e: [error]
        :return Response: [error response object]
        """
        logger.exception(f"Bad Request: {e}")
        return (
            response_failure(
                error_message="Bad Request", error_detail=str(e.description)
            ),
            400,
        )

    @app.errorhandler(404)
    def not_found(e) -> tuple[Response, Literal[404]]:
        """
        :param e: [error]
        :return Response: [error response object]
        """
        logger.exception(f"Not found: {e}")
        return (
            response_failure(
                error_message="Resource not found",
                error_detail=str(e.description),
            ),
            404,
        )
