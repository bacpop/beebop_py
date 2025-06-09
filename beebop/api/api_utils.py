from typing import Any
from beebop.models import ResponseError, ResponseBody
from flask import jsonify, Response


def response_success(data: Any) -> Response:
    """
    :param data: [data to be stored in response object]
    :return dict: [response object for successful response holding data]
    """
    response = ResponseBody(status="success", errors=[], data=data)
    return jsonify(response)


def response_failure(error_message: str, error_detail: str) -> Response:
    """
    :param error_message: [error message to be returned]
    :param error_detail: [detailed error information]
    :return Response: [response object for error
    response holding error message]
    """
    response = ResponseBody(
        status="failure",
        errors=[ResponseError(error=error_message, detail=error_detail)],
        data=[],
    )
    return jsonify(error=response)
