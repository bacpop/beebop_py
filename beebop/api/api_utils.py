from typing import Any
from beebop.models import ResponseError, ResponseBody


def response_success(data: Any) -> ResponseBody:
    """
    :param data: [data to be stored in response object]
    :return dict: [response object for successful response holding data]
    """
    response = ResponseBody(status="success", errors=[], data=data)
    return response


def response_failure(error: ResponseError) -> ResponseBody:
    """
    :param error: [error object with error message and details]
    :return Response: [response object for error
    response holding error message]
    """
    response = ResponseBody(status="failure", errors=[error], data=[])
    return response
