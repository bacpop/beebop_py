from typing import Any
from unittest.mock import patch

from beebop.api.api_utils import response_failure, response_success


@patch("beebop.api.api_utils.jsonify")
def test_response_success(mock_jsonify):
    """
    Test the response_success function to ensure it returns a valid JSON response
    with the expected structure and data.
    """
    test_data = {"key": "value"}
    mock_jsonify.return_value = test_data

    response = response_success(test_data)

    assert response == test_data


@patch("beebop.api.api_utils.jsonify")
def test_response_failure(mock_jsonify):
    """
    Test the response_failure function to ensure it returns a valid JSON response
    with the expected structure and error details.
    """
    error_message = "An error occurred"
    error_detail = "Detailed error information"
    mock_jsonify.return_value = {
        "status": "failure",
        "errors": [{"error": error_message, "detail": error_detail}],
        "data": [],
    }

    response: Any = response_failure(error_message, error_detail)

    assert response["status"] == "failure"
    assert response["errors"][0]["error"] == error_message
    assert response["errors"][0]["detail"] == error_detail
    assert response["data"] == []
