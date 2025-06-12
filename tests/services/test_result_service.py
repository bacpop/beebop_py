from beebop.services.result_service import (
    get_clusters_results,
    generate_zip,
    generate_microreact_url_internal,
    update_microreact_json,
)
from unittest.mock import patch, Mock
from beebop.config import PoppunkFileStore
import os
import pytest
from werkzeug.exceptions import InternalServerError, NotFound
from tests.test_utils import read_data
from tests.setup import storage_location

fs = PoppunkFileStore(storage_location)


def test_update_microreact_json():
    json_microreact = {
        "meta": {"name": "Old Title"},
        "tables": {"table-1": {"columns": [{"field": "ID"}]}},
    }
    cluster_num = "123"

    update_microreact_json(json_microreact, cluster_num)

    # Check title gets updated with correct format
    assert json_microreact["meta"]["name"].startswith(
        f"Cluster {cluster_num} - "
    )
    assert ":" in json_microreact["meta"]["name"]  # Check datetime got added

    # Check expected columns were added
    expected_columns = [
        {"field": "ID"},
        {"field": "Status", "width": 103, "sort": "asc"},
        {"field": "Penicillin Resistance", "width": 183},
        {"field": "Chloramphenicol Resistance", "width": 233},
        {"field": "Erythromycin Resistance", "width": 209},
        {"field": "Tetracycline Resistance", "width": 202},
        {"field": "Cotrim Resistance", "width": 169},
    ]

    assert json_microreact["tables"]["table-1"]["columns"] == expected_columns


@patch("beebop.services.result_service.get_cluster_assignments")
@patch("beebop.services.result_service.get_failed_samples_internal")
def test_get_clusters_results(mock_failed_samples, mock_cluster_assignments):
    mock_cluster_assignments.return_value = {
        0: {"hash": "sample1", "cluster": "A", "raw_cluster_num": 1},
        1: {"hash": "sample2", "cluster": "B", "raw_cluster_num": 2},
    }
    mock_failed_samples.return_value = {
        "sample2": {
            "hash": "sample2",
            "failReasons": [
                "Failed distance QC (too high)",
                "Failed distance QC (too many zeros)",
            ],
        }
    }

    cluster_result = get_clusters_results("test_project", Mock())

    assert cluster_result == {
        "sample1": {"hash": "sample1", "cluster": "A", "raw_cluster_num": 1},
        "sample2": {
            "hash": "sample2",
            "failReasons": [
                "Failed distance QC (too high)",
                "Failed distance QC (too many zeros)",
            ],
        },
    }


@patch("beebop.services.result_service.get_cluster_num", return_value="123")
@patch("beebop.services.result_service.get_network_files_for_zip")
@patch("beebop.services.result_service.add_files")
def test_generate_zip_network(
    mock_add_files, mock_get_network_files_for_zip, mock_get_cluster_num
):
    mock_get_network_files_for_zip.return_value = [
        "file1.graphml",
        "file2.graphml",
    ]
    fs = Mock(spec=PoppunkFileStore)
    fs.output_visualisations.return_value = "/path/to/visualisations"

    zip_path = generate_zip(fs, "test_project", "network", "123")

    mock_get_cluster_num.assert_called_once_with("123")
    mock_get_network_files_for_zip.assert_called_once_with(
        "/path/to/visualisations", "123"
    )
    mock_add_files.assert_called_once_with(
        zip_path,
        "/path/to/visualisations",
        ["file1.graphml", "file2.graphml"],
        exclude=False,
    )


@patch("beebop.services.result_service.get_cluster_num", return_value="123")
@patch("beebop.services.result_service.get_network_files_for_zip")
@patch("beebop.services.result_service.add_files")
def test_generate_zip_microreact(
    mock_add_files, mock_get_network_files_for_zip, mock_get_cluster_num
):
    mock_get_network_files_for_zip.return_value = [
        "file1.graphml",
        "file2.graphml",
    ]
    fs = Mock(spec=PoppunkFileStore)
    fs.output_visualisations.return_value = "/path/to/visualisations"

    zip_path = generate_zip(fs, "test_project", "microreact", "123")

    mock_get_cluster_num.assert_called_once_with("123")
    mock_get_network_files_for_zip.assert_called_once_with(
        "/path/to/visualisations", "123"
    )
    mock_add_files.assert_called_once_with(
        zip_path,
        "/path/to/visualisations",
        ["file1.graphml", "file2.graphml"],
        exclude=True,
    )


@patch("requests.post")
def test_generate_microreact_url_internal(mock_post):
    dummy_url = "https://microreact.org/project/12345-testmicroreactapi"
    mock_post.return_value = Mock(ok=True)
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"url": dummy_url}

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    # for a cluster without tree file
    cluster = "24"

    url = generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        fs,
    )
    print(url)
    assert url == dummy_url
    # for a cluster with tree file
    cluster = "7"
    url2 = generate_microreact_url_internal(
        microreact_api_new_url,
        project_hash,
        cluster,
        api_token,
        fs,
    )

    assert url2 == dummy_url


@patch("requests.post")
def test_generate_microreact_url_internal_API_error_404(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 404
    mock_post.return_value.json.return_value = {"error": "Resource not found"}

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    cluster = "24"

    with pytest.raises(NotFound) as e_info:
        generate_microreact_url_internal(
            microreact_api_new_url,
            project_hash,
            cluster,
            api_token,
            fs,
        )
    assert e_info.value.description == "Cannot reach Microreact API"


@patch("requests.post")
def test_generate_microreact_url_internal_API_error_500(mock_post):
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = 500
    mock_post.return_value.json.return_value = {
        "error": "Internal Server Error"
    }

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    cluster = "24"

    with pytest.raises(InternalServerError) as e_info:
        generate_microreact_url_internal(
            microreact_api_new_url,
            project_hash,
            cluster,
            api_token,
            fs,
        )
    assert (
        e_info.value.description
        == "Microreact reported Internal Server Error. "
        "Most likely Token is invalid!"
    )


@patch("requests.post")
def test_generate_microreact_url_internal_API_other_error(mock_post):
    status_code = 456
    error_text = "random error"
    mock_post.return_value = Mock()
    mock_post.return_value.status_code = status_code
    mock_post.return_value.text = error_text

    microreact_api_new_url = "https://dummy.url"
    project_hash = "test_microreact_api"
    api_token = os.environ["MICROREACT_TOKEN"]
    cluster = "24"

    with pytest.raises(InternalServerError) as e_info:
        generate_microreact_url_internal(
            microreact_api_new_url,
            project_hash,
            cluster,
            api_token,
            fs,
        )
    assert (
        e_info.value.description
        == f"Microreact API returned status code {status_code}. "
        f"Response text: {error_text}."
    )
