from unittest.mock import Mock, call

import pytest
from werkzeug.exceptions import NotFound

from beebop.services import job_service

mock_redis_manager = Mock()
mock_redis_manager.get_job_status.return_value = b"job_id"
mock_redis_manager.get_visualisation_statuses.return_value = {
    b"GPSC1": b"job_id_1",
    b"GPSC2": b"job_id_2",
}


def test_get_project_status_assign_finished(mocker):
    """
    Test the get_project_status function when the assign job is finished.
    """
    mock_redis_manager.reset_mock(side_effect=True)

    mock_job = Mock()
    mock_job.get_status.return_value = "finished"
    mocker.patch("beebop.services.job_service.Job.fetch", return_value=mock_job)

    status = job_service.get_project_status("test_project_hash", mock_redis_manager)

    assert status == {
        "assign": "finished",
        "visualise": "finished",
        "visualiseClusters": {"GPSC1": "finished", "GPSC2": "finished"},
    }
    assert mock_redis_manager.get_job_status.call_args_list == [
        call("assign", "test_project_hash"),
        call("visualise", "test_project_hash"),
    ]


def test_get_project_status_assign_unfinished(mocker):
    """
    Test the get_project_status function when the assign job is not finished.
    """
    mock_redis_manager.reset_mock(side_effect=True)

    mock_job = Mock()
    mock_job.get_status.return_value = "running"
    mocker.patch("beebop.services.job_service.Job.fetch", return_value=mock_job)

    status = job_service.get_project_status("test_project_hash", mock_redis_manager)

    assert status == {
        "assign": "running",
        "visualise": "waiting",
        "visualiseClusters": {},
    }


def test_get_project_status_attribute_error():
    """
    Test the get_project_status function when an AttributeError is raised.
    """
    mock_redis_manager.reset_mock(side_effect=True)
    mock_redis_manager.get_job_status.side_effect = AttributeError

    with pytest.raises(NotFound, match="Unknown project hash"):
        job_service.get_project_status("test_project_hash", mock_redis_manager)


def test_get_status_job(mocker):
    """
    Test the get_status_job function to ensure it
    returns the correct job status.
    """
    mock_redis_manager.reset_mock(side_effect=True)

    mock_job = Mock()
    mock_job.get_status.return_value = "finished"
    mocker.patch("beebop.services.job_service.Job.fetch", return_value=mock_job)

    status = job_service.get_status_job("assign", "test_project_hash", mock_redis_manager)

    assert status == "finished"
    mock_redis_manager.get_job_status.assert_called_once_with("assign", "test_project_hash")


def test_get_visualisation_statuses(mocker):
    """
    Test the get_visualisation_statuses
    function to ensure it returns the correct statuses.
    """
    mock_redis_manager.reset_mock(side_effect=True)
    mock_job_1 = Mock()
    mock_job_1.get_status.return_value = "finished"

    mock_job_2 = Mock()
    mock_job_2.get_status.return_value = "running"

    mocker.patch(
        "beebop.services.job_service.Job.fetch",
        side_effect=[mock_job_1, mock_job_2],
    )

    statuses = job_service.get_visualisation_statuses("test_project_hash", mock_redis_manager)

    assert statuses == {
        "GPSC1": "finished",
        "GPSC2": "running",
    }
    mock_redis_manager.get_visualisation_statuses.assert_called_once_with("test_project_hash")
