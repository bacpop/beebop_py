from unittest.mock import Mock

from redis import Redis

from beebop.config import RedisManager


def test_redis_manager_initialization():
    """
    Test the initialization of RedisManager to
    ensure it sets up the Redis client correctly.
    """
    redis_mock = Mock(spec=Redis)
    redis_manager = RedisManager(redis_mock)
    assert redis_manager.redis == redis_mock
    redis_mock.ping.assert_called_once()  # Ensure connection check was performed


def test_get_job_status():
    """
    Test the get_job_status method to ensure it retrieves the correct job status ID.
    """
    redis_mock = Mock(spec=Redis)
    redis_manager = RedisManager(redis_mock)
    p_hash = "test_project_hash"
    job_type = "assign"
    expected_job_id = b"job123"

    redis_mock.hget.return_value = expected_job_id

    job_id = redis_manager.get_job_status(job_type, p_hash)

    assert job_id == expected_job_id
    redis_mock.hget.assert_called_once_with(
        f"beebop:hash:job:{job_type}", p_hash
    )


def test_set_job_status():
    """
    Test the set_job_status method to ensure it sets the job status ID correctly.
    """
    redis_mock = Mock(spec=Redis)
    redis_manager = RedisManager(redis_mock)
    p_hash = "test_project_hash"
    job_type = "visualise"
    job_id = "job456"

    redis_manager.set_job_status(job_type, p_hash, job_id)

    redis_mock.hset.assert_called_once_with(
        f"beebop:hash:job:{job_type}", p_hash, job_id
    )


def test_delete_visualisation_statuses():
    """
    Test the delete_visualisation_statuses method to
    ensure it deletes the correct visualisation jobs.
    """
    redis_mock = Mock(spec=Redis)
    redis_manager = RedisManager(redis_mock)
    p_hash = "test_project_hash"

    redis_manager.delete_visualisation_statuses(p_hash)

    redis_mock.delete.assert_called_once_with(
        f"beebop:hash:job:visualise:{p_hash}"
    )


def test_get_visualisation_statuses():
    """
    Test the get_visualisation_statuses method to ensure
    it retrieves the correct visualisation job IDs.
    """
    redis_mock = Mock(spec=Redis)
    redis_manager = RedisManager(redis_mock)
    p_hash = "test_project_hash"
    job_type = "visualise"
    expected_job_id = b"job789"

    redis_mock.hgetall.return_value = expected_job_id

    job_id = redis_manager.get_visualisation_statuses(p_hash)

    assert job_id == expected_job_id
    redis_mock.hgetall.assert_called_once_with(
        f"beebop:hash:job:{job_type}:{p_hash}"
    )


def test_set_visualisation_status():
    """
    Test the set_visualisation_status method to
    ensure it sets the visualisation job ID correctly.
    """
    redis_mock = Mock(spec=Redis)
    redis_manager = RedisManager(redis_mock)
    p_hash = "test_project_hash"
    assign_cluster = "cluster1"
    job_id = "job101"

    redis_manager.set_visualisation_status(p_hash, assign_cluster, job_id)

    redis_mock.hset.assert_called_once_with(
        f"beebop:hash:job:visualise:{p_hash}", assign_cluster, job_id
    )
