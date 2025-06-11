from types import SimpleNamespace
from unittest.mock import patch

import pytest
from redis import Redis

from beebop.config.config import Config, ConfigurationError, get_environment
from beebop.config.schemas import Schema


@patch("os.getenv")
def test_get_environment(mock_getenv):
    mock_getenv.side_effect = lambda key: {
        "STORAGE_LOCATION": "/path/to/storage",
        "DBS_LOCATION": "/path/to/dbs",
        "REDIS_HOST": "localhost",
    }.get(key)

    storage_location, dbs_location, redis_host = get_environment()

    assert storage_location == "/path/to/storage"
    assert dbs_location == "/path/to/dbs"
    assert redis_host == "localhost"


@patch("os.getenv")
def test_get_environment_no_redis(mock_getenv):
    mock_getenv.side_effect = lambda key: {
        "STORAGE_LOCATION": "/path/to/storage",
        "DBS_LOCATION": "/path/to/dbs",
    }.get(key)

    storage_location, dbs_location, redis_host = get_environment()

    assert storage_location == "/path/to/storage"
    assert dbs_location == "/path/to/dbs"
    assert redis_host == "127.0.0.1"


@patch("os.getenv")
def test_get_environment_missing_dbs_location(mock_getenv):
    mock_getenv.side_effect = lambda key: {
        "STORAGE_LOCATION": "/path/to/storage",
    }.get(key)

    with pytest.raises(
        ConfigurationError,
        match="DBS_LOCATION environment variable is not set.",
    ) as e_info:
        get_environment()


@patch("os.getenv")
def test_get_environment_missing_storage_location(mock_getenv):
    mock_getenv.side_effect = lambda key: {
        "DBS_LOCATION": "/path/to/dbs",
    }.get(key)

    with pytest.raises(
        ConfigurationError,
        match="STORAGE_LOCATION environment variable is not set.",
    ) as e_info:
        get_environment()


def test_config_setup():
    """
    Test the configuration setup to ensure it initializes correctly.
    """

    config = Config()
    assert config.storage_location is not None
    assert config.dbs_location is not None
    assert config.redis_host is not None
    assert config.job_timeout == 1200  # seconds
    assert isinstance(config.schemas, Schema)
    assert isinstance(config.redis, Redis)
    assert hasattr(config, "args")
    assert isinstance(
        config.args, SimpleNamespace
    )  # Assuming args is loaded as a SimpleNamespace
