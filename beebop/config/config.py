import json
import os
from pathlib import PurePath
from types import SimpleNamespace

from redis import Redis

from .schemas import Schema


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""

    pass


def get_environment() -> tuple[str, str, str]:
    """
    Get the environment variables required for the application.

    :return: A tuple containing the storage location, database location,
             and Redis host.
    """
    storage_location = os.getenv("STORAGE_LOCATION")
    dbs_location = os.getenv("DBS_LOCATION")
    redis_host = os.getenv("REDIS_HOST")
    if not storage_location:
        raise ConfigurationError(
            "STORAGE_LOCATION environment variable is not set."
        )
    if not dbs_location:
        raise ConfigurationError(
            "DBS_LOCATION environment variable is not set."
        )
    if not redis_host:
        redis_host = "127.0.0.1"
    return storage_location, dbs_location, redis_host


def get_args() -> SimpleNamespace:
    """
    [Read in fixed arguments to poppunk that are always set, or used as
    defaults. This is needed because of the large number of arguments that
    poppunk needs]

    :return dict: [arguments loaded from json]
    """
    with open(str(PurePath("beebop", "resources", "args.json"))) as a:
        args_json = a.read()
    return json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))


class Config:
    """
    Configuration class to hold application settings.
    """

    def __init__(self):
        storage_location, dbs_location, redis_host = get_environment()
        self.storage_location = storage_location
        self.dbs_location = dbs_location
        self.redis_host = redis_host
        self.args = get_args()
        self.job_timeout = 1200  # seconds
        self.schemas = Schema()
        self.redis = Redis(host=self.redis_host)
