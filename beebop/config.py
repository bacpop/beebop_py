import os
from types import SimpleNamespace
import json


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
    with open("./beebop/resources/args.json") as a:
        args_json = a.read()
    return json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))
