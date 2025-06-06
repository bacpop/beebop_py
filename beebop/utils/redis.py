from redis.exceptions import ConnectionError
from werkzeug.exceptions import InternalServerError


def check_connection(redis) -> None:
    """
    :param redis: [Redis instance]
    """
    try:
        redis.ping()
    except (ConnectionError, ConnectionRefusedError):
        raise InternalServerError(
            "Redis connection error. Please check if Redis is running."
        )
