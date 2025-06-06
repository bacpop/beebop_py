from typing import Union

from redis import Redis
from rq.job import Job
from werkzeug.exceptions import InternalServerError, NotFound

from beebop.models import ResponseError


def get_project_status(
    p_hash: str, redis: Redis
) -> Union[dict, ResponseError]:
    """
    [returns statuses of all jobs from a given project (cluster assignment,
    initial visualisations job that kicks off all other jobs
    ,and visualisations for all clusters)]

    :param p_hash: [project hash]
    :param redis: [Redis instance]
    :return: [dict with job statuses]
    """
    check_redis_connection(redis)

    def get_status_job(job, p_hash, redis):
        id = redis.hget(f"beebop:hash:job:{job}", p_hash).decode("utf-8")
        return Job.fetch(id, connection=redis).get_status()

    try:
        status_assign = get_status_job("assign", p_hash, redis)
        if status_assign == "finished":
            visualise = get_status_job("visualise", p_hash, redis)
            visualise_cluster_statuses = {
                cluster.decode("utf-8"): Job.fetch(
                    status.decode("utf-8"), connection=redis
                ).get_status()
                for cluster, status in redis.hgetall(
                    f"beebop:hash:job:visualise:{p_hash}"
                ).items()
            }
        else:
            visualise = "waiting"
            visualise_cluster_statuses = {}

        return {
            "assign": status_assign,
            "visualise": visualise,  # visualise for all
            "visualiseClusters": visualise_cluster_statuses,
        }
    except AttributeError:
        raise NotFound("Unknown project hash")


def check_redis_connection(redis) -> None:
    """
    :param redis: [Redis instance]
    """
    try:
        redis.ping()
    except (ConnectionError, ConnectionRefusedError):
        raise InternalServerError(
            "Redis connection error. Please check if Redis is running."
        )
