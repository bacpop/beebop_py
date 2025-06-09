from typing import Union, Literal

from rq.job import Job
from werkzeug.exceptions import NotFound

from beebop.models import ResponseError
from beebop.config import RedisManager


def get_project_status(
    p_hash: str, redis_manager: RedisManager
) -> Union[dict, ResponseError]:
    """
    [returns statuses of all jobs from a given project (cluster assignment,
    initial visualisations job that kicks off all other jobs
    ,and visualisations for all clusters)]

    :param p_hash: [project hash]
    :param redis_manager: [RedisManager instance]
    :return: [dict with job statuses]
    """
    redis_manager.check_redis_connection()

    try:
        status_assign = get_status_job("assign", p_hash, redis_manager)
        if status_assign == "finished":
            visualise = get_status_job("visualise", p_hash, redis_manager)
            visualise_cluster_statuses = get_visualisation_statuses(
                p_hash, redis_manager
            )
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


def get_status_job(
    job_type: Literal["assign", "visualise"],
    p_hash: str,
    redis_manager: RedisManager,
) -> str:
    id = redis_manager.get_job_status(job_type, p_hash).decode("utf-8")
    return Job.fetch(id, connection=redis_manager.redis).get_status()


def get_visualisation_statuses(
    p_hash: str, redis_manager: RedisManager
) -> dict:
    """
    [returns statuses of all visualisation jobs for a given project hash]

    :param p_hash: [project hash]
    :param redis_manager: [RedisManager instance]
    :return: [dict with cluster visualisation job statuses]
    """
    return {
        cluster.decode("utf-8"): Job.fetch(
            status.decode("utf-8"), connection=redis_manager.redis
        ).get_status()
        for cluster, status in redis_manager.get_visualisation_statuses(
            p_hash
        ).items()
    }
