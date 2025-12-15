from typing import Literal, Optional, Union

from rq.job import Job, JobStatus
from werkzeug.exceptions import NotFound

from beebop.db import RedisManager
from beebop.models import ResponseError


def get_project_status(p_hash: str, redis_manager: RedisManager) -> Union[dict, ResponseError]:
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
        sublineage_assign_status = get_sublineage_assign_status(p_hash, redis_manager)
        if status_assign == "finished":
            visualise = get_status_job("visualise", p_hash, redis_manager)
            visualise_cluster_statuses = get_visualisation_statuses(p_hash, redis_manager)
        else:
            visualise = "waiting"
            visualise_cluster_statuses = {}

        result = {
            "assign": status_assign,
            "visualise": visualise,
            "visualiseClusters": visualise_cluster_statuses,
        }
        if sublineage_assign_status is not None:
            result["sublineage_assign"] = sublineage_assign_status
        return result
    except AttributeError as exc:
        raise NotFound("Unknown project hash") from exc


def get_sublineage_assign_status(p_hash: str, redis_manager: RedisManager) -> Optional[JobStatus]:
    """
    [Get status of sub-lineage assign job.
    This job is optional and may not exist for all projects.]

    :param p_hash: [hash of project]
    :param redis_manager: [RedisManager instance]
    :return: [JobStatus: [status of the job]
    """
    try:
        return get_status_job("sublineage_assign", p_hash, redis_manager)
    except AttributeError:
        return None


def get_status_job(
    job_type: Literal["assign", "visualise", "sublineage_assign"],
    p_hash: str,
    redis_manager: RedisManager,
) -> JobStatus:
    """
    [Get status of rq job]

    :param job_type: [type of job, either assign or visualise]
    :param p_hash: [hash of project]
    :param redis_manager: [RedisManager instance]
    :return: [JobStatus: [status of the job]
    """
    job_id = redis_manager.get_job_status(job_type, p_hash).decode("utf-8")
    return Job.fetch(job_id, connection=redis_manager.redis).get_status()


def get_visualisation_statuses(p_hash: str, redis_manager: RedisManager) -> dict:
    """
    [returns statuses of all visualisation jobs for a given project hash]

    :param p_hash: [project hash]
    :param redis_manager: [RedisManager instance]
    :return: [dict with cluster visualisation job statuses]
    """
    return {
        cluster.decode("utf-8"): Job.fetch(status.decode("utf-8"), connection=redis_manager.redis).get_status()
        for cluster, status in redis_manager.get_visualisation_statuses(p_hash).items()
    }
