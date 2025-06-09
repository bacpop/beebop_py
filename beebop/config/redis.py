from werkzeug.exceptions import InternalServerError
from typing import Union, Literal


class RedisManager:
    """
    [manages Redis operations for job status tracking and visualization data storage]
    """

    def __init__(self, redis_client):
        """
        [initializes the Redis manager with a Redis client and checks connection]

        :param redis_client: [Redis client instance]
        """
        self.redis = redis_client
        self.check_redis_connection()

    def get_job_status(
        self,
        job_type: Union[Literal["assign"], Literal["visualise"]],
        p_hash: str,
    ) -> bytes:
        """
        [retrieves job status ID for a specific job type and project hash]

        :param job_type: Type of job (assign or visualise)
        :param p_hash: Project hash
        :return: Job ID as bytes
        """
        return self.redis.hget(f"beebop:hash:job:{job_type}", p_hash)

    def set_job_status(
        self,
        job_type: Union[Literal["assign"], Literal["visualise"]],
        p_hash: str,
        job_id: str,
    ) -> None:
        """
        Sets job status ID for a specific job type and project hash.

        :param job_type: Type of job (assign or visualise)
        :param p_hash: Project hash
        :param job_id: Job ID to store
        """
        self.redis.hset(f"beebop:hash:job:{job_type}", p_hash, job_id)

    def delete_job(
        self,
        job_type: Union[Literal["assign"], Literal["visualise"]],
        p_hash: str,
    ) -> None:
        """
        [deletes all jobs of a specific type for a given project hash]

        :param job_type: Type of job (assign or visualise)
        :param p_hash: [project hash]
        """
        self.redis.delete(f"beebop:hash:job:{job_type}:{p_hash}")

    def get_visualisation_statuses(self, p_hash: str) -> dict:
        """
        [retrieves all visualisation job statuses for a given project hash]

        :param p_hash: [project hash]
        :return: [dict mapping cluster names to job IDs]
        """
        return self.redis.hgetall(f"beebop:hash:job:visualise:{p_hash}")

    def set_visualisation_job(
        self, p_hash: str, assign_cluster: str, cluster_visualise_job_id: str
    ) -> None:
        """
        [sets a visualisation job for a specific cluster in a project]

        :param p_hash: [project hash]
        :param assign_cluster: [cluster identifier]
        :param cluster_visualise_job: [visualisation job object with ID attribute]
        """
        self.redis.hset(
            f"beebop:hash:job:visualise:{p_hash}",
            assign_cluster,
            cluster_visualise_job_id,
        )

    def check_redis_connection(self) -> None:
        """
        [checks the Redis connection and raises error if connection fails]

        :raises InternalServerError: [when Redis connection is unavailable]
        """
        try:
            self.redis.ping()
        except (ConnectionError, ConnectionRefusedError):
            raise InternalServerError(
                "Redis connection error. Please check if Redis is running."
            )
