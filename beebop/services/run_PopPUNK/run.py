import pickle
from collections.abc import ItemsView
from types import SimpleNamespace
from typing import Optional

from flask import current_app
from redis import Redis
from rq import Queue
from rq.job import Job
from werkzeug.exceptions import BadRequest

from beebop.config import PoppunkFileStore
from beebop.db import RedisManager
from beebop.models import SpeciesConfig
from beebop.services.file_service import (
    add_amr_to_metadata,
    setup_db_file_stores,
)

from .assign import assign_clusters, assign_sub_lineages
from .visualise import visualise


class PopPUNKJobRunner:
    """Service class for running PopPUNK jobs"""

    def __init__(self, species: str):
        self.species = species
        self._setup_context()

    def _setup_context(self) -> None:
        """Initialize all required services and configurations"""
        config = current_app.config

        self.redis_host: str = config["redis_host"]
        self.redis: Redis = config["redis"]
        self.redis_manager = RedisManager(self.redis)
        self.args: SimpleNamespace = config["args"]
        self.job_timeout: int = config["job_timeout"]
        self.dbs_location: str = config["dbs_location"]
        self.storage_location: str = config["storage_location"]

        # Validate species configuration
        self.species_args: Optional[SpeciesConfig] = getattr(self.args.species, self.species, None)
        if not self.species_args:
            raise BadRequest(f"No database found for species: {self.species}")

        # Setup file stores and services
        self.sub_lineages_db = self.species_args.sub_lineages_db
        self.ref_db_fs, self.full_db_fs = setup_db_file_stores(self.species_args, self.dbs_location)
        self.queue = Queue(connection=self.redis)
        self.fs = PoppunkFileStore(self.storage_location)

    def run_jobs(
        self,
        sketches: ItemsView,
        p_hash: str,
        name_mapping: dict,
        amr_metadata: list[dict],
    ) -> dict:
        """
        Run all PopPUNK jobs (assign and visualise).
        This method handles the submission of cluster assignment and
        visualization jobs to the Redis queue, prepares the necessary data,
        and manages the output directory.

        :param sketches: All sketches in json format
        :param p_hash: Project hash
        :param name_mapping: Maps filehashes to filenames for all query samples
        :param amr_metadata: AMR metadata for query samples
        :return: Dictionary with job IDs
        """
        # Prepare data and setup output directory
        hashes_list = self._store_sketches_and_setup_output(sketches, p_hash)

        # Setup job configuration
        queue_kwargs = self._get_queue_kwargs()

        # Submit cluster assignment job
        job_assign = self._submit_assign_job(hashes_list, p_hash, queue_kwargs)

        # Submit cluster lineage assignment jobs - only if species supports it
        job_sub_lineage_assign: Optional[Job] = None
        if getattr(self.species_args, "sub_lineages_db", None) is not None:
            job_sub_lineage_assign = self._submit_sub_lineage_assign_jobs(p_hash, job_assign, queue_kwargs)

        # Submit visualization job - only for valid species
        job_visualise = self._submit_visualization_job(p_hash, name_mapping, amr_metadata, job_assign, queue_kwargs)

        return {
            "assign": job_assign.id,
            "visualise": job_visualise.id,
            "sub_lineage_assign": job_sub_lineage_assign.id if job_sub_lineage_assign else None,
        }

    def _store_sketches_and_setup_output(self, sketches: ItemsView, p_hash: str) -> list[str]:
        """Store sketches and setup initial output directory"""
        hashes_list: list[str] = []
        initial_output: dict[int, dict[str, str]] = {}

        for i, (key, value) in enumerate(sketches):
            hashes_list.append(key)
            self.fs.input.put(key, value)
            initial_output[i] = {"hash": key}

        # Setup output directory and save hashes
        self.fs.setup_output_directory(p_hash)
        with open(self.fs.output_cluster(p_hash), "wb") as f:
            pickle.dump(initial_output, f)

        return hashes_list

    def _get_queue_kwargs(self) -> dict:
        """Get standard queue configuration"""
        return {
            "job_timeout": self.job_timeout,
            "result_ttl": -1,
            "failure_ttl": -1,
        }

    def _submit_assign_job(self, hashes_list: list[str], p_hash: str, queue_kwargs: dict):
        """Submit cluster assignment job to Redis queue"""
        job_assign = self.queue.enqueue(
            assign_clusters,
            hashes_list,
            p_hash,
            self.fs,
            self.ref_db_fs,
            self.full_db_fs,
            self.args,
            self.species,
            **queue_kwargs,
        )

        self.redis_manager.set_job_status("assign", p_hash, job_assign.id)
        return job_assign

    def _submit_sub_lineage_assign_jobs(self, p_hash: str, job_assign: Job, queue_kwargs: dict):
        """Submit lineage cluster assignment jobs to Redis queue"""
        job_lineage_assign = self.queue.enqueue(
            assign_sub_lineages,
            args=(p_hash, self.fs, self.full_db_fs, self.args, self.redis_host, self.species),
            depends_on=job_assign,
            **queue_kwargs,
        )

        self.redis_manager.set_job_status("sub_lineage_assign", p_hash, job_lineage_assign.id)
        return job_lineage_assign

    def _submit_visualization_job(
        self,
        p_hash: str,
        name_mapping: dict,
        amr_metadata: list[dict],
        job_assign: Job,
        queue_kwargs: dict,
    ):
        """Submit visualization job to Redis queue"""
        # Prepare metadata
        add_amr_to_metadata(self.fs, p_hash, amr_metadata, self.ref_db_fs.metadata)

        # Clean up previous visualize cluster job results
        self.redis_manager.delete_visualisation_statuses(p_hash)

        job_visualise = self.queue.enqueue(
            visualise,
            args=(
                p_hash,
                self.fs,
                self.full_db_fs,
                self.args,
                name_mapping,
                self.species,
                self.redis_host,
                queue_kwargs,
            ),
            depends_on=job_assign,
            **queue_kwargs,
        )
        self.redis_manager.set_job_status("visualise", p_hash, job_visualise.id)
        return job_visualise


def run_PopPUNK_jobs(
    sketches: ItemsView,
    p_hash: str,
    name_mapping: dict,
    species: str,
    amr_metadata: list[dict],
) -> dict:
    """
    Convenience function to run assign and
    visualise PopPUNK jobs.

    :param sketches: All sketches in json format
    :param p_hash: Project hash
    :param name_mapping: Maps filehashes to filenames for all query samples
    :param species: Type of species to be analyzed
    :param amr_metadata: AMR metadata for query samples
    :return: Dictionary with job IDs
    """
    runner = PopPUNKJobRunner(species)
    return runner.run_jobs(sketches, p_hash, name_mapping, amr_metadata)
