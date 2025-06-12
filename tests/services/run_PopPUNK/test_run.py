import pickle
from types import SimpleNamespace
import pytest
from werkzeug.exceptions import BadRequest
from redis import Redis
from rq import Queue, SimpleWorker
from rq.job import Job

from beebop.config import RedisManager
from beebop.config.filestore import FileStore, PoppunkFileStore
from beebop.services.run_PopPUNK.run import PopPUNKJobRunner, run_PopPUNK_jobs
from tests import setup
from tests.test_utils import read_redis, wait_until

"""
Run tests with application context, via client fixture from conftest.py
"""


def test_sets_up_PopPUNKJobRunner(client):
    runner = PopPUNKJobRunner(setup.species)

    assert runner.storage_location is not None
    assert runner.dbs_location is not None
    assert runner.redis_host is not None
    assert runner.job_timeout == 1200  # seconds
    assert isinstance(runner.redis, Redis)
    assert isinstance(runner.redis_manager, RedisManager)
    assert hasattr(runner, "args")
    assert isinstance(runner.args, SimpleNamespace)


def test_sets_up_PopPUNKJobRunner_no_species(client):
    """
    Test that PopPUNKJobRunner raises BadRequest if species is not found
    in args.species.
    """
    with client.application.app_context():
        with pytest.raises(
            BadRequest,
            match="No database found for species: non_existent_species",
        ):
            PopPUNKJobRunner("non_existent_species")


def test_run_PopPUNK_jobs(client):
    fs_json = FileStore("./tests/files/json")
    sketches = {
        "e868c76fec83ee1f69a95bd27b8d5e76": fs_json.get(
            "e868c76fec83ee1f69a95bd27b8d5e76"
        ),
        "f3d9b387e311d5ab59a8c08eb3545dbb": fs_json.get(
            "f3d9b387e311d5ab59a8c08eb3545dbb"
        ),
    }.items()
    name_mapping = {"hash1": "name1.fa", "hash2": "name2.fa"}
    project_hash = "unit_test_run_poppunk_internal"
    redis = Redis()
    queue = Queue(connection=Redis())
    job_ids = run_PopPUNK_jobs(
        sketches,
        project_hash,
        name_mapping,
        setup.species,
        [],
    )
    # stores sketches in storage
    assert setup.fs.input.exists("e868c76fec83ee1f69a95bd27b8d5e76")
    assert setup.fs.input.exists("f3d9b387e311d5ab59a8c08eb3545dbb")
    # submits assign job to queue
    worker = SimpleWorker([queue], connection=queue.connection)
    worker.work(burst=True)  # Runs enqueued job
    job_assign = Job.fetch(job_ids["assign"], connection=redis)
    status_options = ["queued", "started", "finished", "scheduled", "deferred"]
    assert job_assign.get_status() in status_options
    # saves p-hash with job id in redis
    assert (
        read_redis("beebop:hash:job:assign", project_hash, redis)
        == job_ids["assign"]
    )
    # writes initial output file linking project hash with sample hashes

    with open(setup.fs.output_cluster(project_hash), "rb") as f:
        initial_output = pickle.load(f)
        assert initial_output[0]["hash"] == "e868c76fec83ee1f69a95bd27b8d5e76"
        assert initial_output[1]["hash"] == "f3d9b387e311d5ab59a8c08eb3545dbb"

    # wait for assign job to be finished
    def assign_status_finished():
        job = Job.fetch(job_ids["assign"], connection=redis)
        return job.get_status() == "finished"

    wait_until(assign_status_finished, timeout=20000)
    # submits visualisation jobs to queue
    job_visualise = Job.fetch(job_ids["visualise"], connection=redis)
    assert job_visualise.get_status() in status_options
    assert (
        read_redis("beebop:hash:job:visualise", project_hash, redis)
        == job_ids["visualise"]
    )
