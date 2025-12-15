import os
import pickle
from types import SimpleNamespace
from typing import Optional

from redis import Redis
from rq import Queue, get_current_job
from rq.job import Dependency

from beebop.config import DatabaseFileStore, PoppunkFileStore
from beebop.db import RedisManager
from beebop.services.cluster_service import get_cluster_num
from beebop.services.file_service import create_viz_metadata
from beebop.services.run_PopPUNK.poppunkWrapper import PoppunkWrapper

from .visualise_utils import (
    create_subgraph,
    get_internal_cluster,
    replace_filehashes,
)


def visualise(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: SimpleNamespace,
    name_mapping: dict,
    species: str,
    redis_host: str,
    queue_kwargs: dict,
    amr_metadata: list[dict],
) -> None:
    """
    [generate files to use on microreact.org
    and graphml files for network visualisations.
    Output files are .csv, .dot and .nwk
    (last one only for clusters with >3 isolates).
    Also, a .microreact file is provided which can alternatively be uploaded.]

    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_fs: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param species: [Type of species]
    :param redis_host: [host of redis server]
    :param queue_kwargs: [kwargs for the queue]
    """
    redis = Redis(host=redis_host)
    # get results from previous job
    current_job = get_current_job(connection=redis)
    if not current_job or not current_job.dependency:
        raise ValueError("Current job or its dependencies are not set.")
    # gets first dependency result (i.e assign_clusters)
    assign_result = current_job.dependency.result
    external_to_poppunk_clusters: Optional[dict[str, set[str]]] = None

    # TODO: probs revert to do just metadata before and then add sublineage csv later
    create_viz_metadata(fs, p_hash, amr_metadata, db_fs.metadata)

    try:
        with open(fs.external_to_poppunk_clusters(p_hash), "rb") as pkl_file:
            external_to_poppunk_clusters = pickle.load(pkl_file)
    except FileNotFoundError:
        print("no external cluster info found")

    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)
    queue_visualisation_jobs(
        assign_result,
        p_hash,
        fs,
        wrapper,
        name_mapping,
        external_to_poppunk_clusters,
        redis,
        queue_kwargs,
    )


def queue_visualisation_jobs(
    assign_result: dict,
    p_hash: str,
    fs: PoppunkFileStore,
    wrapper: PoppunkWrapper,
    name_mapping: dict,
    external_to_poppunk_clusters: Optional[dict[str, set[str]]],
    redis: Redis,
    queue_kwargs: dict,
) -> None:
    """
    Enqueues visualisation jobs for each
    unique cluster in the assignment results.
    Runs sequentially, with each job depending on the previous one.

    :param assign_result: Dictionary containing the assignment results,
        where each value is expected to have a "cluster" key.
    :param p_hash: Unique hash identifier for the current process.
    :param fs: Instance of PoppunkFileStore for file storage operations.
    :param wrapper: Instance of PoppunkWrapper for wrapping Poppunk operations.
    :param name_mapping: Dictionary mapping names to
        their respective identifiers.
    :param external_to_poppunk_clusters: Dictionary mapping
        external clusters to Poppunk clusters.
    :param redis: Redis connection instance.
    :param queue_kwargs: Additional keyword arguments to pass
        to the queue when enqueuing jobs.
    """
    q = Queue(connection=redis)
    redis_manager = RedisManager(redis)
    queries_clusters = {item["cluster"] for item in assign_result.values()}
    previous_job = None
    last_cluster_idx = len(queries_clusters) - 1
    for idx, assign_cluster in enumerate(queries_clusters):
        dependency = Dependency([previous_job], allow_failure=True) if previous_job else None
        cluster_visualise_job = q.enqueue(
            visualise_per_cluster,
            args=(
                assign_cluster,
                p_hash,
                fs,
                wrapper,
                name_mapping,
                external_to_poppunk_clusters,
                (idx == last_cluster_idx),
            ),
            depends_on=dependency,
            **queue_kwargs,
        )

        redis_manager.set_visualisation_status(p_hash, assign_cluster, cluster_visualise_job.id)
        previous_job = cluster_visualise_job


def visualise_per_cluster(
    assign_cluster: str,
    p_hash: str,
    fs: PoppunkFileStore,
    wrapper: PoppunkWrapper,
    name_mapping: dict,
    external_to_poppunk_clusters: Optional[dict[str, set[str]]],
    is_last_cluster_to_process: bool = False,
) -> None:
    """
    [This function is called by the queue
    to generate the visualisation files for a single cluster.]


    :param assign_cluster: [cluster number to generate visualisation files for]
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param wrapper: [PoppunkWrapper with paths to input data]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param external_to_poppunk_clusters: [dict of external to poppunk
        clusters, used to identify the include file
        to pass to poppunk]
    :param is_last_cluster_to_process: [Boolean flag to indicate if
    this is the last cluster to process]
    """

    cluster_no = get_cluster_num(assign_cluster)
    output_folder = fs.output_visualisations(p_hash, cluster_no)
    internal_cluster = get_internal_cluster(
        external_to_poppunk_clusters,
        assign_cluster,
        p_hash,
        fs,
    )
    wrapper.create_visualisations(cluster_no, fs.include_file(p_hash, internal_cluster))

    replace_filehashes(output_folder, name_mapping)
    create_subgraph(output_folder, name_mapping, cluster_no)
    if is_last_cluster_to_process:
        os.remove(fs.tmp_output_metadata(p_hash))
