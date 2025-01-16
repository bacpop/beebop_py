from rq import get_current_job, Queue
from rq.job import Dependency
from redis import Redis
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.utils import (
    replace_filehashes,
    create_subgraphs,
)
from beebop.utils import get_cluster_num
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
import pickle
import os

def microreact(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: dict,
    name_mapping: dict,
    species: str,
    redis_host: str,
    queue_kwargs: dict,
) -> None:
    """
    [generate files to use on microreact.org
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
    current_job = get_current_job(redis)
    # gets first dependency result (i.e assign_clusters)
    assign_result = current_job.dependency.result
    external_to_poppunk_clusters = None

    try:
        with open(fs.external_to_poppunk_clusters(p_hash), "rb") as dict:
            external_to_poppunk_clusters = pickle.load(dict)
    except FileNotFoundError:
        print("no external cluster info found")

    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)
    queue_microreact_jobs(
        assign_result,
        p_hash,
        fs,
        wrapper,
        name_mapping,
        external_to_poppunk_clusters,
        redis,
        queue_kwargs,
    )


def queue_microreact_jobs(
    assign_result: dict,
    p_hash: str,
    fs: PoppunkFileStore,
    wrapper: PoppunkWrapper,
    name_mapping: dict,
    external_to_poppunk_clusters: dict,
    redis: Redis,
    queue_kwargs: dict,
) -> None:
    """
    Enqueues microreact jobs for each unique cluster in the assignment results.
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
    queries_clusters = [item["cluster"] for item in assign_result.values()]
    previous_job = None
    last_cluster_idx = len(queries_clusters) - 1
    for idx, assign_cluster in enumerate(set(queries_clusters)):
        dependency = (
            Dependency(previous_job, allow_failure=True)
            if previous_job
            else None
        )
        cluster_microreact_job = q.enqueue(
            microreact_per_cluster,
            args=(
                assign_cluster,
                p_hash,
                fs,
                wrapper,
                name_mapping,
                external_to_poppunk_clusters,
                (idx == last_cluster_idx)
            ),
            depends_on=dependency,
            **queue_kwargs,
        )

        redis.hset(
            f"beebop:hash:job:microreact:{p_hash}",
            assign_cluster,
            cluster_microreact_job.id,
        )
        previous_job = cluster_microreact_job


def microreact_per_cluster(
    assign_cluster: str,
    p_hash: str,
    fs: PoppunkFileStore,
    wrapper: PoppunkWrapper,
    name_mapping: dict,
    external_to_poppunk_clusters: dict = None,
    is_last_cluster_to_process: bool = False,
) -> None:
    """
    This function is called by the queue
        to generate the microreact files for a single cluster.

    :param assign_cluster: [cluster number to generate microreact files for]
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param wrapper: [PoppunkWrapper with paths to input data]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param external_to_poppunk_clusters: [dict of external to poppunk
        clusters, used to identify the include file to pass to poppunk]
    :param is_last_cluster_to_process: [Boolean flag to indicate if this is the last
        cluster to process]
    """

    cluster_no = get_cluster_num(assign_cluster)
    if external_to_poppunk_clusters:
        internal_cluster = external_to_poppunk_clusters[assign_cluster]
    else:
        internal_cluster = assign_cluster

    wrapper.create_microreact(cluster_no, internal_cluster)
    
    replace_filehashes(fs.output_microreact(p_hash, cluster_no), name_mapping)
    if is_last_cluster_to_process:
        os.remove(fs.tmp_output_metadata(p_hash))


def network(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: dict,
    name_mapping: dict,
    species: str,
) -> None:
    """
    [Generate files to draw a network.
    Output files are .csv and .graphml (one overall and several component
    files).
    Since network component number and poppunk cluster number do not
    match, we need to generate a mapping to find the right component files.
    To highlight query samples in the network graph, a ref or query status is
    added to .graphml files.]

    :param p_hash: [project hash to find input data (output from
        assign_clusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_fs: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param species: [Type of species]
    """
    # get results from previous job
    current_job = get_current_job(Redis())
    assign_result = current_job.dependency.result
    network_internal(p_hash, fs, db_fs, args, name_mapping, species)
    return assign_result


def network_internal(
    p_hash,
    fs,
    db_fs: DatabaseFileStore,
    args,
    name_mapping,
    species: str,
) -> None:
    """
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_fs: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param species: [Type of species]
    """
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)
    wrapper.create_network()

    network_folder = fs.output_network(p_hash)
    replace_filehashes(network_folder, name_mapping)
    create_subgraphs(network_folder, name_mapping)
