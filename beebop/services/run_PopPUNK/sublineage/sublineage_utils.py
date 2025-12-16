import os
from collections import defaultdict
from pathlib import PurePath

from redis import Redis
from rq import get_current_job

from beebop.config import PoppunkFileStore


def get_cluster_to_hashes(redis_host: str) -> dict[str, list[str]]:
    """
    [Retrieve a mapping of clusters to sample hashes from the assignment job results.]

    :param redis_host: [host of redis server]
    :return: [dictionary mapping cluster identifiers to lists of sample hashes]
    :raises ValueError: If current job or its dependencies are not set.
    """
    redis = Redis(host=redis_host)
    current_job = get_current_job(connection=redis)

    if not current_job or not current_job.dependency:
        raise ValueError("Current job or its dependencies are not set.")

    assign_result: dict = current_job.dependency.result
    cluster_to_hashes = defaultdict(list)

    for item in assign_result.values():
        cluster_to_hashes[item["cluster"]].append(item["hash"])

    return cluster_to_hashes


def link_sketches_hdf5(
    fs: PoppunkFileStore,
    p_hash: str,
    cluster_num: str,
) -> None:
    """
    [Create a hard link to the query sketches HDF5 file for a specific cluster's sublineage assignment.]

    :param fs: [PoppunkFileStore instance]
    :param p_hash: [project hash]
    :param cluster_num: [cluster number as string]
    """
    output_hdf5_link_path = fs.output_sublineages_hdf5(p_hash, cluster_num)
    queries_hdf5_path = fs.query_sketches_hdf5(p_hash)
    if not os.path.exists(output_hdf5_link_path):
        os.link(queries_hdf5_path, output_hdf5_link_path)
