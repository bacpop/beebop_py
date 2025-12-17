import logging
import os
from types import SimpleNamespace

import pandas as pd
from PopPUNK.utils import setupDBFuncs

from beebop.config import DatabaseFileStore, PoppunkFileStore
from beebop.services.cluster_service import get_cluster_num
from beebop.services.run_PopPUNK.poppunkWrapper import PoppunkWrapper

from .sublineage_utils import (
    get_cluster_to_hashes,
    get_query_sublineage_result,
    link_sketches_hdf5,
    save_sublineage_results,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def assign_sublineages(
    p_hash: str, fs: PoppunkFileStore, db_fs: DatabaseFileStore, args: SimpleNamespace, redis_host: str, species: str
) -> None:
    """
    [Assign sub-lineages for all clusters based on cluster assignment results.]

    :param p_hash: [project hash]
    :param fs: [PoppunkFileStore instance]
    :param db_fs: [DatabaseFileStore instance]
    :param args: [SimpleNamespace containing arguments for PopPUNK functions]
    :param redis_host: [host of redis server]
    :param species: [Type of species]
    """
    if db_fs.sublineages_db_path is None:
        raise ValueError("Sub-lineages database path is not provided.")

    cluster_to_hashes = get_cluster_to_hashes(redis_host)

    sublineage_results_list = []
    for cluster, hashes in cluster_to_hashes.items():
        sublineage_query_df = assign_cluster_sublineages(p_hash, fs, db_fs, args, cluster, hashes, species)
        sublineage_results_list.append(sublineage_query_df)

    sublineage_results = (
        pd.concat(sublineage_results_list, ignore_index=True) if sublineage_results_list else pd.DataFrame()
    )
    save_sublineage_results(p_hash, fs, sublineage_results)


def assign_cluster_sublineages(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: SimpleNamespace,
    cluster: str,
    hashes: list[str],
    species: str,
) -> pd.DataFrame:
    """
    [Assign sub-lineages for each cluster based on provided sample hashes.]

    :param p_hash: [project hash]
    :param fs: [PoppunkFileStore instance]
    :param db_fs: [DatabaseFileStore instance]
    :param args: [SimpleNamespace containing arguments for PopPUNK functions]
    :param cluster: [cluster identifier with prefix. eg. GPSC1]
    :param hashes: [list of sample hashes belonging to the cluster]
    :param species: [Type of species]
    :return pd.DataFrame: [DataFrame containing sub-lineage assignment results for query samples]
    """
    logger.info(f"Assigning sub-lineages for cluster {cluster} with {len(hashes)} samples.")

    model_folder = db_fs.get_sublineages_model_path(cluster)
    # TODO: handle better so user can see details why it cant assign sublineages
    if not os.path.exists(model_folder):
        logger.warning(f"Model folder for cluster {cluster} not found, skipping sub-lineage assignment.")
        return pd.DataFrame()

    distances = db_fs.get_sublineages_distances_path(cluster)
    cluster_num = get_cluster_num(cluster)
    output_sublineage_folder = fs.output_sublineages_folder(p_hash, cluster_num)

    link_sketches_hdf5(fs, p_hash, cluster_num)

    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)
    wrapper.assign_sublineages(
        setupDBFuncs(args=args.assign),
        qNames=hashes,
        output=output_sublineage_folder,
        model_folder=model_folder,
        distances=distances,
    )

    return get_query_sublineage_result(fs, p_hash, cluster_num)
