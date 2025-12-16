import glob
import logging
import os
from collections import defaultdict
from pathlib import PurePath
from types import SimpleNamespace

import pandas as pd
from PopPUNK.utils import setupDBFuncs
from redis import Redis
from rq import get_current_job

from beebop.config import DatabaseFileStore, PoppunkFileStore
from beebop.services.cluster_service import get_cluster_num
from beebop.services.run_PopPUNK.poppunkWrapper import PoppunkWrapper

from .sublineage_utils import get_cluster_to_hashes, link_sketches_hdf5

logger = logging.getLogger(__name__)


def assign_sublineages(
    p_hash: str, fs: PoppunkFileStore, db_fs: DatabaseFileStore, args: SimpleNamespace, redis_host: str, species: str
) -> None:
    if db_fs.sublineages_db_path is None:
        raise ValueError("Sub-lineages database path is not provided.")

    cluster_to_hashes = get_cluster_to_hashes(redis_host)

    sublineage_results = defaultdict(dict)
    for cluster, hashes in cluster_to_hashes.items():
        assign_cluster_sublineages(p_hash, fs, db_fs, args, cluster, hashes, species)

    # after all assigned, combine all lineages into 1 and then also save result. can be added to metadata.csv + used to get results
    # TODO: later viz only needs file that is for that specific cluster.
    base_output_path = fs.output(p_hash)
    sublineage_dirs = glob.glob(os.path.join(base_output_path, "sublineage_*"))
    dfs = []
    for d in sublineage_dirs:
        folder_name = os.path.basename(d)
        csv_path = os.path.join(d, f"{folder_name}_lineages.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            dfs.append(df)
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["id"])

        combined_df = combined_df.rename(
            columns={"id": "ID"}
        )  # TODO: may be better to just set when reading. also just drop cols here that i dont need
        output_file = fs.output_all_sublineages_csv(p_hash)
        combined_df.to_csv(output_file, index=False)

        sublineage_results = fs.sublineage_results(p_hash)
        query_df = (
            combined_df[combined_df["Status"] == "Query"]
            .set_index("ID")
            .drop(columns=["Status", "Status:colour", "overall_Lineage"])
        )

        query_df.to_json(sublineage_results, orient="index")


def assign_cluster_sublineages(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: SimpleNamespace,
    cluster: str,
    hashes: list[str],
    species: str,
) -> None:
    """
    [Assign sub-lineages for each cluster based on provided sample hashes.]

    :param p_hash: [project hash]
    :param fs: [PoppunkFileStore instance]
    :param db_fs: [DatabaseFileStore instance]
    :param args: [SimpleNamespace containing arguments for PopPUNK functions]
    :param cluster_to_hashes: [dictionary mapping cluster identifiers to lists of sample hashes]
    :param species: [Type of species]
    """
    logger.info(f"Assigning sub-lineages for cluster {cluster} with {len(hashes)} samples.")

    model_folder = db_fs.get_sublineages_model_path(cluster)
    # TODO: handle better so user can see details why it cant assign sublineages
    if not os.path.exists(model_folder):
        logger.warning(f"Model folder for cluster {cluster} not found, skipping sub-lineage assignment.")
        return

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
