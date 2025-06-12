import glob
import os
import pickle
import zipfile
from io import BytesIO
from pathlib import PurePath
from typing import Optional

import pandas as pd

from beebop.models import SpeciesConfig
from beebop.config import DatabaseFileStore, PoppunkFileStore


def get_cluster_assignments(
    p_hash: str, fs: PoppunkFileStore
) -> dict[int, dict[str, str]]:
    """
    [returns cluster assignment results.
    Return of type:
    {idx: {hash: hash, cluster: cluster, raw_cluster_num: raw_cluster_num}}]

    :param p_hash: [project hash]
    :param fs: [PoppunkFileStore instance]
    :return dict: [cluster results]
    """
    with open(fs.output_cluster(p_hash), "rb") as f:
        cluster_result = pickle.load(f)
        return cluster_result


def get_failed_samples_internal(
    p_hash: str, fs: PoppunkFileStore
) -> dict[str, dict]:
    """
    [Returns a dictionary of failed samples for a given project hash]

    :param p_hash (str): The hash of the samples to retrieve.
    :param fs (PoppunkFileStore): The PoppunkFileStore instance.

    :return dict[str, dict]: failed samples
    containing hash and reasons for failure.
    """
    qc_report_file_path = fs.output_qc_report(p_hash)
    failed_samples = {}
    if os.path.exists(qc_report_file_path):
        with open(fs.output_qc_report(p_hash), "r") as f:
            for line in f:
                hash, reasons = line.strip().split("\t")
                failed_samples[hash] = {
                    "failReasons": reasons.split(","),
                    "hash": hash,
                }
    return failed_samples


def get_network_files_for_zip(
    visualisations_folder: str, cluster_num: str
) -> list[str]:
    """
    [Get the network files for a given cluster number,
    that will be used for network zip generation.
    These are the graphml files and the csv file for cytoscape.]

    :param visualisations_folder: [path to visualisations folder]
    :param cluster_num: [cluster number]
    :return list[str]: [list of network files to be included in zip]
    """
    network_file_name = os.path.basename(
        get_component_filepath(visualisations_folder, cluster_num)
    )

    return [
        network_file_name,
        f"pruned_{network_file_name}",
        f"visualise_{cluster_num}_cytoscape.csv",
    ]


def get_component_filepath(
    visualisations_folder: str, cluster_num: str
) -> str:
    """
    Get the filename of the network component
    for a given assigned cluster number.

    :param visualisations_folder: Path to the
        folder containing visualisation files.
    :param cluster_num: Cluster number to find the component file for.
    :return: Path to the network component file.
    :raises FileNotFoundError: If no component files are
        found for the given cluster number.
    """
    component_files = glob.glob(
        str(
            PurePath(
                visualisations_folder,
                f"visualise_{cluster_num}_component_*.graphml",
            )
        )
    )
    if not component_files:
        raise FileNotFoundError(
            f"No component files found for cluster {cluster_num}"
        )
    return component_files[0]


def add_files(
    memory_file: BytesIO,
    path_folder: str,
    file_list: list[str],
    exclude: bool,
) -> BytesIO:
    """
    [Add files in specified folder to a memory_file.
    If exclude is True, only files not in file_list are added.
    If exclude is False, only files in file_list are added.]

    :param memory_file: [empty memory file to add files to]
    :param path_folder: [path to folder with files to include]
    :param file_list: [list of files to include/exclude]
    :param: exclude: [whether to exclude the file list or not]
    :return BytesIO: [memory file with added files]
    """
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(path_folder):
            for file in files:
                if (not exclude and file in file_list) or (
                    exclude and file not in file_list
                ):
                    zipf.write(os.path.join(root, file), arcname=file)
    return memory_file


def setup_db_file_stores(
    species_args: SpeciesConfig, dbs_location: str
) -> tuple[DatabaseFileStore, DatabaseFileStore]:
    """
    [Initializes the reference and full database file stores
    with the given species arguments. If the full database
    does not exist, fallback to reference database.]

    :param species_args: [species arguments]
    :param dbs_location: [location of databases]
    :return tuple[DatabaseFileStore, DatabaseFileStore]: [reference and full
        database file stores]
    """
    ref_db_fs = DatabaseFileStore(
        f"{dbs_location}/{species_args.refdb}",
        species_args.external_clusters_file,
        species_args.db_metadata_file,
    )

    if os.path.exists(f"{dbs_location}/{species_args.fulldb}"):
        full_db_fs = DatabaseFileStore(
            f"{dbs_location}/{species_args.fulldb}",
            species_args.external_clusters_file,
            species_args.db_metadata_file,
        )
    else:
        full_db_fs = ref_db_fs

    return ref_db_fs, full_db_fs


def add_amr_to_metadata(
    fs: PoppunkFileStore,
    p_hash: str,
    amr_metadata: list[dict],
    metadata_file: Optional[str] = None,
) -> None:
    """
    [Create new metadata file with AMR metadata
    and existing metadata csv file]

    :param fs: [PoppunkFileStore with paths to in-/outputs]
    :param p_hash: [project hash]
    :param amr_metadata: [AMR metadata]
    :param metadata_file: [db metadata csv file]
    """
    if metadata_file is None:
        metadata = None
    else:
        metadata = pd.read_csv(metadata_file)
    amr_df = pd.DataFrame(amr_metadata)

    pd.concat([metadata, amr_df], ignore_index=True).to_csv(
        fs.tmp_output_metadata(p_hash), index=False
    )
