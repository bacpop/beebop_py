import os
import re

import pandas as pd
from PopPUNK.web import sketch_to_hdf5

from beebop.models import ClusteringConfig, PoppunkFileStore
from beebop.services.cluster_service import get_lowest_cluster


def update_external_clusters_csv(
    dest_query_clustering_file: str,
    source_query_clustering_file: str,
    q_names: list[str],
) -> None:
    """
    [Update the external clusters CSV file with the clusters of the samples
    that were not found in the external clusters file.]

    :param dest_query_clustering_file: [Path to CSV file
    containing sample data to copy into]
    :param source_query_clustering_file: [Path to CSV file
    containing sample data to copy from]
    :param q_names: [List of sample names to match]
    """
    df, samples_mask = get_df_sample_mask(dest_query_clustering_file, q_names)
    sample_cluster_num_mapping = get_external_cluster_nums(
        source_query_clustering_file, q_names
    )

    df.loc[samples_mask, "Cluster"] = [
        sample_cluster_num_mapping[sample_id] for sample_id in q_names
    ]
    df.to_csv(dest_query_clustering_file, index=False)


def get_df_sample_mask(
    previous_query_clustering_file: str, samples: list[str]
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Read a CSV file and create a boolean mask for matching sample names.

    :param previous_query_clustering_file: [Path to CSV file
        containing sample data
        samples: List of sample names to match]
    :param samples: [List of sample names to match]
    :return tuple: [DataFrame containing sample data,
        boolean mask for matching samples]
    """
    df = pd.read_csv(previous_query_clustering_file, dtype=str)
    return df, df["sample"].isin(samples)


def get_external_cluster_nums(
    previous_query_clustering_file: str, hashes_list: list
) -> dict[str, str]:
    """
    [Get external cluster numbers for samples in the external clusters file.]

    :param previous_query_clustering_file: [Path to CSV file
    containing sample data]
    :param hashes_list: [List of sample hashes to find samples for]
    :return dict: [Dictionary mapping sample names to external cluster names]
    """
    filtered_df = get_df_filtered_by_samples(
        previous_query_clustering_file, hashes_list
    )

    sample_cluster_num_mapping = filtered_df["Cluster"].astype(str)
    sample_cluster_num_mapping.index = filtered_df["sample"]

    return sample_cluster_num_mapping.to_dict()


def get_df_filtered_by_samples(
    previous_query_clustering_file: str, hashes_list: list
) -> pd.DataFrame:
    """
    [Filter a DataFrame by sample names.]

    :param previous_query_clustering_file: [Path to CSV file
    containing sample data]
    :param hashes_list: [List of sample hashes to find samples for]
    :return pd.DataFrame: [DataFrame containing sample data]
    """
    df, samples_mask = get_df_sample_mask(
        previous_query_clustering_file, hashes_list
    )
    return df[samples_mask]


def get_external_clusters_from_file(
    previous_query_clustering_file: str,
    hashes_list: list,
    external_clusters_prefix: str,
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """
    [Finds sample hashes defined by hashes_list in
    the given external clusters
    file and returns a dictionary of sample hash to
    external cluster name & raw external cluster number. If
    there is a merged clusters for a sample, the lowest
    cluster number is used for cluster. If any samples are found
    but do  not
    have a cluster assigned, they are returned separately.]

    :param previous_query_clustering_file: [filename
    of the project's external clusters file]
    :param hashes_list: [list of sample hashes to find samples for]
    :param external_clusters_prefix: prefix for external cluster name
    :return tuple: [dictionary of sample hash to
    external cluster name & raw external cluster number,
    list of sample hashes not found in the external]
    """
    filtered_df = get_df_filtered_by_samples(
        previous_query_clustering_file, hashes_list
    )

    # Split into found and not found based on NaN values
    found_mask = filtered_df["Cluster"].notna()
    not_found_hashes = filtered_df[~found_mask]["sample"].tolist()

    # Process only rows with valid clusters
    valid_clusters = filtered_df[found_mask]
    hash_cluster_info = {
        sample: {
            "cluster": f"{external_clusters_prefix}{get_lowest_cluster(cluster)}",
            "raw_cluster_num": cluster,
        }
        for sample, cluster in zip(
            valid_clusters["sample"], valid_clusters["Cluster"]
        )
    }

    return hash_cluster_info, not_found_hashes


def create_sketches_dict(hashes_list: list[str], fs: PoppunkFileStore) -> dict:
    """
    [Create a dictionary of sketches for all query samples]

    :param hashes_list: [list of file hashes to get sketches for]
    :param fs: [PoppunkFileStore with paths to input files]
    :return dict: [dictionary with filehash (key) and sketch (value)]
    """
    return {hash: fs.input.get(hash) for hash in hashes_list}


def preprocess_sketches(sketches_dict: dict, outdir: str) -> list:
    """
    [Convert hexadecimal sketches to decimal and save them to hdf5 file]

    :param sketches_dict: [dictionary with filehash (key) and sketch (value)]
    :param outdir: [path to output directory]
    :return list: [list of sample hashes]
    """
    hex_to_decimal(sketches_dict)
    return sketch_to_hdf5(sketches_dict, outdir)


def hex_to_decimal(sketches_dict) -> None:
    """
    [Converts all hexadecimal numbers in the sketches into decimal numbers.
    These have been stored in hexadecimal format to not loose precision when
    sending the sketches from the backend to the frontend]

    :param sketches_dict: [dictionary holding all sketches]
    """
    for sample in list(sketches_dict.values()):
        for key, value in sample.items():
            if (
                isinstance(value, list)
                and isinstance(value[0], str)
                and re.match("0x.*", value[0])
            ):
                sample[key] = list(map(lambda x: int(x, 16), value))


def filter_queries(
    queries_names: list[str],
    queries_clusters: list[str],
    not_found: list[str],
) -> tuple[list[str], list[str], set[str]]:
    """
    [Filter out queries that were not found in the
        initial external clusters file.]

    :param queries_names: [list of sample hashes]
    :param queries_clusters: [list of sample PopPUNK clusters]
    :param not_found: [list of sample hashes
        that were not found]
    :return tuple[list[str], list[str], set[str]]: [filtered sample hashes,
        filtered sample PopPUNK clusters,
            set of clusters assigned to not found samples]
    """
    filtered_names = [name for name in queries_names if name not in not_found]
    filtered_clusters = [
        cluster
        for name, cluster in zip(queries_names, queries_clusters)
        if name not in not_found
    ]

    return (
        filtered_names,
        filtered_clusters,
        set(queries_clusters) - set(filtered_clusters),
    )


def handle_files_manipulation(
    config: ClusteringConfig,
    output_full_tmp: str,
    not_found_query_clusters: set[str],
) -> None:
    """
    [Handles file manipulations for queries that were not found in the
    initial external clusters file.
    This function copies include files from the
    full assign output directory
    to the output directory, deletes include files for queries
    that were not found,
    and merges the partial query graph files.]

    :param config: [ClusteringConfig with all necessary information]
    :param output_full_tmp: [path to temporary output directory]
    :param not_found_query_clusters: [set of clusters assigned
        to initial not found samples]
    """
    delete_include_files(
        config.fs,
        config.p_hash,
        not_found_query_clusters,
    )
    copy_include_files(output_full_tmp, config.out_dir)
    merge_txt_files(
        config.fs.partial_query_graph(config.p_hash),
        config.fs.partial_query_graph_tmp(config.p_hash),
    )


def delete_include_files(
    fs: PoppunkFileStore, p_hash: str, clusters: set
) -> None:
    """
    [Delete include files for samples that were not found
        in the initial external clusters file.]

    :param fs: [PoppunkFileStore with paths to input files]
    :param p_hash: [project hash]
    :param clusters: [set of cluster numbers to delete include
    """
    for cluster in clusters:
        include_file = fs.include_file(p_hash, cluster)
        if os.path.exists(include_file):
            os.remove(include_file)


def copy_include_files(output_full_tmp: str, outdir: str) -> None:
    """
    [Copy include files from the full assign output directory
        to the output directory
    where all files from the PopPUNK assign job are stored.
    If the include file already exists in the output directory,
    the contents of the full assign output include file are merged]

    :param output_full_tmp: [path to full assign output directory]
    :param outdir: [path to output directory]
    """
    include_files = [
        f for f in os.listdir(output_full_tmp) if f.startswith("include")
    ]
    for include_file in include_files:
        dest_file = f"{outdir}/{include_file}"
        source_file = f"{output_full_tmp}/{include_file}"
        if os.path.exists(dest_file):
            merge_txt_files(dest_file, source_file)
            os.remove(source_file)
        else:
            os.rename(source_file, dest_file)


def merge_txt_files(main_file: str, merge_file: str) -> None:
    """
    [Merge the contents of the merge file into the main file]

    :param main_file: [path to main file]
    :param merge_file: [path to merge file]
    """

    with open(merge_file, "r") as f:
        merge_lines = set(f.read().splitlines())
    with open(main_file, "r") as f:
        main_lines = set(f.read().splitlines())

    combined_lines = list(main_lines.union(merge_lines))
    with open(main_file, "w") as f:
        f.write("\n".join(combined_lines))


def process_unassignable_samples(
    unassignable_names: list[str], fs: PoppunkFileStore, p_hash: str
) -> None:
    """
    [Process samples that are unassignable to external clusters.
    These samples are added to the QC error report file.]

    :param unassignable_names: [List of sample hashes that
    are unassignable to external clusters.]
    :param fs: [PoppunkFileStore with paths to input/output files.]
    :param p_hash: [Project hash.]
    """
    if not unassignable_names:
        return

    qc_report_path = fs.output_qc_report(p_hash)
    strain_assignment_error = (
        "Unable to assign to an existing strain - potentially novel genotype"
    )

    with open(qc_report_path, "a") as report_file:
        for sample_hash in unassignable_names:
            report_file.write(f"{sample_hash}\t{strain_assignment_error}\n")
