from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
from beebop.utils import (
    get_external_clusters_from_file,
    update_external_clusters_csv,
)
import re
import os
import pickle
from typing import Union
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
import shutil
from typing import Optional, Any
from dataclasses import dataclass


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


@dataclass
class ClusteringConfig:
    species: str
    p_hash: str
    args: dict
    external_clusters_prefix: Optional[str]
    fs: PoppunkFileStore
    full_db_fs: DatabaseFileStore
    ref_db_fs: DatabaseFileStore
    db_funcs: dict[str, Any]
    out_dir: str


def get_clusters(
    hashes_list: list,
    p_hash: str,
    fs: PoppunkFileStore,
    ref_db_fs: DatabaseFileStore,
    full_db_fs: DatabaseFileStore,
    args: dict,
    species: str,
) -> dict:
    """
    Assign cluster numbers to samples using PopPUNK.
    This can be either the internal PopPUNK clustering or external
    clustering, depending on the presence of external_clusters.csv for species.

    :param hashes_list: [list of file hashes from all query samples]
    :param p_hash: [project_hash]
    :param fs: [PoppunkFileStore with paths to input files]
    :param ref_db_fs: [DatabaseFileStore which provides paths
        to database files for the reference database]
    :param full_db_fs: [DatabaseFileStore which provides paths
        to database files for the full database]
    :param args: [arguments for Poppunk's assign function, stored in
        resources/args.json]
    :param species: [Type of species]
    :return dict: [dict with filehash (key) and cluster number (value)]
    """
    out_dir = setup_output_directory(fs, p_hash)
    db_funcs = setupDBFuncs(args=args.assign)
    config = ClusteringConfig(
        species,
        p_hash,
        args,
        getattr(args.species, species).external_cluster_prefix,
        fs,
        full_db_fs,
        ref_db_fs,
        db_funcs,
        out_dir,
    )

    sketches_dict = create_sketches_dict(hashes_list, config.fs)
    qNames = preprocess_sketches(sketches_dict, config.out_dir)

    assign_query_clusters(config, config.ref_db_fs, qNames, config.out_dir)
    queries_names, queries_clusters, _, _, _, _, _ = summarise_clusters(
        config.out_dir, species, config.ref_db_fs.db, qNames
    )

    if config.external_clusters_prefix:
        result = handle_external_clusters(
            config,
            sketches_dict,
            queries_names,
            queries_clusters,
        )
    else:
        result = assign_clusters_to_result(
            zip(queries_names, queries_clusters)
        )

    save_result(config, result)
    return result


def setup_output_directory(fs: PoppunkFileStore, p_hash: str) -> str:
    """
    [Create output directory that stores all files from PopPUNK assign job]

    :param fs: [PoppunkFileStore with paths to input files]
    :param p_hash: [project hash]
    :return str: [path to output directory]
    """
    outdir = fs.output(p_hash)
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    return outdir


def create_sketches_dict(hashes_list: list, fs: PoppunkFileStore) -> dict:
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


def assign_query_clusters(
    config: ClusteringConfig,
    db_fs: DatabaseFileStore,
    qNames: list,
    outdir: str,
) -> None:
    """
    [Assign clusters to query samples using PopPUNK assign]

    :param config: [ClusteringConfig with all necessary information]
    :param db_fs: [DatabaseFileStore which provides paths
        to database files]
    :param qNames: [list of sample hashes]
    :param outdir: [path to output directory
        where all files from PopPUNK assign job are stored]
    """
    wrapper = PoppunkWrapper(
        config.fs, db_fs, config.args, config.p_hash, config.species
    )
    wrapper.assign_clusters(config.db_funcs, qNames, outdir)


def handle_external_clusters(
    config: ClusteringConfig,
    sketches_dict: dict,
    queries_names: list,
    queries_clusters: list,
) -> dict:
    """
    [Handles the assignment of external clusters to query sequences.
    This function processes external clusters from a previous query clustering,
    filters out queries that are not found, handles these not found queries,
    updates the external clusters, and finally saves the updated clusters.]

    :param config: [ClusteringConfig with all necessary information]
    :param sketches_dict: [dictionary with filehash (key) and sketch (value)]
    :param queries_names: [list of sample hashes]
    :param queries_clusters: [list of sample PopPUNK clusters]
    :return dict: [dict with filehash (key) and external cluster (value)]
    """
    previous_query_clustering = config.fs.previous_query_clustering(
        config.p_hash
    )
    external_clusters, not_found_query_names = get_external_clusters_from_file(
        previous_query_clustering,
        queries_names,
        config.external_clusters_prefix,
    )
    if not_found_query_names:
        queries_names, queries_clusters = filter_queries(
            queries_names, queries_clusters, not_found_query_names, config
        )
        output_full_tmp = config.fs.output_tmp(config.p_hash)
        not_found_query_names_new, not_found_query_clusters = (
            handle_not_found_queries(
                config,
                sketches_dict,
                not_found_query_names,
                output_full_tmp,
            )
        )
        queries_names.extend(not_found_query_names_new)
        queries_clusters.extend(not_found_query_clusters)
        update_external_clusters(
            config,
            not_found_query_names,
            external_clusters,
            previous_query_clustering,
        )

        # Clean up temporary output directory used to assign to full database
        shutil.rmtree(output_full_tmp)

    save_external_to_poppunk_clusters(
        queries_names,
        queries_clusters,
        external_clusters,
        config.p_hash,
        config.fs,
    )
    return assign_clusters_to_result(external_clusters.items())


def handle_not_found_queries(
    config: ClusteringConfig,
    sketches_dict: dict,
    not_found_query_names: list,
    output_full_tmp: str,
) -> tuple[list, list]:
    """
    [Handles queries that were not found in the
    initial external clusters file.
    This function processes the sketches of the queries that were not found for external clusters from the reference db,
    assigns clusters to them from the full db, and then summarizes the clusters. It also
    copies necessary files and merges partial query graphs.]

    :param config: [ClusteringConfig with all necessary information]
    :param sketches_dict: [dictionary with filehash (key) and sketch (value)]
    :param not_found_query_names: [list of sample hashes that were not found]
    :param output_full_tmp: [path to temporary output directory]
    :return tuple[list, list]: [list initial not found sample hashes,
        list of clusters assigned to initial not found samples]
    """
    not_found_sketches_dict = {
        key: value
        for key, value in sketches_dict.items()
        if key in not_found_query_names
    }
    sketch_to_hdf5(not_found_sketches_dict, output_full_tmp)

    assign_query_clusters(
        config, config.full_db_fs, not_found_query_names, output_full_tmp
    )
    not_found_query_names_new, not_found_query_clusters, _, _, _, _, _ = (
        summarise_clusters(
            output_full_tmp,
            config.species,
            config.full_db_fs.db,
            not_found_query_names,
        )
    )

    copy_include_files(output_full_tmp, config.out_dir)
    merge_txt_files(config.fs.partial_query_graph(config.p_hash), config.fs.partial_query_graph_tmp(config.p_hash))

    return not_found_query_names_new, not_found_query_clusters


def update_external_clusters(
    config: ClusteringConfig,
    not_found_query_names: list,
    external_clusters: dict,
    previous_query_clustering: str,
) -> None:
    """
    [Updates the external clusters with the external clusters found
    in the new previous query clustering from assigning
    using the full database.
    This function reads the external clusters from the
    new previous query clustering file
    and updates the initial external clusters file with the clusters for samples
    that were initially not found, and have now been assigned by the current query with the full database.]

    :param config: [ClusteringConfig
        with all necessary information]
    :param not_found_query_names: [list of sample hashes
        that were not found]
    :param external_clusters: [dict of sample hashes
        to external cluster labels]
    :param previous_query_clustering: [path to previous
        query clustering file]
    """
    not_found_prev_querying = config.fs.external_previous_query_clustering_tmp(
        config.p_hash
    )
    external_clusters_not_found, _ = get_external_clusters_from_file(
        not_found_prev_querying,
        not_found_query_names,
        config.external_clusters_prefix,
    )
    update_external_clusters_csv(
        previous_query_clustering,
        not_found_query_names,
        external_clusters_not_found,
    )
    external_clusters.update(external_clusters_not_found)


def merge_txt_files(main_file: str, merge_file: str) -> None:
    """
    [Merge the contents of the merge file into the main file]

    :param main_file: [path to main file]
    :param merge_file: [path to merge file]
    """

    with open(merge_file, "r") as f:
        failed_lines = set(f.read().splitlines())
    with open(main_file, "r") as f:
        main_lines = set(f.read().splitlines())

    combined_lines = list(main_lines.union(failed_lines))
    with open(main_file, "w") as f:
        f.write("\n".join(combined_lines))


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
            os.rename(
                source_file, dest_file
            )


def filter_queries(
    queries_names: list[str],
    queries_clusters: list[str],
    not_found: list[str],
    config: ClusteringConfig,
) -> tuple[list[str], list[str]]:
    """
    [Filter out queries that were not found in the
        initial external clusters file.
    This function filters out the queries that were not found
        in the initial external clusters file,
    deletes include files for these queries,
        and returns the filtered queries.]

    :param queries_names: [list of sample hashes]
    :param queries_clusters: [list of sample PopPUNK clusters]
    :param not_found: [list of sample hashes that were not found]
    :param config: [ClusteringConfig with all necessary information]
    :return tuple[list[str], list[str]]: [filtered sample hashes,
        filtered sample PopPUNK clusters]
    """
    filtered_names = [name for name in queries_names if name not in not_found]
    filtered_clusters = [
        cluster
        for name, cluster in zip(queries_names, queries_clusters)
        if name not in not_found
    ]

    delete_include_files(
        config.fs,
        config.p_hash,
        set(queries_clusters) - set(filtered_clusters),
    )

    return filtered_names, filtered_clusters


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
        include_file = fs.include_files(p_hash, cluster)
        if os.path.exists(include_file):
            os.remove(include_file)


def assign_clusters_to_result(
    query_cluster_mapping: Union[dict.items, zip]
) -> dict:
    """
    [Assign clusters to the result dictionary,
        where the key is the index and the value is a dictionary
        with the sample hash and the cluster number]

    :param query_cluster_mapping: [dictionary items or zip object
        with sample hash and cluster number]
    :return dict: [dict with index (key)
        and sample hash and cluster number (value)]
    """
    result = {}
    for i, (name, cluster) in enumerate(query_cluster_mapping):
        result[i] = {"hash": name, "cluster": cluster}
    return result


def save_result(config: ClusteringConfig, result: dict) -> None:
    """
    [save result to retrieve when reloading project results - this
    overwrites the initial output file written before the assign
    job ran]

    :param config: [ClusteringConfig with
        all necessary information]
    :param result: [dict with index (key)
        and sample hash and cluster number (value)]
    """
    with open(config.fs.output_cluster(config.p_hash), "wb") as f:
        pickle.dump(result, f)


def save_external_to_poppunk_clusters(
    queries_names: list,
    queries_clusters: list,
    external_clusters: dict,
    p_hash: str,
    fs: PoppunkFileStore,
) -> None:
    """
    [Save a mapping of external to PopPUNK clusters which we'll use
    to pass include files (generated by summarise_clusters) to
    generate_visualisation for microreact]

    :param queries_names: [list of sample hashes, output by summarise_clusters]
    :param queries_clusters: [list of sample PopPUNK clusters, also output by
    summarise_clusters, and with corresponding indices to queries_names]
    :param external_clusters: [dict of sample hashes to external cluster
        labels]
    :param p_hash: [project hash]
    :param fs: [project filestore]
    """
    external_to_poppunk_clusters = {}
    for i, name in enumerate(queries_names):
        external_to_poppunk_clusters[external_clusters[name]] = str(
            queries_clusters[i]
        )
    with open(fs.external_to_poppunk_clusters(p_hash), "wb") as f:
        pickle.dump(external_to_poppunk_clusters, f)
