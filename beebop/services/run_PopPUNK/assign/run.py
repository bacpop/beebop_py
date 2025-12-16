import pickle
import shutil
from collections import defaultdict
from collections.abc import ItemsView
from types import SimpleNamespace
from typing import Union

from PopPUNK.utils import setupDBFuncs
from PopPUNK.web import sketch_to_hdf5

from beebop.config import DatabaseFileStore, PoppunkFileStore
from beebop.models import ClusteringConfig
from beebop.services.run_PopPUNK.poppunkWrapper import PoppunkWrapper

from .assign_utils import (
    create_sketches_dict,
    filter_queries,
    get_external_clusters_from_file,
    handle_files_manipulation,
    preprocess_sketches,
    process_assign_clusters_csv,
    process_unassignable_samples,
    update_external_clusters_csv,
)


def assign_clusters(
    hashes_list: list,
    p_hash: str,
    fs: PoppunkFileStore,
    ref_db_fs: DatabaseFileStore,
    full_db_fs: DatabaseFileStore,
    args: SimpleNamespace,
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
        fs.output(p_hash),
    )

    sketches_dict = create_sketches_dict(hashes_list, config.fs)
    qNames = preprocess_sketches(sketches_dict, config.out_dir)

    assign_query_clusters(config, config.ref_db_fs, qNames, config.out_dir)

    queries_names, queries_clusters = process_assign_clusters_csv(
        qNames, p_hash, config.full_db_fs, config.fs.output(p_hash)
    )

    if config.external_clusters_prefix:
        result = handle_external_clusters(
            config,
            sketches_dict,
            queries_names,
            queries_clusters,
        )
    else:
        result = get_internal_clusters_result(queries_names, queries_clusters)

    save_result(config, result)
    return result


def get_internal_clusters_result(queries_names: list[str], queries_clusters: list[str]) -> dict:
    """
    [Get internal clusters result]

    :param queries_names: [list of sample hashes]
    :param queries_clusters: [list of sample PopPUNK clusters]
    :return dict: [dict with index (key)
        and sample hash with cluster and raw cluster number (value)]
    """
    return assign_clusters_to_result(
        zip(
            queries_names,
            [{"cluster": cluster, "raw_cluster_num": cluster} for cluster in queries_clusters],
        )
    )


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
    wrapper = PoppunkWrapper(config.fs, db_fs, config.args, config.p_hash, config.species)
    wrapper.assign_clusters(config.db_funcs, qNames, outdir)


def handle_external_clusters(
    config: ClusteringConfig,
    sketches_dict: dict,
    queries_names: list[str],
    queries_clusters: list[str],
) -> dict[int, dict[str, str]]:
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
    previous_query_clustering = config.fs.previous_query_clustering(config.p_hash)
    external_clusters, not_found_query_names = get_external_clusters_from_file(
        previous_query_clustering,
        queries_names,
        config.external_clusters_prefix,
    )
    if not_found_query_names:
        queries_names, queries_clusters, not_found_query_clusters = filter_queries(
            queries_names, queries_clusters, not_found_query_names
        )
        output_full_tmp = config.fs.output_tmp(config.p_hash)
        found_query_names_full_db, found_query_clusters_full_db = handle_not_found_queries(
            config,
            sketches_dict,
            not_found_query_names,
            output_full_tmp,
            not_found_query_clusters,
        )
        queries_names.extend(found_query_names_full_db)
        queries_clusters.extend(found_query_clusters_full_db)
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
    not_found_query_clusters: set[str],
) -> tuple[list, list]:
    """
    [Handles queries that were not found in the
    initial external clusters file.
    This function processes the sketches of the queries that were not found
    for external clusters from the reference db,
    assigns clusters to them from the full db, and then
    summarizes the clusters. It also
    handles all file manipulations needed]

    :param config: [ClusteringConfig with all necessary information]
    :param sketches_dict: [dictionary
        with filehash (key) and sketch (value)]
    :param not_found_query_names: [list of sample hashes that were not found]
    :param output_full_tmp: [path to temporary output directory]
    :param not_found_query_clusters: [set of clusters assigned to
        initial not found samples]
    :return tuple[list, list]: [list initial not found sample hashes,
        list of clusters assigned to initial not found samples]
    """
    not_found_sketches_dict = {key: value for key, value in sketches_dict.items() if key in not_found_query_names}
    sketch_to_hdf5(not_found_sketches_dict, output_full_tmp)

    assign_query_clusters(config, config.full_db_fs, not_found_query_names, output_full_tmp)

    query_names, query_clusters = process_assign_clusters_csv(
        not_found_query_names, config.p_hash, config.full_db_fs, config.fs.output_tmp(config.p_hash)
    )

    handle_files_manipulation(
        config,
        output_full_tmp,
        not_found_query_clusters,
    )

    return query_names, query_clusters


def update_external_clusters(
    config: ClusteringConfig,
    found_in_full_db_query_names: list[str],
    external_clusters: dict[str, dict[str, str]],
    previous_query_clustering: str,
) -> None:
    """
    [Updates the external clusters with the external clusters found
    in previous query clustering from assigning
    using the full database.
    This function reads the external clusters from the
    new previous query clustering file
    and updates the initial external clusters
    file on ref db with the clusters for samples
    that were initially not found, and have now been
    assigned by the current query with the full database.]

    :param config: [ClusteringConfig
        with all necessary information]
    :param found_in_full_db_query_names: [list of sample hashes
        that were not found in the initial external clusters file]
    :param external_clusters: [dict of sample hashes
        to external cluster labels]
    :param previous_query_clustering: [path to previous
        query clustering file]
    """
    not_found_prev_querying = config.fs.external_previous_query_clustering_tmp(config.p_hash)

    update_external_clusters_csv(
        previous_query_clustering,
        not_found_prev_querying,
        found_in_full_db_query_names,
    )

    external_clusters_full_db, not_found_query_names_full_db = get_external_clusters_from_file(
        not_found_prev_querying,
        found_in_full_db_query_names,
        config.external_clusters_prefix,
    )

    process_unassignable_samples(not_found_query_names_full_db, config.fs, config.p_hash)

    external_clusters.update(external_clusters_full_db)


def assign_clusters_to_result(
    query_cluster_mapping: Union[ItemsView[str, dict[str, str]], zip],
) -> dict[int, dict[str, str]]:
    """
    [Assign clusters to the result dictionary,
        where the key is the index and the value is a dictionary
        with the sample hash, cluster, raw_cluster_num]

    :param query_cluster_mapping: [dictionary items or zip object
        with sample hash and cluster number]
    :return dict: [dict with index (key)
        and sample hash and cluster number (value)]
    """
    result = {}
    for i, (p_hash, cluster_info) in enumerate(query_cluster_mapping):
        result[i] = {
            "hash": p_hash,
            "cluster": cluster_info["cluster"],
            "raw_cluster_num": cluster_info["raw_cluster_num"],
        }
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
    queries_names: list[str],
    queries_clusters: list[str],
    external_clusters: dict[str, dict[str, str]],
    p_hash: str,
    fs: PoppunkFileStore,
) -> None:
    """
    [Save a mapping of external to internal PopPUNK clusters which we'll use
    to pass include files (generated by summarise_clusters) to
    generate_visualisation for microreact.
    Usually this will be a 1:1 mapping, but in some cases
    an external cluster may map to multiple PopPUNK clusters.]

    :param queries_names: [list of sample hashes, output by summarise_clusters]
    :param queries_clusters: [list of sample PopPUNK clusters, also output by
    summarise_clusters, and with corresponding indices to queries_names]
    :param external_clusters: [dict of sample hashes to external cluster
        labels]
    :param p_hash: [project hash]
    :param fs: [project filestore]
    """
    external_to_poppunk_clusters: defaultdict[str, set[str]] = defaultdict(set)
    for i, name in enumerate(queries_names):
        external_cluster = external_clusters.get(name, {}).get("cluster")
        if external_cluster is None:
            continue
        external_to_poppunk_clusters[external_cluster].add(queries_clusters[i])

    with open(fs.external_to_poppunk_clusters(p_hash), "wb") as f:
        pickle.dump(external_to_poppunk_clusters, f)
