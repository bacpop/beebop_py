from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
from beebop.utils import get_external_clusters_from_file, update_external_clusters_csv
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

    :param hashes_list: [list of file hashes from all query samples]
    :param p_hash: [project_hash]
    :param fs: [PoppunkFileStore with paths to input files]
    :param db_fs: [DatabaseFileStore which provides paths
        to database files]
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

    # transform json to dict
    sketches_dict = create_sketches_dict(hashes_list, config.fs)

    # Preprocess sketches
    qNames = preprocess_sketches(sketches_dict, config.out_dir)

    # Run query assignment
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
        result = assign_clusters_to_result(zip(queries_names, queries_clusters))

    save_result(config, result)
    return result

def setup_output_directory(fs: PoppunkFileStore, p_hash: str) -> str:
    outdir = fs.output(p_hash)
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    return outdir

def create_sketches_dict(hashes_list: list, fs: PoppunkFileStore) -> dict:
    return {hash: fs.input.get(hash) for hash in hashes_list}

def preprocess_sketches(sketches_dict: dict, outdir: str) -> list:
    hex_to_decimal(sketches_dict)
    return sketch_to_hdf5(sketches_dict, outdir)

def assign_query_clusters(
    config: ClusteringConfig,
    db_fs: DatabaseFileStore,
    qNames: list,
    outdir: str,
):
    wrapper = PoppunkWrapper(config.fs, db_fs, config.args, config.p_hash, config.species)
    wrapper.assign_clusters(config.db_funcs, qNames, outdir)

def handle_external_clusters(
    config: ClusteringConfig,
    sketches_dict: dict,
    queries_names: list,
    queries_clusters: list,
) -> dict:
    previous_query_clustering = config.fs.previous_query_clustering(config.p_hash)
    external_clusters, not_found_query_names = get_external_clusters_from_file(
        previous_query_clustering, queries_names, config.external_clusters_prefix
    )
    if not_found_query_names:
        queries_names, queries_clusters = filter_queries(
            queries_names, queries_clusters, not_found_query_names, config
        )
        output_full_tmp = config.fs.assign_output_full(config.p_hash)
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
        
        shutil.rmtree(output_full_tmp)

    save_external_to_poppunk_clusters(
        queries_names, queries_clusters, external_clusters, config.p_hash, config.fs
    )
    return assign_clusters_to_result(external_clusters.items())

def handle_not_found_queries(
    config: ClusteringConfig,
    sketches_dict: dict,
    not_found_query_names: list,
    output_full_tmp: str,
) -> tuple[list, list]:
    not_found_sketches_dict = {
        key: value for key, value in sketches_dict.items() if key in not_found_query_names
    }
    sketch_to_hdf5(not_found_sketches_dict, output_full_tmp)
    
    assign_query_clusters(config, config.full_db_fs, not_found_query_names, output_full_tmp)
    not_found_query_names_new, not_found_query_clusters, _, _, _, _, _ = (
            summarise_clusters(output_full_tmp, config.species, config.full_db_fs.db, not_found_query_names) 
        )
    
    copy_include_files(output_full_tmp, config.out_dir)
    merge_partial_query_graphs(config.p_hash, config.fs)
    
    return not_found_query_names_new, not_found_query_clusters

def update_external_clusters(
    config: ClusteringConfig,
    not_found_query_names: list,
    external_clusters: dict,
    previous_query_clustering: str,
):
    not_found_prev_querying = config.fs.external_previous_query_clustering_path_full_assign(config.p_hash)
    external_clusters_not_found, _ = get_external_clusters_from_file(
        not_found_prev_querying, not_found_query_names, config.external_clusters_prefix
    )
    update_external_clusters_csv(
        previous_query_clustering, not_found_query_names, external_clusters_not_found
    )
    external_clusters.update(external_clusters_not_found)

def merge_partial_query_graphs(p_hash: str, fs: PoppunkFileStore) -> None:
    full_assign_subset_file = fs.partial_query_graph_full_assign(p_hash)
    main_subset_file = fs.partial_query_graph(p_hash)
    with open(full_assign_subset_file, "r") as f:
        failed_lines = set(f.read().splitlines())
    with open(main_subset_file, "r") as f:
        main_lines = set(f.read().splitlines())
    
    combined_lines = list(main_lines.union(failed_lines))
    with open(main_subset_file, "w") as f:
        f.write("\n".join(combined_lines))  

def copy_include_files(assign_full_dir: str, outdir: str) -> None:
    include_files = [f for f in os.listdir(assign_full_dir) if f.startswith("include")]
    for include_file in include_files:
        os.rename(f"{assign_full_dir}/{include_file}", f"{outdir}/{include_file}")


def filter_queries(
    queries_names: list[str],
    queries_clusters: list[str],
    not_found: list[str],
    config: ClusteringConfig,
) -> tuple[list[str], list[str]]:
    filtered_names = [name for name in queries_names if name not in not_found]
    filtered_clusters = [cluster for name, cluster in zip(queries_names, queries_clusters) if name not in not_found]

    delete_include_files(config.fs, config.p_hash, set(queries_clusters) - set(filtered_clusters))
    
    return filtered_names, filtered_clusters

def delete_include_files(fs: PoppunkFileStore, p_hash: str, clusters: set) -> None:
    for cluster in clusters:
        include_file = fs.include_files(p_hash, cluster)
        if os.path.exists(include_file):
            os.remove(include_file)

def assign_clusters_to_result(query_cluster_mapping: Union[dict.items, zip]) -> dict:
    result = {}
    for i, (name, cluster) in enumerate(query_cluster_mapping):
        result[i] = {"hash": name, "cluster": cluster}
    return result

def save_result(config: ClusteringConfig, result: dict):
    """
     save result to retrieve when reloading project results - this
     overwrites the initial output file written before the assign
     job ran

    Args:
        fs (PoppunkFileStore): _description_
        p_hash (str): _description_
        result (dict): _description_
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
