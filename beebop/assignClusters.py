from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
from beebop.utils import get_external_clusters_from_file, get_cluster_num
import re
import os
import pickle
import pandas as pd
from typing import Union
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
import shutil


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
    # set output directory
    outdir = fs.output(p_hash)
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # create dbFuncs
    dbFuncs = setupDBFuncs(args=args.assign)

    # transform json to dict
    sketches_dict = {}
    for hash in hashes_list:
        sketches_dict[hash] = fs.input.get(hash)

    # convert hex to decimal
    hex_to_decimal(sketches_dict)

    # create hdf5 db
    qNames = sketch_to_hdf5(sketches_dict, outdir)

    # run query assignment
    wrapper = PoppunkWrapper(fs, ref_db_fs, args, p_hash, species)
    wrapper.assign_clusters(dbFuncs, qNames, outdir)

    queries_names, queries_clusters, _, _, _, _, _ = summarise_clusters(
        outdir, species, ref_db_fs.db, qNames
    )

    external_clusters_prefix = getattr(
        args.species, species
    ).external_cluster_prefix
    if external_clusters_prefix:
        previous_query_clustering_file = fs.previous_query_clustering(p_hash)

        external_clusters, not_found_q_names = get_external_clusters_from_file(
            previous_query_clustering_file,
            queries_names,
            external_clusters_prefix,
        ) 
        if len(not_found_q_names) > 0:
            queries_names, queries_clusters = filter_queries(queries_names, queries_clusters, not_found_q_names)
            # run assign clusters for failed samples in new directory
            assign_full_dir = fs.assign_output_full(p_hash)
            # create hdf5 db
            not_found_sketches_dict = { key: value for key, value in sketches_dict.items() if key in not_found_q_names}
            sketch_to_hdf5(not_found_sketches_dict, assign_full_dir)
            # run poppunk assign
            wrapper = PoppunkWrapper(fs, full_db_fs, args, p_hash, species)
            wrapper.assign_clusters(dbFuncs, not_found_q_names, assign_full_dir)
            
            # summarise and update queries_names and queries_clusters
            not_found_q_names, not_found_q_clusters, _, _, _, _, _ = (
                summarise_clusters(assign_full_dir, species, full_db_fs.db, not_found_q_names) 
            )
            queries_names.extend(not_found_q_names)
            queries_clusters.extend(not_found_q_clusters)
            
            # copy include_.txt files from failed_output_dir to outdir
            copy_include_files(assign_full_dir, outdir)
            # copy over .subset file
            merge_partial_query_graphs(p_hash, fs)
                
            # get external clusters from previous querying    
            not_found_prev_querying = fs.external_previous_query_clustering_path_full_assign(p_hash)
            external_clusters_not_found, _ = get_external_clusters_from_file(
                not_found_prev_querying,
                not_found_q_names,
                external_clusters_prefix,
            )
            # update original external_clusters with new found clusters
            update_external_clusters_csv(previous_query_clustering_file, not_found_q_names, external_clusters_not_found)
            # update external_clusters             
            external_clusters.update(external_clusters_not_found)
                        
        save_external_to_poppunk_clusters(
            queries_names, queries_clusters, external_clusters, p_hash, fs
        )
        # delete full_assign directory as dont need anymore
        shutil.rmtree(assign_full_dir)
        
        result = assign_clusters_to_result(external_clusters.items())
    else:
        result = assign_clusters_to_result(zip(queries_names, queries_clusters))

    # save result to retrieve when reloading project results - this
    # overwrites the initial output file written before the assign
    # job ran
    with open(fs.output_cluster(p_hash), "wb") as f:
        pickle.dump(result, f)

    return result

def update_external_clusters_csv(previous_query_clustering_file: str, not_found_q_names: list, external_clusters_not_found: dict) -> None:
    df = pd.read_csv(previous_query_clustering_file)
    sample_id_col = df.columns[0]
    cluster_col = df.columns[1]
    query_names_mask = df[sample_id_col].isin(not_found_q_names)
    df.loc[query_names_mask, cluster_col] = [get_cluster_num(external_clusters_not_found[sample_id]) for sample_id in not_found_q_names]
    df.to_csv(previous_query_clustering_file, index=False)
    
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
        
def filter_queries(queries_names: list[str], queries_clusters: list[str], not_found: list[str]) -> tuple[list[str], list[str]]:
    filtered_names = [name for name in queries_names if name not in not_found]
    filtered_clusters = [cluster for name, cluster in zip(queries_names, queries_clusters) if name not in not_found]
    return filtered_names, filtered_clusters

def assign_clusters_to_result(query_cluster_mapping: Union[dict.items, zip]) -> dict:
    result = {}
    for i, (name, cluster) in enumerate(query_cluster_mapping):
        result[i] = {"hash": name, "cluster": cluster}
    return result


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
