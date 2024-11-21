from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
from beebop.utils import get_external_clusters_from_file, get_cluster_num
import re
import os
import pickle
import pandas as pd
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.filestore import PoppunkFileStore, DatabaseFileStore


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
    wrapper.assign_clusters(dbFuncs, qNames)

    queries_names, queries_clusters, _, _, _, _, _ = summarise_clusters(
        outdir, species, ref_db_fs.db, qNames
    )

    result = {}
    external_clusters_prefix = getattr(
        args.species, species
    ).external_cluster_prefix
    if external_clusters_prefix:
        previous_query_clustering_file = fs.previous_query_clustering(p_hash)

        external_clusters = get_external_clusters_from_file(
            previous_query_clustering_file,
            queries_names,
            external_clusters_prefix,
        )
        # not matched
        not_found = external_clusters["not_found"]        
        # TODO: only filter if len > 0.. i.e move below code to if block
        filtered_names = [name for name in queries_names if name not in not_found]
        filtered_clusters = [cluster for name, cluster in zip(queries_names, queries_clusters) if name not in not_found]
        if len(not_found) > 0:
            # run assign clusters for failed samples in new directory
            failed_output_dir = f"{outdir}/failed/{p_hash}"
            if not os.path.exists(failed_output_dir):
                os.makedirs(failed_output_dir, exist_ok=True) 
                # create hdf5 db
            failed_sketches_dict = { key: value for key, value in sketches_dict.items() if key in not_found}
            failed_q_names = sketch_to_hdf5(failed_sketches_dict, failed_output_dir)
            wrapper = PoppunkWrapper(fs, full_db_fs, args, p_hash, species)
            wrapper.assign_clusters(dbFuncs, not_found,failed_output_dir)
            
            q_names, q_clusters, _, _, _, _, _ = (
                summarise_clusters(failed_output_dir, species, full_db_fs.db, failed_q_names)
            )
            # copy include_.txt files from failed_output_dir to outdir
            include_files = [f for f in os.listdir(failed_output_dir) if f.startswith("include")]
            for include_file in include_files:
                os.rename(f"{failed_output_dir}/{include_file}", f"{outdir}/{include_file}")
            # copy over .subset file
            failed_subset_file = f"{failed_output_dir}/{p_hash}_query.subset"
            main_subset_file = fs.parital_query_graph(p_hash)
            with open(failed_subset_file, "r") as f:
                failed_lines = set(f.read().splitlines())
            with open(main_subset_file, "r") as f:
                main_lines = set(f.read().splitlines())
            
            combined_lines = list(main_lines.union(failed_lines))
            with open(main_subset_file, "w") as f:
                f.write("\n".join(combined_lines))           
            
            # get external clusters from previous querying    
            failed_prev_querying = f"{failed_output_dir}/{p_hash}_external_clusters.csv" 
            external_clusters_new = get_external_clusters_from_file(
                failed_prev_querying,
                q_names,
                external_clusters_prefix,
            )
            filtered_names.extend(q_names)
            filtered_clusters.extend(q_clusters)
            # update original external_clusters with new found clusters
            df = pd.read_csv(previous_query_clustering_file)
            sample_id_col = df.columns[0]
            cluster_col = df.columns[1]
            mask = df[sample_id_col].isin(q_names)
            df.loc[mask, cluster_col] = [get_cluster_num(external_clusters_new["found"][sample_id]) for sample_id in q_names]
            df.to_csv(previous_query_clustering_file, index=False)
            # update external_clusters             
            external_clusters["found"].update(external_clusters_new["found"])
                        
            

        for i, (name, cluster) in enumerate(
            external_clusters["found"].items()
        ):
            result[i] = {"hash": name, "cluster": cluster}

        save_external_to_poppunk_clusters(
            filtered_names, filtered_clusters, external_clusters["found"], p_hash, fs
        )
    else:
        for i, (name, cluster) in enumerate(
            zip(queries_names, queries_clusters)
        ):
            result[i] = {"hash": name, "cluster": cluster}

    # save result to retrieve when reloading project results - this
    # overwrites the initial output file written before the assign
    # job ran
    with open(fs.output_cluster(p_hash), "wb") as f:
        pickle.dump(result, f)

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
