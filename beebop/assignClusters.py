from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs, createOverallLineage
from PopPUNK.assign import assign_query_hdf5
from PopPUNK.lineages import print_overall_clustering
import re
import os
import pickle

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
        if type(sample['14'][0]) == str and re.match('0x.*', sample['14'][0]):
            for x in range(14, 30, 3):
                sample[str(x)] = list(map(lambda x: int(x, 16),
                                          sample[str(x)]))


def get_clusters(hashes_list: list,
                 p_hash: str,
                 fs: PoppunkFileStore,
                 db_paths: DatabaseFileStore,
                 args: dict) -> dict:
    """
    Assign cluster numbers to samples using PopPUNK.

    :param hashes_list: [list of file hashes from all query samples]
    :param p_hash: [project_hash]
    :param fs: [PoppunkFileStore with paths to input files]
    :param db_paths: [DatabaseFileStore which provides paths
        to database files]
    :param args: [arguments for Poppunk's assign function, stored in
        resources/args.json]
    :return dict: [dict with filehash (key) and cluster number (value)]
    """
    # set output directory
    outdir = fs.output(p_hash)
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # create qc_dict
    qc_dict = {'run_qc': False}

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
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    isolateClustering = wrapper.assign_clusters(dbFuncs, qc_dict, qNames)

    # --------------------------------------------------------------------------

    # Process clustering
    query_strains = {}
    clustering_type = 'combined'
    for isolate in isolateClustering[clustering_type]:
        if isolate in qNames:
          strain = isolateClustering[clustering_type][isolate]
          if strain in query_strains:
              query_strains[strain].append(isolate)
          else:
              query_strains[strain] = [isolate]

    # Read querying scheme
    with open(db_paths.lineage_scheme, 'rb') as pickle_file:
        ref_db, rlist, model_dir, clustering_file, args.clustering_col_name, distances, \
        kmers, sketch_sizes, codon_phased, max_search_depth, rank_list, use_accessory, min_count, \
        count_unique_distances, reciprocal_only, strand_preserved, core, accessory, lineage_dbs = \
          pickle.load(pickle_file)

    overall_lineage = {}
    for strain in query_strains:
        if strain in lineage_dbs.keys():
            lineage_distances = os.path.join(db_paths.path, lineage_dbs[strain],os.path.basename(lineage_dbs[strain]) + '.dists')
            lineageClustering = \
                assign_query_hdf5(dbFuncs,
                            (db_paths.path + "/" + lineage_dbs[strain]),
                            query_strains[strain],
                            fs.output(p_hash), #output
                            qc_dict,
                            False, # update DB - not yet
                            False, # write references - need to consider whether to support ref-only databases for assignment
                            lineage_distances,
                            False, # serial - needs to be supported for web version?
                            1, #threads
                            True, # overwrite - probably OK?
                            False, # plot_fit - turn off for now
                            False, # graph weights - might be helpful for MSTs not for strains
                            (db_paths.path + "/" + lineage_dbs[strain]),
                            strand_preserved,
                            (db_paths.path + "/" + lineage_dbs[strain]),
                            None, # No external clustering
                            core,
                            accessory,
                            False, # args.gpu_dist,
                            False, # args.gpu_graph,
                            save_partial_query_graph = False)
            overall_lineage[strain] = createOverallLineage(rank_list, lineageClustering)
    
    # Print combined strain and lineage clustering
    print_overall_clustering(overall_lineage,fs.lineages_output(p_hash) + '.csv',qNames)

    # ---------------------------------------------------------------------------

    queries_names, queries_clusters, _, _, _, _, _ = \
        summarise_clusters(outdir, args.assign.species, db_paths.db, qNames)

    result = {}
    for i, (name, cluster) in enumerate(zip(queries_names, queries_clusters)):
        result[i] = {
            "hash": name,
            "cluster": cluster
        }
        print(name)
        print(cluster)
    return result
