
import pickle
from pathlib import PurePath
import shutil
from rq import get_current_job
from redis import Redis
import os
from PopPUNK.web import sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs, createOverallLineage
from PopPUNK.lineages import print_overall_clustering

from beebop.poppunkWrapper import PoppunkWrapper
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from beebop.utils import hex_to_decimal


def get_lineages(hashes_list: list,
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
    try:
        # get results from previous job
        current_job = get_current_job(Redis())
        query_strains = current_job.dependency.result

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

        # generate wrapper
        wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)

        # Read querying scheme
        with open(db_paths.lineage_scheme, 'rb') as pickle_file:
            ref_db, rlist, model_dir, clustering_file, \
                args.clustering_col_name, distances, kmers, sketch_sizes, \
                codon_phased, max_search_depth, rank_list, use_accessory, \
                min_count, count_unique_distances, reciprocal_only, \
                strand_preserved, core, accessory, lineage_dbs = \
                pickle.load(pickle_file)

        # add symlink: poppunks function assign_query_hdf5() takes a path as
        # 'output' argument, which is where it will look for a .h5 file, but
        # also writes .dists.pkl and .dists.npy to this location. When running
        # this function for lineage models, we need to point it to the .h5
        # file in /poppunk_output, but using this path as 'output' results in
        # the .dists files from cluster assignment being overwritten by those
        # from lineage assignment. Since these are still needed for the
        # visualisations, writing .dists files to another location is
        # necessary. This is done by setting 'outpu5' to a subfolder of
        # /poppunk_output, and to find the .h5 file this symlink is generated.
        currentwd = os.getcwd()
        target = str(PurePath(currentwd, fs.output(p_hash), f"{p_hash}.h5"))
        link = str(PurePath(currentwd,
                            fs.lineages_output(p_hash),
                            "lineages_output.h5"))
        os.makedirs(fs.lineages_output(p_hash), exist_ok=True)
        os.symlink(target, link)

        overall_lineage = {}
        for strain in query_strains:
            if strain in lineage_dbs.keys():
                lineageClustering = wrapper.assign_lineages(dbFuncs,
                                                            qc_dict,
                                                            lineage_dbs,
                                                            strain,
                                                            query_strains,
                                                            strand_preserved,
                                                            core,
                                                            accessory)
                overall_lineage[strain] = \
                    createOverallLineage(rank_list, lineageClustering)

        # Print combined strain and lineage clustering
        print_overall_clustering(overall_lineage,
                                 fs.lineages_output(p_hash) + '.csv',
                                 qNames)

    finally:
        # remove output folder - otherwise rerunning the code with the same
        # project hash will cause an error when generating the symlink
        shutil.rmtree(fs.lineages_output(p_hash))

    return overall_lineage
