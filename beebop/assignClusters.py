from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
import os

from beebop.poppunkWrapper import PoppunkWrapper
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from beebop.utils import hex_to_decimal


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

    summarise_clusters(outdir, args.assign.species, db_paths.db, qNames)

    return query_strains
