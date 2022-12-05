from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
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
    wrapper.assign_clusters(dbFuncs, qc_dict, qNames)

    queries_names, queries_clusters, _, _, _, _, _ = \
        summarise_clusters(outdir, args.assign.species, db_paths.db, qNames)

    result = {}
    for i, (name, cluster) in enumerate(zip(queries_names, queries_clusters)):
        result[i] = {
            "hash": name,
            "cluster": cluster
        }

    # save result to retrieve when reloading project results
    with open(fs.output_cluster(p_hash), 'wb') as f:
        pickle.dump(result, f)

    return result
