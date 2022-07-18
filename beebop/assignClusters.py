from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
import re
import os

from beebop.poppunkWrapper import PoppunkWrapper


def hex_to_decimal(sketches_dict):
    for sample in list(sketches_dict.values()):
        if type(sample['14'][0]) == str and re.match('0x.*', sample['14'][0]):
            for x in range(14, 30, 3):
                sample[str(x)] = list(map(lambda x: int(x, 16),
                                          sample[str(x)]))


def get_clusters(hashes_list, p_hash, fs, db_paths, args):
    """
    assign clusterIDs to sketches
    hashes_list: list of json objects stored json object of multiple sketches
    """
    # set output directory
    outdir = fs.output(p_hash)
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # create qc_dict
    qc_dict = {'run_qc': False}

    # create dbFuncs
    dbFuncs = setupDBFuncs(args=args.assign, qc_dict=qc_dict)

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
    return result
