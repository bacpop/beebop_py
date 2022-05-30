from PopPUNK.assign import assign_query_hdf5
from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
import json
from types import SimpleNamespace
import re
import os

# this depends on where the rqworker is running
storageLocation = './storage'


def get_clusters(hashes_list, p_hash):
    """
    assign clusterIDs to sketches
    hashes_list: list of json objects stored json object of multiple sketches
    """
    # set output directory
    outdir = storageLocation + '/poppunk_output/' + p_hash
    if not os.path.exists(storageLocation + '/poppunk_output'):
        os.mkdir(storageLocation + '/poppunk_output')
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # read arguments
    with open("./beebop/resources/args.json") as a:
        args_json = a.read()
    args = json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))

    # set database path
    db_path = '/home/mmg220/Documents/run_poppunk'
    db_name = 'GPS_v4'
    species_db = db_path + '/' + db_name

    # create qc_dict
    qc_dict = {'run_qc': False}

    # create dbFuncs
    dbFuncs = setupDBFuncs(args=args.assign, qc_dict=qc_dict)

    # transform json to dict
    sketches_dict = {}
    for hash in hashes_list:
        with open(storageLocation+'/json/'+hash+'.json', 'r') as fp:
            sketches_dict[hash] = json.load(fp)

    # convert hex to decimal
    for sample in list(sketches_dict.values()):
        if type(sample['14'][0]) == str and re.match('0x.*', sample['14'][0]):
            for x in range(14, 30, 3):
                sample[str(x)] = list(map(lambda x: int(x, 16),
                                          sample[str(x)]))

    # create hdf5 db
    qNames = sketch_to_hdf5(sketches_dict, outdir)

    print(qNames)

    # run query assignment
    assign_query_hdf5(
        dbFuncs=dbFuncs,
        ref_db=species_db,
        qNames=qNames,
        output=outdir,
        qc_dict=qc_dict,
        update_db=args.assign.update_db,
        write_references=args.assign.write_references,
        distances=species_db + '/' + db_name + '.dists.pkl',
        threads=args.assign.threads,
        overwrite=args.assign.overwrite,
        plot_fit=args.assign.plot_fit,
        graph_weights=False,
        max_a_dist=args.assign.max_a_dist,
        max_pi_dist=args.assign.max_pi_dist,
        type_isolate=args.assign.type_isolate,
        model_dir=species_db,
        strand_preserved=args.assign.strand_preserved,
        previous_clustering=species_db,
        external_clustering=args.assign.external_clustering,
        core=args.assign.core_only,
        accessory=args.assign.accessory_only,
        gpu_sketch=args.assign.gpu_sketch,
        gpu_dist=args.assign.gpu_dist,
        gpu_graph=args.assign.gpu_graph,
        deviceid=args.assign.deviceid,
        save_partial_query_graph=args.assign.save_partial_query_graph
    )

    queries_names, queries_clusters, _, _, _, _, _ = \
        summarise_clusters(outdir, args.assign.species, species_db, qNames)

    result = {}
    for i, (name, cluster) in enumerate(zip(queries_names, queries_clusters)):
        result[i] = {
            "hash": name,
            "cluster": cluster
        }
    return(result)