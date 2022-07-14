import json
import os
import re
from types import SimpleNamespace
from PopPUNK.assign import assign_query_hdf5
from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from tests import setup


def hex_to_decimal(sketches_dict):
    for sample in list(sketches_dict.values()):
        if type(sample['14'][0]) == str and re.match('0x.*', sample['14'][0]):
            for x in range(14, 30, 3):
                sample[str(x)] = list(map(lambda x: int(x, 16),
                                          sample[str(x)]))


storageLocation = './tests/files'
p_hash = 'unit_test_visualisations'
fs = PoppunkFileStore(storageLocation)
outdir = fs.output(p_hash)
if not os.path.exists(outdir):
    os.mkdir(outdir)
db_paths = DatabaseFileStore('./storage/GPS_v4_references')
with open("./beebop/resources/args.json") as a:
    args_json = a.read()
args = json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))
qc_dict = {'run_qc': False}
dbFuncs = setupDBFuncs(args=args.assign, qc_dict=qc_dict)

sketches_dict = json.loads(setup.generate_json())
hex_to_decimal(sketches_dict)

qNames = sketch_to_hdf5(sketches_dict, outdir)

assign_query_hdf5(
        dbFuncs=dbFuncs,
        ref_db=db_paths.db,
        qNames=qNames,
        output=outdir,
        qc_dict=qc_dict,
        update_db=args.assign.update_db,
        write_references=args.assign.write_references,
        distances=db_paths.distances,
        threads=args.assign.threads,
        overwrite=args.assign.overwrite,
        plot_fit=args.assign.plot_fit,
        graph_weights=False,
        max_a_dist=args.assign.max_a_dist,
        max_pi_dist=args.assign.max_pi_dist,
        type_isolate=args.assign.type_isolate,
        model_dir=db_paths.db,
        strand_preserved=args.assign.strand_preserved,
        previous_clustering=db_paths.db,
        external_clustering=args.assign.external_clustering,
        core=args.assign.core_only,
        accessory=args.assign.accessory_only,
        gpu_sketch=args.assign.gpu_sketch,
        gpu_dist=args.assign.gpu_dist,
        gpu_graph=args.assign.gpu_graph,
        deviceid=args.assign.deviceid,
        save_partial_query_graph=args.assign.save_partial_query_graph
    )

summarise_clusters(outdir, args.assign.species, db_paths.db, qNames)
