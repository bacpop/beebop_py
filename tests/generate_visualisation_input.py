import json
import os
import re
from PopPUNK.assign import assign_query_hdf5
from PopPUNK.web import summarise_clusters, sketch_to_hdf5
from PopPUNK.utils import setupDBFuncs
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from beebop.utils import get_args
from tests import setup


def hex_to_decimal(sketches_dict):
    for sample in list(sketches_dict.values()):
        if isinstance(sample['14'][0], str) and \
                re.match('0x.*', sample['14'][0]):
            for x in range(14, 30, 3):
                sample[str(x)] = list(map(lambda x: int(x, 16),
                                          sample[str(x)]))


storageLocation = './tests/files'
p_hash = 'unit_test_visualisations'
fs = PoppunkFileStore(storageLocation)
fs.ensure_output_dir_exists(p_hash)
outdir = fs.output(p_hash)
db_paths = DatabaseFileStore('./storage/GPS_v8_ref')
args = get_args()
qc_dict = {'run_qc': False}
dbFuncs = setupDBFuncs(args=args.assign)

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
        serial=False,
        threads=args.assign.threads,
        overwrite=args.assign.overwrite,
        plot_fit=args.assign.plot_fit,
        graph_weights=False,
        model_dir=db_paths.db,
        strand_preserved=args.assign.strand_preserved,
        previous_clustering=db_paths.db,
        external_clustering=args.assign.external_clustering,
        core=args.assign.core_only,
        accessory=args.assign.accessory_only,
        gpu_dist=args.assign.gpu_dist,
        gpu_graph=args.assign.gpu_graph,
        save_partial_query_graph=args.assign.save_partial_query_graph
    )

summarise_clusters(outdir, args.assign.species, db_paths.db, qNames)
