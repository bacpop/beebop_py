from unittest.mock import patch
from beebop.services.run_PopPUNK.poppunkWrapper import PoppunkWrapper
from beebop.config import DatabaseFileStore
from tests.setup import fs, args, species
from unittest.mock import Mock


@patch("beebop.services.run_PopPUNK.poppunkWrapper.assign_query_hdf5")
def test_poppunk_wrapper_assign_cluster(mock_assign):
    db_fs = DatabaseFileStore(
        "./storage/GPS_v9_ref", "GPS_v9_external_clusters.csv"
    )
    p_hash = "random hash"
    db_funcs = Mock()
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)

    wrapper.assign_clusters(db_funcs, ["ms1", "ms2"], fs.output(p_hash))

    mock_assign.assert_called_with(
        dbFuncs=db_funcs,
        ref_db=db_fs.db,
        qNames=["ms1", "ms2"],
        output=fs.output(wrapper.p_hash),
        qc_dict=vars(getattr(args.species, species).qc_dict),
        update_db=args.assign.update_db,
        write_references=args.assign.write_references,
        distances=db_fs.distances,
        serial=args.assign.serial,
        threads=args.assign.threads,
        overwrite=args.assign.overwrite,
        plot_fit=args.assign.plot_fit,
        graph_weights=args.assign.graph_weights,
        model_dir=db_fs.db,
        strand_preserved=args.assign.strand_preserved,
        previous_clustering=db_fs.db,
        external_clustering=db_fs.external_clustering,
        core=args.assign.core_only,
        accessory=args.assign.accessory_only,
        gpu_dist=args.assign.gpu_dist,
        gpu_graph=args.assign.gpu_graph,
        save_partial_query_graph=args.assign.save_partial_query_graph,
        stable=args.assign.stable,
        use_full_network=args.assign.use_full_network,
    )
