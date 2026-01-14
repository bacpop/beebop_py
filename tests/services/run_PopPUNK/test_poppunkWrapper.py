from unittest.mock import Mock, patch

from beebop.config import DatabaseFileStore
from beebop.services.run_PopPUNK.poppunkWrapper import PoppunkWrapper
from tests.setup import args, fs, species


@patch("beebop.services.run_PopPUNK.poppunkWrapper.assign_query_hdf5")
def test_poppunk_wrapper_assign_cluster(mock_assign):
    db_fs = DatabaseFileStore("./storage/GPS_v9_ref", "GPS_v9_external_clusters.csv")
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


@patch("beebop.services.run_PopPUNK.poppunkWrapper.assign_query_hdf5")
def test_poppunk_wrapper_assign_sublineages(mock_assign):
    db_fs = DatabaseFileStore("./storage/GPS_v9_ref", "GPS_v9_external_clusters.csv")
    p_hash = "random hash"
    db_funcs = Mock()
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)

    model_folder = "/path/to/sublineage/model"
    distances = "/path/to/sublineage/distances"
    output_dir = fs.output(p_hash)

    wrapper.assign_sublineages(db_funcs, ["ms1", "ms2"], output_dir, model_folder, distances)

    mock_assign.assert_called_once_with(
        dbFuncs=db_funcs,
        ref_db=db_fs.db,
        qNames=["ms1", "ms2"],
        output=output_dir,
        qc_dict={"run_qc": False},
        update_db=args.assign.update_db,
        write_references=args.assign.write_references,
        distances=distances,
        serial=args.assign.serial,
        threads=args.assign.threads,
        overwrite=False,
        plot_fit=args.assign.plot_fit,
        graph_weights=args.assign.graph_weights,
        model_dir=model_folder,
        strand_preserved=args.assign.strand_preserved,
        previous_clustering=None,
        external_clustering=None,
        core=args.assign.core_only,
        accessory=args.assign.accessory_only,
        gpu_dist=args.assign.gpu_dist,
        gpu_graph=args.assign.gpu_graph,
        save_partial_query_graph=False,
        stable=args.assign.stable,
        use_full_network=args.assign.use_full_network,
    )


@patch("beebop.services.run_PopPUNK.poppunkWrapper.get_metadata_with_sublineages")
@patch("beebop.services.run_PopPUNK.poppunkWrapper.shutil.which")
@patch("beebop.services.run_PopPUNK.poppunkWrapper.generate_visualisations")
def test_poppunk_wrapper_create_visualisations(mock_generate_vis, mock_which, mock_get_metadata):
    db_fs = DatabaseFileStore("./storage/GPS_v9_ref", "GPS_v9_external_clusters.csv")
    p_hash = "random hash"
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)

    cluster = "GPSC10"
    include_file = "/path/to/include10.txt"
    mock_which.return_value = "/usr/bin/rapidnj"
    mock_get_metadata.return_value = "/path/to/merged_metadata.csv"

    wrapper.create_visualisations(cluster, include_file)

    mock_which.assert_called_with("rapidnj")
    mock_get_metadata.assert_called_once_with(fs, p_hash, cluster)

    mock_generate_vis.assert_called_once_with(
        query_db=fs.output(p_hash),
        ref_db=db_fs.db,
        distances=None,
        rank_fit=None,
        threads=args.visualise.threads,
        output=fs.output_visualisations(p_hash, cluster),
        gpu_dist=args.visualise.gpu_dist,
        deviceid=args.visualise.deviceid,
        external_clustering=db_fs.external_clustering,
        microreact=args.visualise.microreact,
        phandango=args.visualise.phandango,
        grapetree=args.visualise.grapetree,
        cytoscape=args.visualise.cytoscape,
        perplexity=args.visualise.perplexity,
        maxIter=args.visualise.maxIter,
        strand_preserved=args.visualise.strand_preserved,
        include_files=include_file,
        model_dir=db_fs.db,
        previous_clustering=db_fs.previous_clustering,
        previous_query_clustering=fs.previous_query_clustering(p_hash),
        previous_mst=None,
        previous_distances=None,
        network_file=None,
        gpu_graph=args.visualise.gpu_graph,
        info_csv=mock_get_metadata.return_value,
        rapidnj="/usr/bin/rapidnj",
        api_key=None,
        tree=args.visualise.tree,
        mst_distances=args.visualise.mst_distances,
        overwrite=args.visualise.overwrite,
        display_cluster=args.visualise.display_cluster,
        read_distances=args.visualise.read_distances,
        use_partial_query_graph=fs.partial_query_graph(p_hash),
        tmp=fs.tmp(p_hash),
        extend_query_graph=args.visualise.extend_query_graph,
    )
