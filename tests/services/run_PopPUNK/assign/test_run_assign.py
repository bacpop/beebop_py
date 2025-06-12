import pickle
from unittest.mock import Mock, patch
from pytest_unordered import unordered
from beebop.services.run_PopPUNK.assign.run import (
    assign_clusters_to_result,
    get_internal_clusters_result,
    assign_query_clusters,
    handle_external_clusters,
    handle_not_found_queries,
    save_external_to_poppunk_clusters,
    save_result,
    update_external_clusters,
)
import tests.setup as setup


def test_assign_clusters():
    result = setup.do_assign_clusters("unit_test_poppunk_assign")
    expected = unordered(list(setup.expected_assign_result.values()))
    assert list(result.values()) == expected


def test_get_internal_clusters_result():
    queries_names = ["sample1", "sample2"]
    queries_clusters = ["5", "10"]

    res = get_internal_clusters_result(queries_names, queries_clusters)

    assert res == {
        0: {"hash": "sample1", "cluster": "5", "raw_cluster_num": "5"},
        1: {"hash": "sample2", "cluster": "10", "raw_cluster_num": "10"},
    }


def test_assign_query_clusters(mocker, clustering_config):
    samples = ["sample1", "sample2"]
    wrapper = Mock()
    mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.PoppunkWrapper",
        return_value=wrapper,
    )

    assign_query_clusters(
        clustering_config,
        clustering_config.ref_db_fs,
        samples,
        clustering_config.out_dir,
    )

    wrapper.assign_clusters.assert_called_once_with(
        clustering_config.db_funcs, samples, clustering_config.out_dir
    )


def test_handle_external_clusters_all_found(mocker, clustering_config):
    external_clusters, not_found = {
        "sample1": {"cluster": "GPSC69", "raw_cluster_num": "69"},
        "sample2": {"cluster": "GPSC420", "raw_cluster_num": "420"},
    }, []
    mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.get_external_clusters_from_file",
        return_value=(external_clusters, not_found),
    )
    mock_save_clusters = mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.save_external_to_poppunk_clusters"
    )
    clustering_config.fs.previous_query_clustering.return_value = (
        "previous_query_clustering"
    )

    res = handle_external_clusters(
        clustering_config, {}, ["sample1", "sample2"], ["1", "2"]
    )

    assert res == {
        0: {"hash": "sample1", "cluster": "GPSC69", "raw_cluster_num": "69"},
        1: {"hash": "sample2", "cluster": "GPSC420", "raw_cluster_num": "420"},
    }
    mock_save_clusters.assert_called_once_with(
        ["sample1", "sample2"],
        ["1", "2"],
        external_clusters,
        clustering_config.p_hash,
        clustering_config.fs,
    )


def test_handle_external_clusters_with_not_found(mocker, clustering_config):
    q_names = ["sample1", "sample2", "sample3"]
    q_clusters = ["1", "2", "1000"]
    not_found_q_clusters = {1234, 6969}
    external_clusters, not_found = {
        "sample1": {"cluster": "GPSC69", "raw_cluster_num": "69"},
        "sample2": {"cluster": "GPSC420", "raw_cluster_num": "420"},
    }, ["sample3"]
    mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.get_external_clusters_from_file",
        return_value=(external_clusters, not_found),
    )
    mock_save_clusters = mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.save_external_to_poppunk_clusters"
    )
    # mock function calls for not found queries
    mock_filter_queries = mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.filter_queries",
        return_value=(q_names, q_clusters, not_found_q_clusters),
    )
    mock_handle_not_found = mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.handle_not_found_queries",
        return_value=(q_names, q_clusters),
    )
    mock_update_external_clusters = mocker.patch(
        "beebop.services.run_PopPUNK.assign.run.update_external_clusters"
    )
    mock_shutil_rmtree = mocker.patch("shutil.rmtree")

    tmp_output = "output_tmp"
    clustering_config.fs.previous_query_clustering.return_value = (
        "previous_query_clustering"
    )
    clustering_config.fs.output_tmp.return_value = tmp_output

    res = handle_external_clusters(clustering_config, {}, q_names, q_clusters)

    # not found function calls
    mock_filter_queries.assert_called_once_with(q_names, q_clusters, not_found)
    mock_handle_not_found.assert_called_once_with(
        clustering_config, {}, not_found, tmp_output, not_found_q_clusters
    )
    mock_update_external_clusters.assert_called_once_with(
        clustering_config,
        not_found,
        external_clusters,
        "previous_query_clustering",
    )
    mock_shutil_rmtree.assert_called_once_with(tmp_output)

    # check return calls
    assert res == {
        0: {"hash": "sample1", "cluster": "GPSC69", "raw_cluster_num": "69"},
        1: {"hash": "sample2", "cluster": "GPSC420", "raw_cluster_num": "420"},
    }
    mock_save_clusters.assert_called_once_with(
        q_names,
        q_clusters,
        external_clusters,
        clustering_config.p_hash,
        clustering_config.fs,
    )


@patch("beebop.services.run_PopPUNK.assign.run.sketch_to_hdf5")
@patch("beebop.services.run_PopPUNK.assign.run.assign_query_clusters")
@patch("beebop.services.run_PopPUNK.assign.run.summarise_clusters")
@patch("beebop.services.run_PopPUNK.assign.run.handle_files_manipulation")
def test_handle_not_found_queries(
    mock_files_manipulation,
    mock_summarise,
    mock_assign,
    mock_sketch_to_hdf5,
    clustering_config,
):
    sketches = {"hash1": "sketch sample 1", "hash2": "sketch sample 2"}
    not_found = ["hash2"]
    not_found_query_clusters = {"6969"}
    output_dir = "output_dir"
    mock_summarise.return_value = ["hash1"], [10], "", "", "", "", ""

    query_names, query_clusters = handle_not_found_queries(
        clustering_config,
        sketches,
        not_found,
        output_dir,
        not_found_query_clusters,
    )

    mock_sketch_to_hdf5.assert_called_once_with(
        {"hash2": "sketch sample 2"}, output_dir
    )
    mock_assign.assert_called_once_with(
        clustering_config, clustering_config.full_db_fs, not_found, output_dir
    )
    mock_files_manipulation.assert_called_once_with(
        clustering_config, output_dir, not_found_query_clusters
    )
    assert query_names == ["hash1"]
    assert query_clusters == [10]


@patch("beebop.services.run_PopPUNK.assign.run.process_unassignable_samples")
@patch("beebop.services.run_PopPUNK.assign.run.update_external_clusters_csv")
@patch(
    "beebop.services.run_PopPUNK.assign.run.get_external_clusters_from_file"
)
def test_update_external_clusters(
    mock_get_external_clusters,
    mock_update_external_clusters,
    mock_process_unassignable_samples,
    clustering_config,
):
    previous_query_clustering = "previous_query_clustering"
    clustering_config.fs.external_previous_query_clustering_tmp.return_value = (
        "tmp_previous_query_clustering"
    )
    query_names = ["sample3", "samples4"]
    external_clusters = {
        "sample1": {"cluster": "GPSC69"},
        "sample2": {"cluster": "GPSC420"},
    }
    new_external_clusters = {
        "sample3": {"cluster": "GPSC11"},
        "samples4": {"cluster": "GPSC33"},
    }
    not_found_samples = ["sample4"]
    mock_get_external_clusters.return_value = (
        new_external_clusters,
        not_found_samples,
    )

    update_external_clusters(
        clustering_config,
        query_names,
        external_clusters,
        previous_query_clustering,
    )

    mock_get_external_clusters.assert_called_once_with(
        "tmp_previous_query_clustering",
        query_names,
        clustering_config.external_clusters_prefix,
    )
    mock_update_external_clusters.assert_called_once_with(
        previous_query_clustering, "tmp_previous_query_clustering", query_names
    )
    mock_process_unassignable_samples.assert_called_once_with(
        not_found_samples, clustering_config.fs, clustering_config.p_hash
    )

    assert external_clusters == {
        "sample1": {"cluster": "GPSC69"},
        "sample2": {"cluster": "GPSC420"},
        "sample3": {"cluster": "GPSC11"},
        "samples4": {"cluster": "GPSC33"},
    }


def test_assign_clusters_to_result_dict_items():
    query_cluster_mapping = {
        "sample1": {"cluster": "GPSC69", "raw_cluster_num": "69"},
        "sample2": {"cluster": "GPSC420", "raw_cluster_num": "420"},
    }

    result = assign_clusters_to_result(query_cluster_mapping.items())

    assert result == {
        0: {"hash": "sample1", "cluster": "GPSC69", "raw_cluster_num": "69"},
        1: {"hash": "sample2", "cluster": "GPSC420", "raw_cluster_num": "420"},
    }


def test_assign_clusters_to_result_zip():
    queries_names = ["sample1", "sample2"]
    queries_clusters = [5, 10]
    cluster_info = [
        {"cluster": cluster, "raw_cluster_num": cluster}
        for cluster in queries_clusters
    ]

    result = assign_clusters_to_result(
        zip(
            queries_names,
            cluster_info,
        )
    )

    assert result == {
        0: {"hash": "sample1", "cluster": 5, "raw_cluster_num": 5},
        1: {"hash": "sample2", "cluster": 10, "raw_cluster_num": 10},
    }


def test_save_result(tmp_path, clustering_config):
    assign_result = {
        0: {"hash": "sample1", "cluster": 1},
        1: {"hash": "sample2", "cluster": 2},
    }
    result_path = tmp_path / "output.pkl"
    clustering_config.fs.output_cluster.return_value = str(result_path)

    save_result(clustering_config, assign_result)

    clustering_config.fs.output_cluster.assert_called_once_with(
        clustering_config.p_hash
    )
    assert result_path.exists()
    with open(result_path, "rb") as f:
        assert assign_result == pickle.load(f)


def test_save_external_to_poppunk_clusters(
    tmp_path,
):
    q_names = ["sample1", "sample2", "sample3"]
    q_clusters = ["1", "2", "3"]
    external_clusters = {
        "sample1": {"cluster": "GPSC69", "raw_cluster_num": "69"},
        "sample2": {"cluster": "GPSC420", "raw_cluster_num": "420;908"},
        "sample3": {"cluster": "GPSC69", "raw_cluster_num": "69"},
    }
    fs = Mock()
    external_clusters_path = tmp_path / "external_clusters.pkl"
    fs.external_to_poppunk_clusters.return_value = str(external_clusters_path)

    save_external_to_poppunk_clusters(
        q_names, q_clusters, external_clusters, "test_hash", fs
    )

    fs.external_to_poppunk_clusters.assert_called_once_with("test_hash")
    assert external_clusters_path.exists()
    with open(external_clusters_path, "rb") as f:
        assert pickle.load(f) == {
            "GPSC69": {"1", "3"},
            "GPSC420": {"2"},
        }
