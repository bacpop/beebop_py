from unittest.mock import Mock, patch

import pandas as pd

from beebop.models.enums import FailedSampleType
from beebop.services.run_PopPUNK.assign.assign_utils import (
    copy_include_files,
    create_sketches_dict,
    delete_include_files,
    filter_queries,
    get_df_filtered_by_samples,
    get_df_sample_mask,
    get_external_cluster_nums,
    get_external_clusters_from_file,
    handle_files_manipulation,
    hex_to_decimal,
    merge_txt_files,
    preprocess_sketches,
    process_unassignable_samples,
    update_external_clusters_csv,
)
from tests.setup import fs


@patch("beebop.services.run_PopPUNK.assign.assign_utils.get_external_cluster_nums")
def test_update_external_clusters_csv(mock_get_external_cluster_nums, sample_clustering_csv):
    not_found_samples = ["sample1", "sample3"]
    sample_cluster_num_mapping = {"sample1": "11", "sample3": "69;191"}
    source_query_clustering = "tmp_query_clustering.csv"
    mock_get_external_cluster_nums.return_value = sample_cluster_num_mapping
    update_external_clusters_csv(
        sample_clustering_csv,
        source_query_clustering,
        not_found_samples,
    )

    df = pd.read_csv(sample_clustering_csv)

    mock_get_external_cluster_nums.assert_called_once_with(source_query_clustering, not_found_samples)
    assert df.loc[df["sample"] == "sample1", "Cluster"].values[0] == "11"
    assert df.loc[df["sample"] == "sample2", "Cluster"].values[0] == "309;20;101"  # Unchanged
    assert df.loc[df["sample"] == "sample3", "Cluster"].values[0] == "69;191"
    assert df.loc[df["sample"] == "sample4", "Cluster"].values[0] == "40"  # Unchanged


def test_get_df_sample_mask(sample_clustering_csv):
    """Test getting mask for existing samples"""
    samples = ["sample1", "sample3"]

    df, mask = get_df_sample_mask(sample_clustering_csv, samples)

    # Check DataFrame
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert list(df.columns) == ["sample", "Cluster"]

    # Check column types
    assert df["sample"].dtype == object  # string type in pandas
    assert df["Cluster"].dtype == object  # string type in pandas

    # Check mask
    assert isinstance(mask, pd.Series)
    assert mask.tolist() == [True, False, True, False, False]
    assert sum(mask) == 2


def test_get_external_cluster_nums(sample_clustering_csv):
    samples = ["sample1", "sample2"]

    result = get_external_cluster_nums(sample_clustering_csv, samples)

    assert result == {
        "sample1": "10",
        "sample2": "309;20;101",
    }


def test_get_df_filtered_by_samples(sample_clustering_csv):
    """Test getting mask for existing samples"""
    samples = ["sample1", "sample3"]

    filtered_df = get_df_filtered_by_samples(sample_clustering_csv, samples)

    # Check DataFrame
    assert isinstance(filtered_df, pd.DataFrame)
    assert len(filtered_df) == 2
    assert list(filtered_df["sample"]) == ["sample1", "sample3"]


def test_get_external_clusters_from_file(sample_clustering_csv):
    samples = ["sample1", "sample2", "sample5"]
    prefix = "PRE"

    external_clusters, not_found = get_external_clusters_from_file(sample_clustering_csv, samples, prefix)

    assert not_found == ["sample5"]
    assert external_clusters["sample1"] == {
        "cluster": "PRE10",
        "raw_cluster_num": "10",
    }
    assert external_clusters["sample2"] == {
        "cluster": "PRE20",
        "raw_cluster_num": "309;20;101",
    }


def test_create_sketches_dict():
    sketches = {
        "e868c76fec83ee1f69a95bd27b8d5e76": fs.input.get("e868c76fec83ee1f69a95bd27b8d5e76"),
        "f3d9b387e311d5ab59a8c08eb3545dbb": fs.input.get("f3d9b387e311d5ab59a8c08eb3545dbb"),
    }

    sketches_dict = create_sketches_dict(list(sketches.keys()), fs)

    assert sketches_dict == sketches


@patch("beebop.services.run_PopPUNK.assign.assign_utils.hex_to_decimal")
@patch("beebop.services.run_PopPUNK.assign.assign_utils.sketch_to_hdf5")
def test_preprocess_sketches(mock_sketch_to_hdf5, mock_hex_to_decimal):
    sketches = {"hash1": "sketch sample 1", "hash2": "sketch sample 2"}
    outdir = "outdir"
    mock_sketch_to_hdf5.return_value = list(sketches.keys())

    hashes = preprocess_sketches(sketches, outdir)

    assert hashes == list(sketches.keys())
    mock_hex_to_decimal.assert_called_once_with(sketches)
    mock_sketch_to_hdf5.assert_called_once_with(sketches, outdir)


def test_hex_to_decimal():
    dummy_sketch = {
        "sample1": {
            "14": ["0x2964619C7"],
            "17": ["0x52C8C338E"],
            "20": ["0x7C2D24D55"],
            "23": ["0xA5918671C"],
            "26": ["0xCEF5E80E3"],
            "29": ["0xF85A49AAA"],
        }
    }
    dummy_converted = {
        "sample1": {
            "14": [11111111111],
            "17": [22222222222],
            "20": [33333333333],
            "23": [44444444444],
            "26": [55555555555],
            "29": [66666666666],
        }
    }

    hex_to_decimal(dummy_sketch)

    assert dummy_sketch == dummy_converted


def test_filter_queries():
    q_names = ["sample1", "sample2", "sample3"]
    q_clusters = ["1", "2", "3"]
    not_found = ["sample2"]

    filtered_names, filtered_clusters, not_found_q_clusters = filter_queries(q_names, q_clusters, not_found)

    assert filtered_names == ["sample1", "sample3"]
    assert filtered_clusters == ["1", "3"]
    assert not_found_q_clusters


@patch("beebop.services.run_PopPUNK.assign.assign_utils.merge_txt_files")
@patch("beebop.services.run_PopPUNK.assign.assign_utils.copy_include_files")
@patch("beebop.services.run_PopPUNK.assign.assign_utils.delete_include_files")
def test_handle_files_manipulation(mock_delete, mock_copy, mock_merge, clustering_config):
    outdir_tmp = "outdir_tmp"
    not_found_query_clusters = {"1234", "6969"}
    clustering_config.fs.partial_query_graph.return_value = "partial_query_graph"
    clustering_config.fs.partial_query_graph_tmp.return_value = "partial_query_graph_tmp"

    handle_files_manipulation(clustering_config, outdir_tmp, not_found_query_clusters)

    mock_delete.assert_called_once_with(
        clustering_config.fs,
        clustering_config.p_hash,
        not_found_query_clusters,
    )
    mock_copy.assert_called_once_with(outdir_tmp, clustering_config.out_dir)
    mock_merge.assert_called_once_with("partial_query_graph", "partial_query_graph_tmp")


def test_delete_include_files(tmp_path):
    fs = Mock()
    fs.include_file.side_effect = lambda _p_hash, cluster: str(tmp_path / f"include_{cluster}.txt")
    clusters = {"10", "15", "20"}

    for cluster in clusters:
        (tmp_path / f"include_{cluster}.txt").touch()

    delete_include_files(fs, "test_hash", clusters)

    assert fs.include_file.call_count == len(clusters)
    for cluster in clusters:
        fs.include_file.assert_any_call("test_hash", cluster)
        assert not (tmp_path / f"include_{cluster}.txt").exists()


def test_copy_include_files_no_conflict(tmp_path):
    output_full_tmp = tmp_path / "output_full_tmp"
    outdir = tmp_path / "outdir"
    output_full_tmp.mkdir()
    outdir.mkdir()

    # Create some dummy include files in output_full_tmp
    include_files = ["include_1.txt", "include_2.txt", "include_test.txt"]
    other_files = ["other.txt", "data.csv"]

    # Create include files
    for f in include_files:
        (output_full_tmp / f).write_text("test content")

    # Create other non-include files
    for f in other_files:
        (output_full_tmp / f).write_text("other content")

    # Run the function
    copy_include_files(str(output_full_tmp), str(outdir))

    # Check include files were copied
    for f in include_files:
        assert not (output_full_tmp / f).exists()  # Original removed
        assert (outdir / f).exists()  # New location exists
        assert (outdir / f).read_text() == "test content"  # Content preserved

    # Check non-include files were not copied
    for f in other_files:
        assert (output_full_tmp / f).exists()  # Still in original location
        assert not (outdir / f).exists()  # Not in new location


def test_copy_include_file_conflict(tmp_path):
    output_full_tmp = tmp_path / "output_full_tmp"
    outdir = tmp_path / "outdir"
    output_full_tmp.mkdir()
    outdir.mkdir()

    include_files_tmp = [
        "include_1.txt",
    ]
    include_files = ["include_1.txt"]

    # Create include files
    (output_full_tmp / include_files_tmp[0]).write_text("new content")
    (outdir / include_files[0]).write_text("original content")

    copy_include_files(str(output_full_tmp), str(outdir))

    assert not (output_full_tmp / include_files_tmp[0]).exists()  # Original removed
    included_file_content = (outdir / include_files[0]).read_text()
    assert "new content" in included_file_content  # New content
    assert "original content" in included_file_content  # Original content


def test_merge_partial_query_graphs(tmp_path):
    tmp_file = tmp_path / "tmp_query.subset"
    main_file = tmp_path / "main_query.subset"

    tmp_file.write_text("sample2\nsample10\n")
    main_file.write_text("sample1\nsample2\nsample3\nsample4\n")

    merge_txt_files(main_file, tmp_file)

    main_file_queries = list(main_file.read_text().splitlines())

    assert len(main_file_queries) == 5
    assert sorted(main_file_queries) == sorted(["sample1", "sample2", "sample3", "sample4", "sample10"])


def test_process_unassignable_samples(tmp_path):
    unassignable_samples = ["sample1", "sample2"]
    strain_assignment_error = "Unable to assign to an existing strain - potentially novel genotype."
    expected_output = [
        f"{sample}\t{strain_assignment_error}\t{FailedSampleType.WARNING.value}" for sample in unassignable_samples
    ]
    fs = Mock()
    report_path = tmp_path / "qc_report.txt"

    fs.output_qc_report.return_value = str(report_path)

    process_unassignable_samples(unassignable_samples, fs, "hash")

    fs.output_qc_report.assert_called_once_with("hash")

    qc_report_lines = list(report_path.read_text().splitlines())
    assert qc_report_lines == expected_output


def test_process_unassignable_samples_no_samples():
    fs = Mock()

    process_unassignable_samples([], fs, "")

    fs.output_qc_report.assert_not_called()
