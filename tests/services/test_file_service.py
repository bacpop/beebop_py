import pickle
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from beebop.config import PoppunkFileStore
from beebop.services.file_service import (
    add_amr_to_metadata,
    add_files,
    get_cluster_assignments,
    get_component_filepath,
    get_failed_samples_internal,
    get_network_files_for_zip,
    setup_db_file_stores,
)
from tests.setup import fs


def test_get_cluster_assignments(tmp_path):
    """
    Test the get_cluster_assignments function.
    """
    fs = Mock(spec=PoppunkFileStore)
    fs.output_cluster.return_value = tmp_path / "cluster_assignments.pkl"
    p_hash = "test_project_hash"
    cluster_data = {
        0: {"hash": "sample1", "cluster": "A", "raw_cluster_num": 1},
        1: {"hash": "sample2", "cluster": "B", "raw_cluster_num": 2},
    }
    with open(fs.output_cluster(p_hash), "wb") as f:
        pickle.dump(cluster_data, f)

    result = get_cluster_assignments(p_hash, fs)
    assert result == cluster_data


def test_get_failed_samples_internal_no_file():
    p_hash = "unit_test_get_clusters_internal"

    result = get_failed_samples_internal(p_hash, fs)

    assert result == {}


def test_get_failed_samples_internal_file_exists():
    p_hash = "unit_test_get_failed_samples_internal"

    result = get_failed_samples_internal(p_hash, fs)

    assert result["3eaf3ff220d15f8b7ce9ee47aaa9b4a9"] == {
        "hash": "3eaf3ff220d15f8b7ce9ee47aaa9b4a9",
        "failReasons": [
            "Failed distance QC (too high)",
            "Failed distance QC (too many zeros)",
        ],
        "failType": "error",
    }
    assert result["6dfg6ff220d15f8b7ce9ee47aaa9b2i8"] == {
        "hash": "6dfg6ff220d15f8b7ce9ee47aaa9b2i8",
        "failReasons": ["Potential novel genotype"],
        "failType": "warning",
    }


@patch("beebop.services.file_service.get_component_filepath")
def test_get_network_files_for_zip(mock_component_filepath):
    component_filename = "component_filename"
    visualise_folder = "visualise_folder"
    cluster_num = "3"
    mock_component_filepath.return_value = f"{visualise_folder}/{component_filename}"

    files = get_network_files_for_zip(visualise_folder, cluster_num)

    mock_component_filepath.assert_called_with(visualise_folder, cluster_num)

    assert files == [
        component_filename,
        f"pruned_{component_filename}",
        f"visualise_{cluster_num}_cytoscape.csv",
    ]


def test_get_component_filepath(tmp_path):
    visualise_folder = tmp_path / "visualise"
    visualise_folder.mkdir()

    # Create matching files
    cluster_num = "1"
    expected_file = visualise_folder / f"visualise_{cluster_num}_component_*.graphml"

    expected_file.touch()

    # Create non-matching files
    (visualise_folder / "other_file.txt").touch()
    (visualise_folder / "visualise_other.graphml").touch()

    result = get_component_filepath(str(visualise_folder), cluster_num)

    assert result == str(expected_file)


def test_get_component_filepath_not_found(tmp_path):
    visualise_folder = tmp_path / "visualise"
    visualise_folder.mkdir()

    # Create matching files
    cluster_num = "1"
    expected_file = visualise_folder / f"visualise_{cluster_num}_component_*.graphml"

    expected_file.touch()

    # Create non-matching files
    (visualise_folder / "other_file.txt").touch()
    (visualise_folder / "visualise_other.graphml").touch()

    with pytest.raises(FileNotFoundError):
        get_component_filepath(str(visualise_folder), "69")


def test_add_files_include_files():
    memory_file = BytesIO()
    add_files(memory_file, "tests/files/sketchlib_input", ["rfile.txt"], False)
    memory_file.seek(0)
    contents2 = memory_file.read()
    assert "rfile.txt".encode("utf-8") in contents2
    assert "6930_8_9.fa".encode("utf-8") not in contents2
    assert "7622_5_91.fa".encode("utf-8") not in contents2


def test_add_files_exclude_files():
    memory_file = BytesIO()
    add_files(memory_file, "tests/files/sketchlib_input", ["rfile.txt"], True)
    memory_file.seek(0)
    contents2 = memory_file.read()
    assert "rfile.txt".encode("utf-8") not in contents2
    assert "6930_8_9.fa".encode("utf-8") in contents2
    assert "7622_5_91.fa".encode("utf-8") in contents2


@patch("os.path.exists")
def test_setup_db_file_stores_both_dbs_exist(mock_exists):
    """Test when both reference and full databases exist"""
    mock_exists.return_value = True

    species_args = Mock(spec=SimpleNamespace)
    species_args.refdb = "ref_database"
    species_args.fulldb = "full_database"
    species_args.external_clusters_file = "clusters.csv"
    species_args.db_metadata_file = "metadata.csv"
    dbs_location = "tests/dbs"

    ref_db_fs, full_db_fs = setup_db_file_stores(species_args, dbs_location)

    # Verify correct paths used
    assert ref_db_fs.db == f"{dbs_location}/ref_database"
    assert full_db_fs.db == f"{dbs_location}/full_database"


@patch("os.path.exists")
def test_setup_db_file_stores_fulldb_missing(mock_exists):
    """Test fallback to refdb when fulldb doesn't exist"""
    mock_exists.return_value = False

    species_args = Mock()
    species_args.refdb = "ref_database"
    species_args.fulldb = "full_database"
    species_args.external_clusters_file = "clusters.csv"
    species_args.db_metadata_file = "metadata.csv"
    dbs_location = "tests/dbs"
    ref_db_fs, full_db_fs = setup_db_file_stores(species_args, dbs_location)

    # Verify ref database path used
    assert ref_db_fs.db == f"{dbs_location}/ref_database"
    assert full_db_fs.db == f"{dbs_location}/ref_database"


def test_add_amr_to_metadata_no_init_metadata(tmp_path):
    fs = Mock()
    fs.tmp_output_metadata.return_value = str(tmp_path / "tmp_output_metadata.csv")
    amr_metadata = [
        {"ID": "sample1", "AMR": "AMR1"},
        {"ID": "sample2", "AMR": "AMR2"},
    ]
    p_hash = "hash"

    add_amr_to_metadata(fs, p_hash, amr_metadata)

    res = pd.read_csv(tmp_path / "tmp_output_metadata.csv")
    fs.tmp_output_metadata.assert_called_once_with(p_hash)
    assert len(res) == 2
    assert res["ID"].tolist() == ["sample1", "sample2"]
    assert res["AMR"].tolist() == ["AMR1", "AMR2"]


def test_add_amr_to_metadata_init_metadata(tmp_path):
    fs = Mock()
    fs.tmp_output_metadata.return_value = str(tmp_path / "tmp_output_metadata.csv")
    metadata = pd.DataFrame(
        {
            "ID": ["sample1", "sample2"],
            "AMR": ["AMR1", "AMR2"],
        }
    )
    metadata.to_csv(tmp_path / "metadata.csv", index=False)
    amr_metadata = [
        {"ID": "sample3", "AMR": "AMR3"},
        {"ID": "sample4", "AMR": "AMR4"},
    ]
    metadata_file = tmp_path / "metadata.csv"
    p_hash = "hash"

    add_amr_to_metadata(fs, p_hash, amr_metadata, metadata_file)

    res = pd.read_csv(tmp_path / "tmp_output_metadata.csv")
    fs.tmp_output_metadata.assert_called_once_with(p_hash)
    assert len(res) == 4
    assert res["ID"].tolist() == ["sample1", "sample2", "sample3", "sample4"]
    assert res["AMR"].tolist() == ["AMR1", "AMR2", "AMR3", "AMR4"]
