import os
import random
import string
from pathlib import PurePath
from unittest.mock import patch

import pytest

from beebop.config.filepaths import (
    DatabaseFileStore,
    FileStore,
    PoppunkFileStore,
)


def test_set_metadata_database_filestore():
    metadata_file = "metadata.csv"

    db_fs = DatabaseFileStore("./storage/dbs/GPS_v9_ref", db_metadata_file=metadata_file)

    assert db_fs.metadata == str(PurePath("beebop", "resources", metadata_file))


def test_set_sublineages_database_filestore():
    sublineages_db = "GPS_v9_sub_lineages"

    db_fs = DatabaseFileStore("./storage/GPS_v9", sublineages_db=sublineages_db)

    assert db_fs.sublineages_db_path == str(PurePath("storage", sublineages_db))
    assert db_fs.sublineages_prefix == "GPS_v9"


def test_get_sublineages_model_path_error():
    db_fs = DatabaseFileStore("./storage/dbs/GPS_v9_ref")

    with pytest.raises(ValueError, match="Sub-lineages database path is not provided."):
        db_fs.get_sublineages_model_path("GPSC2")


def test_get_sublineages_model_path_success():
    sublineages_db = "GPS_v9_sub_lineages"
    db_fs = DatabaseFileStore("./storage/GPS_v9", sublineages_db=sublineages_db)

    assert db_fs.sublineages_db_path is not None
    expected_path = str(PurePath(db_fs.sublineages_db_path, "GPS_v9_GPSC2_lineage_db"))
    result_path = db_fs.get_sublineages_model_path("GPSC2")

    assert result_path == expected_path


def test_get_sublineages_distances_path_error():
    db_fs = DatabaseFileStore("./storage/dbs/GPS_v9_ref")

    with pytest.raises(ValueError, match="Sub-lineages database path is not provided."):
        db_fs.get_sublineages_distances_path("GPSC2")


def test_get_sublineages_distances_path_success():
    sublineages_db = "GPS_v9_sub_lineages"
    db_fs = DatabaseFileStore("./storage/GPS_v9", sublineages_db=sublineages_db)
    cluster = "GPSC2"

    assert db_fs.sublineages_db_path is not None
    expected_path = str(PurePath(db_fs.get_sublineages_model_path(cluster), "GPS_v9_GPSC2_lineage_db.dists"))

    result_path = db_fs.get_sublineages_distances_path(cluster)

    assert result_path == expected_path


def test_tmp_output_metadata(tmp_path):
    fs = PoppunkFileStore(tmp_path)

    result = fs.tmp_output_metadata("hash")

    assert result == str(PurePath(fs.tmp("hash"), "metadata.csv"))


def test_pruned_network_output_component(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "hash"
    component = "909;1;2"
    cluster = "4"

    result = fs.pruned_network_output_component(p_hash, component, cluster)

    assert result == str(
        PurePath(
            fs.output_visualisations(p_hash, cluster),
            f"pruned_visualise_{cluster}_component_{component}.graphml",
        )
    )


@patch("os.makedirs")
@patch("os.path.exists")
@patch("shutil.rmtree")
def test_setup_output_directory_removes_existing_directory(mock_rmtree, mock_exists, mock_makedirs, tmp_path):
    fs = PoppunkFileStore(tmp_path)
    # Test when the directory already exists
    mock_exists.return_value = True
    directory = fs.output("mock_hash")

    fs.setup_output_directory("mock_hash")

    mock_exists.assert_called_once_with(directory)
    mock_rmtree.assert_called_once_with(directory)
    mock_makedirs.assert_called_with(directory)
    assert os.path.exists(directory)


def test_partial_query_graph(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    expected_path = str(PurePath(fs.output(p_hash), f"{p_hash}_query.subset"))
    assert fs.partial_query_graph(p_hash) == expected_path


@patch("os.makedirs")
def test_tmp(mock_makedirs, tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    expected_path = PurePath(fs.output(p_hash), "tmp")

    # Call the tmp method
    result_path = fs.tmp(p_hash)

    # Check if the directory is created and the path is correct
    mock_makedirs.assert_called_with(expected_path, exist_ok=True)
    assert result_path == str(expected_path)


@patch("os.makedirs")
def test_output_sublineages_folder(mock_makedirs, tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    cluster_num = "3"
    expected_path = PurePath(fs.output(p_hash), f"sublineage_{cluster_num}")

    result_path = fs.output_sublineages_folder(p_hash, cluster_num)

    mock_makedirs.assert_called_with(expected_path, exist_ok=True)
    assert result_path == str(expected_path)


def test_output_sublineages_hdf5(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    cluster_num = "3"
    foldername = f"sublineage_{cluster_num}"
    expected_path = str(PurePath(fs.output(p_hash), foldername, f"{foldername}.h5"))

    result_path = fs.output_sublineages_hdf5(p_hash, cluster_num)

    assert result_path == expected_path


def test_output_sublineages_csv(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    cluster_num = "3"
    foldername = f"sublineage_{cluster_num}"
    expected_path = str(PurePath(fs.output(p_hash), foldername, f"{foldername}_lineages.csv"))

    result_path = fs.output_sublineages_csv(p_hash, cluster_num)

    assert result_path == expected_path


def test_sublineage_results(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    expected_path = str(PurePath(fs.output(p_hash), "sublineage_results.json"))

    result_path = fs.sublineage_results(p_hash)

    assert result_path == expected_path


def test_tmp_output_cluster_metadata(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    cluster_num = "1"
    expected_path = str(PurePath(fs.tmp(p_hash), f"metadata_cluster_{cluster_num}.csv"))

    result_path = fs.tmp_output_cluster_metadata(p_hash, cluster_num)

    assert result_path == expected_path


def test_query_sketches_hdf5(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "test_hash"
    expected_path = str(PurePath(fs.output(p_hash), f"{p_hash}.h5"))

    result_path = fs.query_sketches_hdf5(p_hash)

    assert result_path == expected_path


def test_setup_output_directory(tmp_path):
    fs = PoppunkFileStore(tmp_path)
    p_hash = "unit_test_get_clusters_internal"

    fs.setup_output_directory(p_hash)

    assert os.path.exists(fs.output(p_hash))


def test_filestore():
    fs_test = FileStore("./tests/results/json")
    # check for existing file
    assert fs_test.exists("e868c76fec83ee1f69a95bd27b8d5e76") is True
    # get existing sketch
    fs_test.get("e868c76fec83ee1f69a95bd27b8d5e76")
    # raises exception when trying to get non-existent sketch
    with pytest.raises(Exception):
        fs_test.get("random_non_existent_hash")
    # stores new hash
    characters = string.ascii_letters + string.digits
    new_hash = "".join(random.choice(characters) for i in range(32))
    new_sketch = {"random": "input"}
    assert fs_test.exists(new_hash) is False
    fs_test.put(new_hash, new_sketch)
    assert fs_test.exists(new_hash) is True
