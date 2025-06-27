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
