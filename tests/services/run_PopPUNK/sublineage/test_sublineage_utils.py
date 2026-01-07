import json
import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from beebop.config import PoppunkFileStore
from beebop.services.run_PopPUNK.sublineage.sublineage_utils import (
    get_cluster_to_hashes,
    get_query_sublineage_result,
    link_sketches_hdf5,
    save_sublineage_results,
)


@patch("beebop.services.run_PopPUNK.sublineage.sublineage_utils.Redis")
@patch("beebop.services.run_PopPUNK.sublineage.sublineage_utils.get_current_job")
def test_get_cluster_to_hashes_success(mock_get_current_job, mock_redis):
    host = "localhost"
    mock_job = Mock()
    mock_job.dependency.result = {
        0: {"hash": "sample1", "cluster": "5", "raw_cluster_num": "5"},
        1: {"hash": "sample2", "cluster": "10", "raw_cluster_num": "10"},
        2: {"hash": "sample3", "cluster": "5", "raw_cluster_num": "5"},
    }
    mock_get_current_job.return_value = mock_job

    result = get_cluster_to_hashes(host)

    mock_get_current_job.assert_called_once_with(connection=mock_redis.return_value)
    mock_redis.assert_called_once_with(host=host)
    assert result == {
        "5": ["sample1", "sample3"],
        "10": ["sample2"],
    }


@patch("beebop.services.run_PopPUNK.sublineage.sublineage_utils.Redis")
@patch("beebop.services.run_PopPUNK.sublineage.sublineage_utils.get_current_job")
def test_get_cluster_to_hashes_no_current_job(mock_get_current_job, _mock_redis):
    mock_get_current_job.return_value = None

    with pytest.raises(ValueError, match="Current job or its dependencies are not set."):
        get_cluster_to_hashes("localhost")


@patch("beebop.services.run_PopPUNK.sublineage.sublineage_utils.Redis")
@patch("beebop.services.run_PopPUNK.sublineage.sublineage_utils.get_current_job")
def test_get_cluster_to_hashes_no_dependency(mock_get_current_job, _mock_redis):
    mock_job = Mock()
    mock_job.dependency = None
    mock_get_current_job.return_value = mock_job

    with pytest.raises(ValueError, match="Current job or its dependencies are not set."):
        get_cluster_to_hashes("localhost")


def test_link_sketches_hdf5(tmp_path):
    output_path = tmp_path / "output_sublineages.h5"
    query_sketches_path = tmp_path / "query_sketches.h5"
    query_sketches_path.write_text("dummy content")

    fs = Mock(spec=PoppunkFileStore)
    fs.output_sublineages_hdf5.return_value = str(output_path)
    fs.query_sketches_hdf5.return_value = str(query_sketches_path)

    link_sketches_hdf5(fs, "test_project", "42")

    assert os.path.exists(output_path)
    assert os.path.samefile(output_path, query_sketches_path)


def test_link_sketches_hdf5_already_exists(tmp_path):
    output_path = tmp_path / "output_sublineages.h5"
    query_sketches_path = tmp_path / "query_sketches.h5"
    query_sketches_path.write_text("dummy content")
    output_path.write_text("existing content")

    fs = Mock(spec=PoppunkFileStore)
    fs.output_sublineages_hdf5.return_value = str(output_path)
    fs.query_sketches_hdf5.return_value = str(query_sketches_path)

    link_sketches_hdf5(fs, "test_project", "42")

    assert os.path.exists(output_path)
    assert output_path.read_text() == "existing content"


def test_get_query_sublineage_result(tmp_path):
    csv_path = tmp_path / "sublineages.csv"
    csv_content = """Hash,Sublineage,Status
hash1,SL11,Query
hash2,SL2,Reference
hash3,SL22,Query
hash4,SL3,Reference
"""
    csv_path.write_text(csv_content)

    fs = Mock(spec=PoppunkFileStore)
    fs.output_sublineages_csv.return_value = str(csv_path)

    result_df = get_query_sublineage_result(fs, "test_project", "42")

    assert len(result_df) == 2
    assert set(result_df["Hash"]) == {"hash1", "hash3"}
    assert set(result_df["Sublineage"]) == {"SL11", "SL22"}


def test_save_sublineage_results(tmp_path):
    p_hash = "test_project"
    fs = Mock(spec=PoppunkFileStore)
    result_path = tmp_path / "sublineages.json"
    fs.sublineage_results.return_value = str(result_path)

    sublineage_result = pd.DataFrame(
        {
            "id": ["sample1", "sample2"],
            "Rank_5_Lineage": ["SL1", "SL2"],
            "Rank_10_Sublineage": ["SSL1", "SSL2"],
            "Rank_25_Subsublineage": ["SSSL1", "SSSL2"],
            "Rank_50_Subsubsublineage": ["SSSSL1", "SSSSL2"],
            "overall_Lineage": ["L1", "L2"],
            "Status": ["Query", "Query"],
            "Status:colour": ["red", "red"],
        }
    )

    save_sublineage_results(p_hash, fs, sublineage_result)

    assert result_path.exists()

    with open(result_path, "r") as f:
        data = json.load(f)

    assert len(data) == 2
    assert "sample1" in data
    assert "sample2" in data

    assert data["sample1"] == {
        "Rank_5_Lineage": "SL1",
        "Rank_10_Sublineage": "SSL1",
        "Rank_25_Subsublineage": "SSSL1",
        "Rank_50_Subsubsublineage": "SSSSL1",
    }

    assert data["sample2"] == {
        "Rank_5_Lineage": "SL2",
        "Rank_10_Sublineage": "SSL2",
        "Rank_25_Subsublineage": "SSSL2",
        "Rank_50_Subsubsublineage": "SSSSL2",
    }
