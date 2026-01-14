from unittest.mock import Mock, patch

import pandas as pd
import pytest

from beebop.config import DatabaseFileStore, PoppunkFileStore
from beebop.services.run_PopPUNK.sublineage.run import assign_cluster_sublineages, assign_sublineages


@patch("beebop.services.run_PopPUNK.sublineage.run.get_cluster_to_hashes")
@patch("beebop.services.run_PopPUNK.sublineage.run.save_sublineage_results")
@patch("beebop.services.run_PopPUNK.sublineage.run.assign_cluster_sublineages")
def test_assign_sublineages(mock_assign_cluster_sublineages, mock_save_sublineage_results, mock_get_cluster_to_hashes):
    mock_get_cluster_to_hashes.return_value = {
        "GPSC1": ["hash1", "hash2"],
        "GPSC2": ["hash3"],
    }
    sublineage_dfs = [
        pd.DataFrame({"id": ["hash1", "hash2"], "Sublineage": [5, 10]}),
        pd.DataFrame({"id": ["hash3"], "Sublineage": [15]}),
    ]
    mock_assign_cluster_sublineages.side_effect = sublineage_dfs
    p_hash = "test_hash"
    fs = Mock(spec=PoppunkFileStore)
    db_fs = Mock(spec=DatabaseFileStore, sublineages_db_path="/sublineages")
    args = Mock()
    redis_host = "localhost"
    species = "test_species"

    assign_sublineages(p_hash, fs, db_fs, args, redis_host, species)

    mock_get_cluster_to_hashes.assert_called_once_with(redis_host)

    assert mock_assign_cluster_sublineages.call_count == 2
    mock_assign_cluster_sublineages.assert_any_call(p_hash, fs, db_fs, args, "GPSC1", ["hash1", "hash2"], species)
    mock_assign_cluster_sublineages.assert_any_call(p_hash, fs, db_fs, args, "GPSC2", ["hash3"], species)

    mock_save_sublineage_results.assert_called_once()
    call_args = mock_save_sublineage_results.call_args[0]
    assert call_args[0] == p_hash
    assert call_args[1] == fs
    pd.testing.assert_frame_equal(call_args[2], pd.concat(sublineage_dfs, ignore_index=True))


def test_assign_sublineages_no_sublineages():
    with pytest.raises(ValueError, match="Sub-lineages database path is not provided."):
        assign_sublineages(
            p_hash="test_hash",
            fs=Mock(),
            db_fs=Mock(sublineages_db_path=None),
            args=Mock(),
            redis_host="localhost",
            species="test_species",
        )


@patch("os.path.exists")
@patch("beebop.services.run_PopPUNK.sublineage.run.get_cluster_num")
@patch("beebop.services.run_PopPUNK.sublineage.run.link_sketches_hdf5")
@patch("beebop.services.run_PopPUNK.sublineage.run.PoppunkWrapper")
@patch("beebop.services.run_PopPUNK.sublineage.run.setupDBFuncs")
@patch("beebop.services.run_PopPUNK.sublineage.run.get_query_sublineage_result")
def test_assign_cluster_sublineages_success(
    mock_get_query_sublineage_result,
    mock_setupDBFuncs,
    mock_PoppunkWrapper,
    mock_link_sketches_hdf5,
    mock_get_cluster_num,
    mock_exists,
):
    p_hash = "test_hash"
    cluster = "GPSC1"
    cluster_num = "1"
    fs = Mock(spec=PoppunkFileStore)
    db_fs = Mock(spec=DatabaseFileStore)
    args = Mock()
    hashes = ["hash1", "hash2"]
    species = "test_species"
    model_path = "/model/folder"
    distances_path = "/model/folder/distances.dists"
    output_folder = "/output/folder"

    db_fs.get_sublineages_model_path.return_value = model_path
    db_fs.get_sublineages_distances_path.return_value = distances_path
    fs.output_sublineages_folder.return_value = output_folder
    mock_exists.return_value = True
    mock_get_cluster_num.return_value = cluster_num
    mock_result = pd.DataFrame({"id": hashes, "Sublineage": [5, 10]})
    mock_get_query_sublineage_result.return_value = mock_result
    wrapper = Mock()
    mock_PoppunkWrapper.return_value = wrapper

    result = assign_cluster_sublineages(
        p_hash,
        fs,
        db_fs,
        args,
        cluster,
        hashes,
        species,
    )

    assert result.equals(mock_result)

    db_fs.get_sublineages_model_path.assert_called_once_with(cluster)
    db_fs.get_sublineages_distances_path.assert_called_once_with(cluster)
    fs.output_sublineages_folder.assert_called_once_with(p_hash, cluster_num)

    mock_link_sketches_hdf5.assert_called_once_with(fs, p_hash, cluster_num)
    mock_setupDBFuncs.assert_called_once_with(args=args.assign)
    wrapper.assign_sublineages.assert_called_once_with(
        mock_setupDBFuncs.return_value,
        qNames=hashes,
        output=output_folder,
        model_folder=model_path,
        distances=distances_path,
    )
    mock_get_query_sublineage_result.assert_called_once_with(fs, p_hash, cluster_num)


@patch("os.path.exists")
def test_assign_cluster_sublineages_no_model_folder(mock_exists):
    mock_exists.return_value = False

    result = assign_cluster_sublineages(
        "test_hash",
        Mock(),
        Mock(),
        Mock(),
        "GPSC1",
        ["hash1", "hash2"],
        "test_species",
    )

    assert result.empty
