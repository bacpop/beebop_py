import os
import time
from unittest.mock import Mock, call, patch

from rq.job import Job

from beebop.services.cluster_service import get_cluster_num
from beebop.services.run_PopPUNK.visualise.run import (
    queue_visualisation_jobs,
    visualise,
    visualise_per_cluster,
)
from tests import setup

name_mapping = {
    "02ff334f17f17d775b9ecd69046ed296": "name1.fa",
    "9c00583e2f24fed5e3c6baa87a4bfa4c": "name2.fa",
}
external_to_poppunk_clusters = {
    "GPSC16": {"9"},
    "GPSC29": {"41"},
    "GPSC8": {"10", "11"},
}


def test_visualise(mocker):
    mock_job = Mock(spec=Job)
    mock_job.dependency.result = setup.expected_assign_result
    mocker.patch(
        "beebop.services.run_PopPUNK.visualise.run.get_current_job",
        return_value=mock_job,
    )
    p_hash = "unit_test_visualise"

    setup.do_assign_clusters(p_hash)

    visualise(
        p_hash,
        setup.fs,
        setup.ref_db_fs,
        setup.args,
        name_mapping,
        setup.species,
        "localhost",
        {},
    )

    time.sleep(60)  # wait for jobs to finish

    for cluster in external_to_poppunk_clusters.keys():
        cluster_num = get_cluster_num(cluster)

        # microreact
        assert os.path.exists(
            setup.fs.output_visualisations(p_hash, cluster_num) + f"/visualise_{cluster_num}_core_NJ.nwk"
        )
        assert os.path.exists(
            setup.fs.output_visualisations(p_hash, cluster_num) + f"/visualise_{cluster_num}_microreact_clusters.csv"
        )
        assert os.path.exists(
            setup.fs.output_visualisations(p_hash, cluster_num) + f"/visualise_{cluster_num}.microreact"
        )
        # network
        assert os.path.exists(
            setup.fs.output_visualisations(p_hash, cluster_num)
            + f"/visualise_{cluster_num}_component_{cluster_num}.graphml"
        )
        assert os.path.exists(
            setup.fs.output_visualisations(p_hash, cluster_num)
            + f"/pruned_visualise_{cluster_num}"
            + f"_component_{cluster_num}.graphml"
        )
        assert os.path.exists(
            setup.fs.output_visualisations(p_hash, cluster_num) + f"/visualise_{cluster_num}_cytoscape.csv"
        )


@patch("beebop.services.run_PopPUNK.visualise.run.replace_filehashes")
@patch("beebop.services.run_PopPUNK.visualise.run.create_subgraph")
@patch("beebop.services.run_PopPUNK.visualise.run.get_internal_cluster")
def test_visualise_per_cluster(mock_get_internal_cluster, mock_create_subgraph, mock_replace_filehashes):
    p_hash = "unit_test_visualise_internal"
    cluster = "GPSC16"
    wrapper = Mock()
    internal_cluster = "9"
    mock_get_internal_cluster.return_value = internal_cluster

    visualise_per_cluster(
        cluster,
        p_hash,
        setup.fs,
        wrapper,
        name_mapping,
        external_to_poppunk_clusters,
    )

    wrapper.create_visualisations.assert_called_with("16", setup.fs.include_file(p_hash, internal_cluster))
    mock_replace_filehashes.assert_called_with(setup.fs.output_visualisations(p_hash, 16), name_mapping)
    mock_create_subgraph.assert_called_with(setup.fs.output_visualisations(p_hash, 16), name_mapping, "16")
    mock_get_internal_cluster.assert_called_with(external_to_poppunk_clusters, cluster, p_hash, setup.fs)


@patch("beebop.services.run_PopPUNK.visualise.run.replace_filehashes")
@patch("shutil.rmtree")
@patch("beebop.services.run_PopPUNK.visualise.run.create_subgraph")
@patch("beebop.services.run_PopPUNK.visualise.run.get_internal_cluster")
def test_visualise_per_cluster_last_cluster(
    mock_get_internal_cluster, mock_create_subgraph, mock_rmtree, mock_replace_filehashes
):
    p_hash = "unit_test_visualise_internal"
    cluster = "GPSC16"
    wrapper = Mock()
    internal_cluster = "9"
    mock_get_internal_cluster.return_value = internal_cluster
    output_folder = setup.fs.output_visualisations(p_hash, 16)

    visualise_per_cluster(
        cluster,
        p_hash,
        setup.fs,
        wrapper,
        name_mapping,
        external_to_poppunk_clusters,
        True,  # is_last_cluster_to_process
    )

    wrapper.create_visualisations.assert_called_with("16", setup.fs.include_file(p_hash, internal_cluster))
    mock_create_subgraph.assert_called_with(output_folder, name_mapping, "16")
    mock_rmtree.assert_called_with(setup.fs.tmp(p_hash))
    mock_replace_filehashes.assert_called_with(output_folder, name_mapping)


def test_queue_visualise_jobs(mocker):
    p_hash = "unit_test_visualise_internal"
    wrapper = Mock()
    redis = Mock()
    mocker.patch.object(redis, "hset")
    mockQueue = Mock()
    mockJob = Mock()
    mockJob.id.return_value = "1234"
    mockQueue.enqueue.return_value = mockJob
    mocker.patch(
        "beebop.services.run_PopPUNK.visualise.run.Queue",
        return_value=mockQueue,
    )
    mocker.patch("beebop.services.run_PopPUNK.visualise.run.Dependency")
    expected_hset_calls = [
        call(f"beebop:hash:job:visualise:{p_hash}", item["cluster"], mockJob.id)
        for item in setup.expected_assign_result.values()
    ]
    expected_enqueue_calls = [
        call(
            visualise_per_cluster,
            args=(
                item["cluster"],
                p_hash,
                setup.fs,
                wrapper,
                name_mapping,
                external_to_poppunk_clusters,
                mocker.ANY,
            ),
            job_timeout=60,
            depends_on=mocker.ANY,
        )
        for i, item in enumerate(setup.expected_assign_result.values())
    ]

    queue_visualisation_jobs(
        setup.expected_assign_result,
        p_hash,
        setup.fs,
        wrapper,
        name_mapping,
        external_to_poppunk_clusters,
        redis,
        queue_kwargs={"job_timeout": 60},
    )

    redis.hset.assert_has_calls(expected_hset_calls, any_order=True)
    mockQueue.enqueue.assert_has_calls(expected_enqueue_calls, any_order=True)
