from unittest.mock import Mock, patch
import graph_tool.all as gt
import pytest
import os
from beebop.services.run_PopPUNK.visualise.visualise_utils import (
    add_query_ref_to_graph,
    build_subgraph,
    create_combined_include_file,
    add_neighbor_nodes,
    create_subgraph,
    get_component_filepath,
    get_internal_cluster,
    replace_filehashes,
)


def test_replace_filehashes(tmp_path):

    folder = tmp_path / "replace_filehashes"
    folder.mkdir()

    # Create test files with hash content
    test_data = {
        "file1": "filehash1",
        "file2": "filehash2",
        "file3": "filehash3",
    }
    for filename, content in test_data.items():
        (folder / filename).write_text(content)

    filename_dict = {
        "filehash1": "filename1",
        "filehash2": "filename2",
        "filehash3": "filename3",
    }

    replace_filehashes(str(folder), filename_dict)

    # Verify results
    for filename, original_hash in test_data.items():
        expected_name = filename_dict[original_hash]
        content = (folder / filename).read_text()
        assert expected_name in content
        assert original_hash not in content


@patch("beebop.services.run_PopPUNK.visualise.visualise_utils.build_subgraph")
@patch(
    "beebop.services.run_PopPUNK.visualise.visualise_utils.add_query_ref_to_graph"
)
@patch(
    "beebop.services.run_PopPUNK.visualise.visualise_utils.get_component_filepath"
)
def test_create_subgraph(
    mock_get_component_filepath,
    mock_add_query_ref_to_graph,
    mock_build_subgraph,
):
    mock_get_component_filepath.return_value = "network_component_1.graphml"

    mock_subgraph = Mock()
    mock_build_subgraph.return_value = mock_subgraph
    filename_dict = {
        "filehash1": "filename1",
        "filehash2": "filename2",
    }
    query_names = list(filename_dict.values())

    create_subgraph("network_folder", filename_dict, "1")

    mock_get_component_filepath.assert_called_once_with("network_folder", "1")
    mock_build_subgraph.assert_called_once_with(
        "network_component_1.graphml", query_names
    )
    mock_add_query_ref_to_graph.assert_called_once_with(
        mock_subgraph, query_names
    )
    mock_subgraph.save.assert_called_once_with(
        "network_component_1.graphml", fmt="graphml"
    )


def test_component_filepath(tmp_path):
    """
    Test the get_component_filepath function to ensure it returns the correct
    file path for a given cluster number.
    """
    cluster_num = "123"
    visualisations_folder = tmp_path / "visualisations"
    visualisations_folder.mkdir()

    # Create a mock component file
    component_file = (
        visualisations_folder
        / f"visualise_{cluster_num}_component_{cluster_num}.graphml"
    )
    component_file.touch()

    result = get_component_filepath(str(visualisations_folder), cluster_num)

    assert result == str(component_file)


def test_component_filepath_no_file(tmp_path):
    """
    Test the get_component_filepath function to ensure it raises a FileNotFoundError
    when no component files are found for the given cluster number.
    """
    cluster_num = "123"
    visualisations_folder = tmp_path / "visualisations"
    visualisations_folder.mkdir()

    with pytest.raises(
        FileNotFoundError,
        match=f"No component files found for cluster {cluster_num}",
    ):
        get_component_filepath(str(visualisations_folder), cluster_num)


@patch("beebop.services.run_PopPUNK.visualise.visualise_utils.gt.load_graph")
def test_build_subgraph(mock_load_graph):
    graph = gt.complete_graph(50)  # 50 nodes fully conected
    query_names = ["sample1", "sample2", "sample3"]
    id_vertex_properties = graph.new_vertex_property("string")
    id_vertex_properties[45] = "sample2"
    graph.vp.id = id_vertex_properties
    mock_load_graph.return_value = graph

    subgraph = build_subgraph("network_component_1.graphml", query_names)

    assert subgraph.num_vertices() == 25  # max number


@patch("beebop.services.run_PopPUNK.visualise.visualise_utils.gt.load_graph")
@patch(
    "beebop.services.run_PopPUNK.visualise.visualise_utils.add_neighbor_nodes"
)
def test_build_subgraph_no_prune(mock_add_neighbor_nodes, mock_load_graph):
    graph = gt.complete_graph(10)  # 50 nodes fully conected
    query_names = ["sample1", "sample2", "sample3"]
    mock_load_graph.return_value = graph

    subgraph = build_subgraph("network_component_1.graphml", query_names)

    assert subgraph.num_vertices() == 10
    mock_add_neighbor_nodes.assert_not_called()


def test_add_neighbor_nodes_max_more_than_available():
    graph_nodes = {1}
    neighbours = {2, 3, 4, 5}
    max_nodes = 10

    add_neighbor_nodes(graph_nodes, neighbours, max_nodes)

    assert graph_nodes == {1, 2, 3, 4, 5}


def test_add_neighbor_nodes_max_less_than_available():
    graph_nodes = {1}
    neighbours = {2, 3, 4, 5, 6, 7, 8, 9, 10}
    max_nodes = 3

    add_neighbor_nodes(graph_nodes, neighbours, max_nodes)

    assert len(graph_nodes) == 4


def test_add_query_ref_to_graph():
    graph = gt.complete_graph(10)  # 10 nodes fully conected
    query_names = ["sample1", "sample2", "sample3"]
    id_vertex_properties = graph.new_vertex_property("string")
    id_vertex_properties[0] = "sample2"
    graph.vp.id = id_vertex_properties

    add_query_ref_to_graph(graph, query_names)

    assert graph.vp["ref_query"][0] == "query"
    for i in range(1, 10):
        assert graph.vp["ref_query"][i] == "ref"


def test_get_internal_cluster_no_external():
    cluster = get_internal_cluster(None, "GPSC123", "hash", Mock())
    assert cluster == "GPSC123"


def test_get_internal_cluster_single_internal():
    assign_cluster = "GPSC123"
    external_to_poppunk_clusters = {assign_cluster: {"1"}}

    cluster = get_internal_cluster(
        external_to_poppunk_clusters,
        assign_cluster,
        "hash",
        Mock(),
    )
    assert cluster == "1"


@patch(
    "beebop.services.run_PopPUNK.visualise.visualise_utils.create_combined_include_file"
)
def test_get_internal_cluster_multiple_internal(
    mock_create_combined_include_file,
):
    mock_create_combined_include_file.return_value = "1_2"
    assign_cluster = "GPSC123"
    external_to_poppunk_clusters = {assign_cluster: {"1", "2"}}

    cluster = get_internal_cluster(
        external_to_poppunk_clusters,
        assign_cluster,
        "hash",
        Mock(),
    )
    assert cluster == "1_2"


def test_create_combined_include_file(tmp_path):
    def include_path(cluster):
        return tmp_path / f"include_{cluster}.txt"

    internal_cluster = {"1", "2"}
    # need to check both combinations as "_".join() does not guarantee order
    combined_cluster_combinations = ["1_2", "2_1"]
    for cluster in internal_cluster:
        (include_path(cluster)).write_text(f"data for {cluster}")

    fs = Mock()
    fs.include_file.side_effect = lambda _p_hash, cluster: str(
        include_path(cluster)
    )

    cluster = create_combined_include_file(fs, "hash", internal_cluster)

    assert cluster in combined_cluster_combinations
    for cluster in combined_cluster_combinations:
        combined_include_file_path = str(include_path(cluster))
        if os.path.exists(combined_include_file_path):
            with open(combined_include_file_path, "r", errors="ignore") as f:
                content = f.read()
                assert (
                    content
                    == f"data for {cluster.split('_')[0]}\ndata for {cluster.split('_')[1]}\n"
                )
