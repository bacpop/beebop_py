import fileinput
import glob
import os
import random
from pathlib import PurePath
from typing import Optional

import graph_tool.all as gt

from beebop.config import PoppunkFileStore


def replace_filehashes(folder: str, filename_dict: dict) -> None:
    """
    [Since the analyses run with filehashes rather than filenames (because we
    store the json sketches by filehash rather than filename to avoid saving
    the same sketch multiple times with different filenames) the results are
    also reported with file hashes rather than filenames. To report results
    back to the user using their original filenames, the hashes get replaced.]

    :param folder: [path to folder in which the replacement should be
        performed. Will be a microreact or network folder.]
    :param filename_dict: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    """
    file_list = []
    for root, _dirs, files in os.walk(folder):
        for file in files:
            if not file.endswith(".h5"):
                file_list.append(os.path.join(root, file))
    with fileinput.input(files=(file_list), inplace=True) as input:
        for line in input:
            line = line.rstrip()
            if not line:
                continue
            for f_key, f_value in filename_dict.items():
                if f_key in line:
                    line = line.replace(f_key, f_value)
            print(line)


def create_subgraph(
    visualisations_folder: str, filename_dict: dict, cluster_num: str
) -> None:
    """
    [Create subgraphs for the network visualisation. These are what
    will be sent back to the user to see.
    The subgraphs are created
    by selecting a maximum number nodes, prioritizing query nodes and adding
    neighbor nodes until the maximum number of nodes is reached. The query
    nodes are highlighted in the network graph by adding a ref or query status
    to the .graphml files.]

    :param visualisations_folder: [path to the visualisations folder]
    :param filename_dict: [dict that maps filehashes(keys) to
        corresponding filenames (values) of all query samples. We only need
        the filenames here.]
    :param cluster_num: [cluster number to create subgraph for]
    """
    query_names = list(filename_dict.values())
    component_path = get_component_filepath(visualisations_folder, cluster_num)

    sub_graph = build_subgraph(component_path, query_names)
    add_query_ref_to_graph(sub_graph, query_names)

    sub_graph.save(
        component_path.replace(
            f"visualise_{cluster_num}_component",
            f"pruned_visualise_{cluster_num}_component",
        ),
        fmt="graphml",
    )


def get_component_filepath(
    visualisations_folder: str, cluster_num: str
) -> str:
    """
    Get the filename of the network component
    for a given assigned cluster number.

    :param visualisations_folder: Path to the
        folder containing visualisation files.
    :param cluster_num: Cluster number to find the component file for.
    :return: Path to the network component file.
    :raises FileNotFoundError: If no component files are
        found for the given cluster number.
    """
    component_files = glob.glob(
        str(
            PurePath(
                visualisations_folder,
                f"visualise_{cluster_num}_component_*.graphml",
            )
        )
    )
    if not component_files:
        raise FileNotFoundError(
            f"No component files found for cluster {cluster_num}"
        )
    return component_files[0]


def build_subgraph(path: str, query_names: list) -> gt.Graph:
    """
    [Build a subgraph from a network graph, prioritizing query nodes and
    adding neighbor nodes until the maximum number of nodes is reached.]

    :param path: [path to the network graph]
    :param query_names: [list of query sample names]
    :return gt.Graph: [subgraph]
    """
    MAX_NODES = 25  # arbitrary number based on performance & visibility
    graph = gt.load_graph(path, fmt="graphml")
    if MAX_NODES >= graph.num_vertices():
        return graph
    # get query nodes
    query_nodes = {
        v for v in graph.get_vertices() if graph.vp["id"][v] in query_names
    }

    neighbor_nodes = set()
    for node in query_nodes:
        neighbor_nodes.update(graph.get_all_neighbors(node))

    neighbor_nodes = neighbor_nodes - query_nodes

    # create final set of nodes, prioritizing query nodes
    sub_graph_nodes = set()
    sub_graph_nodes.update(query_nodes)
    remaining_capacity = MAX_NODES - len(sub_graph_nodes)
    if remaining_capacity > 0:
        add_neighbor_nodes(sub_graph_nodes, neighbor_nodes, remaining_capacity)

    sub_graph = gt.GraphView(graph, vfilt=lambda v: v in sub_graph_nodes)
    sub_graph.purge_vertices()
    return sub_graph


def add_neighbor_nodes(
    graph_nodes: set, neighbor_nodes: set, max_nodes_to_add: int
) -> None:
    """
    [Add neighbor nodes to the set of nodes until the
    maximum number of nodes is reached.
    Randomly select nodes to add if there are more neighbor nodes than
    can be added.]

    :param graph_nodes: [set of nodes to add to]
    :param neighbor_nodes: [set of neighbor nodes to add]
    :param max_nodes_to_add: [maximum number of nodes to add]
    """
    if max_nodes_to_add >= len(neighbor_nodes):
        graph_nodes.update(neighbor_nodes)
    else:
        graph_nodes.update(
            random.sample(list(neighbor_nodes), max_nodes_to_add)
        )


def add_query_ref_to_graph(graph: gt.Graph, query_names: list) -> None:
    """
    [The standard poppunk visualisation output for the cytoscape network graph
    (.graphml file) does not include information on whether a sample has been
    added by the user (we call these query samples) or is from the database
    (called reference samples). To highlight the query samples in the network,
    this information must be added to the .graphml file.
    This is done by adding a new <data> element to the nodes, with the key
    "ref_query" and the value being coded as either 'query' or 'ref'.]

    :param graph: [networkx graph object]
    :param query_names: [list of query sample names]
    """
    vertex_property_ref_query = graph.new_vertex_property(
        "string",
        vals=[
            "query" if graph.vp["id"][v] in query_names else "ref"
            for v in graph.get_vertices()
        ],
    )
    graph.vp.ref_query = vertex_property_ref_query


def get_internal_cluster(
    external_to_poppunk_clusters: Optional[dict[str, set[str]]],
    assign_cluster: str,
    p_hash: str,
    fs: PoppunkFileStore,
) -> str:
    """
    [Determines the internal cluster name based on
    the external cluster mapping.
    In the edge case where the external cluster maps
    to multiple internal clusters,
    it creates a combined include file.]

    :param external_to_poppunk_clusters: [dict mapping external clusters
        to internal clusters]
    :param assign_cluster: [the external cluster number]
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :return: [internal cluster name or combined include file name]
    """
    if not external_to_poppunk_clusters:
        return assign_cluster

    internal_clusters = external_to_poppunk_clusters[assign_cluster]
    if len(internal_clusters) == 1:
        return internal_clusters.pop()

    return create_combined_include_file(fs, p_hash, internal_clusters)


def create_combined_include_file(
    fs: PoppunkFileStore,
    p_hash: str,
    internal_clusters: set[str],
) -> str:
    """
    [Creates a combined include file for multiple internal clusters.
    The combined file contains the contents of
    all specified internal clusters.]

    :param fs: [PoppunkFileStore with paths to input data]
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param internal_clusters: [list of internal cluster names to combine]
    :return: [combined internal cluster]
    """
    combined_internal_cluster = "_".join(internal_clusters)
    with open(
        fs.include_file(p_hash, combined_internal_cluster), "w"
    ) as combined_include_file:
        for internal_cluster in internal_clusters:
            with open(
                fs.include_file(p_hash, internal_cluster), "r"
            ) as include_file:
                combined_include_file.write(include_file.read() + "\n")

    return combined_internal_cluster
