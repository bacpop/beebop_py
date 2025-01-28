from types import SimpleNamespace
import json
import xml.etree.ElementTree as ET
import os
import re
import fileinput
import glob
import pandas as pd
from beebop.filestore import PoppunkFileStore
from networkx import read_graphml, write_graphml, Graph
import random

ET.register_namespace("", "http://graphml.graphdrawing.org/xmlns")
ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")


def get_args() -> SimpleNamespace:
    """
    [Read in fixed arguments to poppunk that are always set, or used as
    defaults. This is needed because of the large number of arguments that
    poppunk needs]

    :return dict: [arguments loaded from json]
    """
    with open("./beebop/resources/args.json") as a:
        args_json = a.read()
    return json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))


NODE_SCHEMA = ".//{http://graphml.graphdrawing.org/xmlns}node/"


def get_cluster_num(cluster: str) -> str:
    """
    [Extract the numeric part from a cluster label, regardless of the prefix.]

    :param cluster: [cluster from assign result.
    Can be prefixed with external_cluster_prefix]
    :return str: [numeric part of the cluster]
    """
    match = re.search(r"\d+", str(cluster))
    return match.group(0) if match else str(cluster)


def cluster_nums_from_assign(assign_result: dict) -> list:
    """
    [Get all cluster numbers from a cluster assign result.]

    :param assign_result: [cluster assign result, as returned through the API]
    :return: [list of all external cluster numbers in the result]
    """
    result = set()
    for item in assign_result.values():
        result.add(get_cluster_num(item["cluster"]))
    return list(result)


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


def create_subgraphs(network_folder: str, filename_dict: dict) -> None:
    """
    [Create subgraphs for the network visualisation. These are what
    will be sent back to the user to see.
    The subgraphs are created
    by selecting a maximum number nodes, prioritizing query nodes and adding
    neighbor nodes until the maximum number of nodes is reached. The query
    nodes are highlighted in the network graph by adding a ref or query status
    to the .graphml files.]

    :param network_folder: [path to the network folder]
    :param filename_dict: [dict that maps filehashes(keys) to
        corresponding filenames (values) of all query samples. We only need
        the filenames here.]
    """
    query_names = list(filename_dict.values())

    for path in get_component_filenames(network_folder):
        sub_graph = build_subgraph(path, query_names)

        add_query_ref_to_graph(sub_graph, query_names)

        write_graphml(
            sub_graph,
            path.replace("network_component", "pruned_network_component"),
        )


def get_component_filenames(network_folder: str) -> list[str]:
    """
    [Get all network component filenames in the network folder.]

    :param network_folder: [path to the network folder]
    :return list: [list of all network component filenames]
    """
    return glob.glob(network_folder + "/network_component_*.graphml")


def build_subgraph(path: str, query_names: list) -> Graph:
    """
    [Build a subgraph from a network graph, prioritizing query nodes and
    adding neighbor nodes until the maximum number of nodes is reached.]

    :param path: [path to the network graph]
    :param query_names: [list of query sample names]
    :return nx.Graph: [subgraph]
    """
    MAX_NODES = 30  # arbitrary number based on performance
    graph = read_graphml(path)
    if MAX_NODES >= len(graph.nodes()):
        return graph
    # get query nodes
    query_nodes = {
        node for (node, id) in graph.nodes(data="id") if id in query_names
    }

    # get neighbor nodes of query nodes
    neighbor_nodes = set()
    for node in query_nodes:
        neighbor_nodes.update(graph.neighbors(node))

    # remove query nodes from neighbor nodes
    neighbor_nodes = neighbor_nodes - query_nodes

    # create final set of nodes, prioritizing query nodes
    sub_graph_nodes = set()
    sub_graph_nodes.update(query_nodes)

    # add neighbor nodes until we reach the maximum number of nodes
    remaining_capacity = MAX_NODES - len(sub_graph_nodes)
    if remaining_capacity > 0:
        add_neighbor_nodes(
            sub_graph_nodes, neighbor_nodes, remaining_capacity
        )

    return graph.subgraph(sub_graph_nodes)


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


def add_query_ref_to_graph(graph: Graph, query_names: list) -> None:
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
    for node, id in graph.nodes(data="id"):
        if id in query_names:
            graph.nodes[node]["ref_query"] = "query"
        else:
            graph.nodes[node]["ref_query"] = "ref"


def get_lowest_cluster(clusters_str: str) -> int:
    """
    [Get numerically lowest cluster number from semicolon-separated clusters
    string.]

    :param clusters_str: [string of all clusters for a sample, separated by
        semicolons]
    :return int: [lowest cluster number from the string]
    """
    clusters = map(int, clusters_str.split(";"))
    return min(clusters)


def get_external_clusters_from_file(
    previous_query_clustering_file: str,
    hashes_list: list,
    external_clusters_prefix: str,
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """
    [Finds sample hashes defined by hashes_list in
    the given external clusters
    file and returns a dictionary of sample hash to
    external cluster name & raw external cluster number. If
    there is a merged clusters for a sample, the lowest
    cluster number is used for cluster. If any samples are found
    but do  not
    have a cluster assigned, they are returned separately.]

    :param previous_query_clustering_file: [filename
    of the project's external clusters file]
    :param hashes_list: [list of sample hashes to find samples for]
    :param external_clusters_prefix: prefix for external cluster name
    :return tuple: [dictionary of sample hash to
    external cluster name & raw external cluster number,
    list of sample hashes not found in the external]
    """
    filtered_df = get_df_filtered_by_samples(
        previous_query_clustering_file, hashes_list
    )

    # Split into found and not found based on NaN values
    found_mask = filtered_df["Cluster"].notna()
    not_found_hashes = filtered_df[~found_mask]["sample"].tolist()

    # Process only rows with valid clusters
    valid_clusters = filtered_df[found_mask]
    hash_cluster_info = {
        sample: {
            "cluster":
                f"{external_clusters_prefix}{get_lowest_cluster(cluster)}",
            "raw_cluster_num": cluster,
        }
        for sample, cluster in zip(
            valid_clusters["sample"], valid_clusters["Cluster"]
        )
    }

    return hash_cluster_info, not_found_hashes


def get_external_cluster_nums(
    previous_query_clustering_file: str, hashes_list: list
) -> dict[str, str]:
    """
    [Get external cluster numbers for samples in the external clusters file.]

    :param previous_query_clustering_file: [Path to CSV file
    containing sample data]
    :param hashes_list: [List of sample hashes to find samples for]
    :return dict: [Dictionary mapping sample names to external cluster names]
    """
    filtered_df = get_df_filtered_by_samples(
        previous_query_clustering_file, hashes_list
    )

    sample_cluster_num_mapping = filtered_df["Cluster"].astype(str)
    sample_cluster_num_mapping.index = filtered_df["sample"]

    return sample_cluster_num_mapping.to_dict()


def get_df_filtered_by_samples(previous_query_clustering_file: str,
                               hashes_list: list) -> pd.DataFrame:
    """
    [Filter a DataFrame by sample names.]

    :param previous_query_clustering_file: [Path to CSV file
    containing sample data]
    :param hashes_list: [List of sample hashes to find samples for]
    :return pd.DataFrame: [DataFrame containing sample data]
    """
    df, samples_mask = get_df_sample_mask(
        previous_query_clustering_file, hashes_list
    )
    return df[samples_mask]


def update_external_clusters_csv(
    dest_query_clustering_file: str,
    source_query_clustering_file: str,
    q_names: list,
) -> None:
    """
    [Update the external clusters CSV file with the clusters of the samples
    that were not found in the external clusters file.]

    :param dest_query_clustering_file: [Path to CSV file
    containing sample data to copy into]
    :param source_query_clustering_file: [Path to CSV file
    containing sample data to copy from]
    :param q_names: [List of sample names to match]
    """
    df, samples_mask = get_df_sample_mask(
        dest_query_clustering_file, q_names
    )
    sample_cluster_num_mapping = get_external_cluster_nums(
        source_query_clustering_file, q_names
    )

    df.loc[samples_mask, "Cluster"] = [
        sample_cluster_num_mapping[sample_id] for sample_id in q_names
    ]
    df.to_csv(dest_query_clustering_file, index=False)


def get_df_sample_mask(
    previous_query_clustering_file: str, samples: str
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Read a CSV file and create a boolean mask for matching sample names.

    :param previous_query_clustering_file: [Path to CSV file
        containing sample data
        samples: List of sample names to match]
    :param samples: [List of sample names to match]
    :return tuple: [DataFrame containing sample data,
        boolean mask for matching samples]
    """
    df = pd.read_csv(previous_query_clustering_file, dtype=str)
    return df, df["sample"].isin(samples)
