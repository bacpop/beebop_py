from types import SimpleNamespace
import json
import xml.etree.ElementTree as ET
import os
import re
import fileinput
import glob
import pandas as pd
from beebop.filestore import PoppunkFileStore

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


def add_query_ref_status(
    fs: PoppunkFileStore, p_hash: str, filename_dict: dict
) -> None:
    """
    [The standard poppunk visualisation output for the cytoscape network graph
    (.graphml file) does not include information on whether a sample has been
    added by the user (we call these query samples) or is from the database
    (called reference samples). To highlight the query samples in the network,
    this information must be added to the .graphml file.
    This is done by adding a new <data> element to the nodes, with the key
    "ref_query" and the value being coded as either 'query' or 'ref'.]

    :param fs: [filestore to locate output files]
    :param p_hash: [project hash to find right project folder]
    :param filename_dict: [dict that maps filehashes(keys) toclear
        corresponding filenames (values) of all query samples. We only need
        the filenames here.]
    """
    # list of query filenames
    query_names = list(filename_dict.values())
    # list of all component graph filenames
    file_list = glob.glob(
        fs.output_network(p_hash) + "/network_component_*.graphml"
    )
    for path in file_list:
        xml_tree = ET.parse(path)
        graph = xml_tree.getroot()
        nodes = graph.findall(".//{http://graphml.graphdrawing.org/xmlns}node")
        for node in nodes:
            name = node.find("./").text
            child = ET.Element("data")
            child.set("key", "ref_query")
            child.text = "query" if name in query_names else "ref"
            node.append(child)
        ET.indent(xml_tree, space="  ", level=0)
        with open(path, "wb") as f:
            xml_tree.write(f, encoding="utf-8")


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
) -> tuple[dict[str, str], list[str]]:
    """
    [Finds sample hashes defined by hashes_list in the given external clusters
    file and returns a dictionary of sample hash to external cluster name. If
    there are multiple external clusters listed for a sample, the lowest
    cluster number is returned. If any samples are found but do  not
    have a cluster assigned, they are returned separately.]

    :param previous_query_clustering_file: [filename
    of the project's external clusters file]
    :param hashes_list: [list of sample hashes to find samples for]
    :param external_clusters_prefix: prefix for external cluster name
    :return tuple: [dictionary of sample hash to external cluster name,
        list of sample hashes that were not found]
    """
    df, samples_mask = get_df_sample_mask(
        previous_query_clustering_file, hashes_list
    )
    filtered_df = df[samples_mask]

    # Split into found and not found based on NaN values
    found_mask = filtered_df["Cluster"].notna()
    not_found_hashes = filtered_df[~found_mask]["sample"].tolist()

    # Process only rows with valid clusters
    valid_clusters = filtered_df[found_mask]
    cluster_numbers = valid_clusters["Cluster"].apply(get_lowest_cluster)

    hash_to_cluster_mapping = (
        external_clusters_prefix + cluster_numbers.astype(str)
    )
    hash_to_cluster_mapping.index = valid_clusters["sample"]

    return hash_to_cluster_mapping.to_dict(), not_found_hashes


def update_external_clusters_csv(
    previous_query_clustering_file: str,
    not_found_q_names: list,
    external_clusters_not_found: dict,
) -> None:
    """
    [Update the external clusters CSV file with the clusters of the samples
    that were not found in the external clusters file.]

    :param previous_query_clustering_file:Path to CSV file
    containing sample data]
    :param not_found_q_names:[List of sample names
    that were not
    found in the external clusters file]
    :param external_clusters_not_found:[Dictionary mapping
    sample names to external cluster names]
    """
    df, samples_mask = get_df_sample_mask(
        previous_query_clustering_file, not_found_q_names
    )
    df.loc[samples_mask, "Cluster"] = [
        get_cluster_num(external_clusters_not_found[sample_id])
        for sample_id in not_found_q_names
    ]
    df.to_csv(previous_query_clustering_file, index=False)


def get_df_sample_mask(
    previous_query_clustering_file: str, samples: str
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Read a CSV file and create a boolean mask for matching sample names.

    :param previous_query_clustering_file:[Path to CSV file
        containing sample data
        samples: List of sample names to match]
    :param samples: [List of sample names to match]
    :return tuple:[DataFrame containing sample data,
        boolean mask for matching samples]
    """
    df = pd.read_csv(previous_query_clustering_file, dtype=str)
    return df, df["sample"].isin(samples)
