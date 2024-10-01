from types import SimpleNamespace
import json
import csv
import xml.etree.ElementTree as ET
import fnmatch
import os
import re
import pickle
import fileinput
import glob

from beebop.filestore import PoppunkFileStore

ET.register_namespace('', "http://graphml.graphdrawing.org/xmlns")
ET.register_namespace('xsi', "http://www.w3.org/2001/XMLSchema-instance")


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


def generate_mapping(p_hash: str,
                     cluster_nums_to_map: list,
                     fs: PoppunkFileStore,
                     is_external_clusters: bool) -> dict:
    """
    [PopPUNKs network visualisation generates one overall .graphml file
    covering all clusters/ components. Furthermore, it generates one .graphml
    file per component, where the component numbers are arbitrary and do not
    match poppunk cluster numbers.
    To find the right component file by cluster number, we need to generate
    a mapping to be able to return the right component number based on cluster
    number. This function will generate that mapping by looking up the first
    filename from each component file in the csv file that holds all filenames
    and their corresponding clusters.]

    :param p_hash: [project hash]
    :param cluster_nums_to_map: [clusters of interest for this project -
        skip all other clusters]
    :param fs: [PoppunkFileStore with paths to input data]
    :return dict: [dict that maps clusters to components]
    """
    samples_to_clusters = {}
    with open(fs.network_output_csv(p_hash)) as f:
        next(f)  # Skip the header
        reader = csv.reader(f, skipinitialspace=True)
        for row in reader:
            sample_id, sample_clusters = row
            cluster = str(get_lowest_cluster(sample_clusters)) if is_external_clusters else str(sample_clusters)
            if cluster in cluster_nums_to_map:
                samples_to_clusters[sample_id] = cluster

    # list of all component graph filenames
    file_list = []
    for file in os.listdir(fs.output_network(p_hash)):
        if fnmatch.fnmatch(file, 'network_component_*.graphml'):
            file_list.append(file)

    # generate dict that maps cluster number to component number
    cluster_component_dict = match_clusters_to_components(p_hash,
                                                          fs,
                                                          file_list,
                                                          samples_to_clusters)

    # save as pickle
    with open(fs.network_mapping(p_hash), 'wb') as mapping:
        pickle.dump(cluster_component_dict, mapping)
    return cluster_component_dict


def match_clusters_to_components(p_hash: str,
                                 fs: PoppunkFileStore,
                                 file_list: list,
                                 samples_to_clusters: dict) -> dict:
    """
    [Generate a dictionary mapping clusters to network components based on
    matching their shared samples]
    :param p_hash: [project hash]
    :param fs: [project filestore]
    :param file_list: [list of network component files]
    :param samples_to_clusters: [dict of sample ids to cluster labels]
    :return dict: [dict of cluster labels to network component numbers]
    """
    cluster_component_dict = {}
    # Match each cluster with the component with which it shares the most
    # samples - keep a tally on how many matches the current winner has
    # TODO: Really, PopPUNK should only return one component per sample,
    # which should match the corresponding cluster - this appears to be
    # an issue with PopPUNK. Simplify this logic when this is fixed
    # in PopPUNK.
    cluster_sample_counts = {}
    for component_filename in file_list:
        component_number = re.findall(R'\d+', component_filename)[0]
        component_xml = ET.parse(fs.network_output_component(
            p_hash,
            component_number)).getroot()
        sample_nodes = component_xml.findall(NODE_SCHEMA)
        component_cluster_counts = {}
        # for every sample in the component, increment its cluster's
        # count in component_cluster_counts
        for sample_node in sample_nodes:
            sample_id = sample_node.text
            if sample_id in samples_to_clusters.keys():
                cluster = samples_to_clusters[sample_id]
                if cluster not in component_cluster_counts.keys():
                    component_cluster_counts[cluster] = 0
                component_cluster_counts[cluster] += 1

        # For every cluster we have samples for in this component
        # update the cluster component mapping, if this component
        # has more matching samples than the current value (or
        # if this is the first matching component)
        for cluster, count in component_cluster_counts.items():
            if cluster not in cluster_sample_counts.keys() \
                    or count > cluster_sample_counts[cluster]:
                cluster_component_dict[cluster] = component_number
                cluster_sample_counts[cluster] = count

    return cluster_component_dict

# TODO: rename? because can just be cluster num
def cluster_num_from_label(cluster_label: str) -> str:
    """
    [Extract the numeric part from a cluster label, regardless of the prefix.]

    :param cluster_label: [external cluster label with any prefix]
    :return str: [numeric part of the cluster label]
    """
    match = re.search(r'\d+', str(cluster_label))
    return match.group(0) if match else str(cluster_label)  


def cluster_nums_from_assign_result(assign_result: dict) -> list:
    """
    [Get all cluster numbers from a cluster assign result.]

    :param assign_result: [cluster assign result, as returned through the API]
    :return: [list of all external cluster numbers in the result]
    """
    result = set()
    for item in assign_result.values():
        result.add(cluster_num_from_label(item['cluster']))
    return list(result)


def delete_component_files(cluster_component_dict: dict,
                           fs: PoppunkFileStore,
                           assign_result: dict,
                           p_hash: str) -> None:
    """
    [poppunk generates >1100 component graph files. We only need to store those
    files from the clusters our queries belong to.]

    :param cluster_component_dict: [dictionary that maps cluster number
        to component number]
    :param fs: [PoppunkFilestore with paths to component files]
    :param assign_result: [result from clustering, needed here to define
        which clusters we want to keep]
    :param p_hash: [project hash]
    """
    components = []
    for cluster_no in cluster_nums_from_assign_result(assign_result):
        components.append(cluster_component_dict[cluster_no])

    # delete redundant component files
    keep_filenames = list(map(lambda x: f"network_component_{x}.graphml",
                              components))
    keep_filenames.append('network_cytoscape.csv')
    keep_filenames.append('network_cytoscape.graphml')
    keep_filenames.append('cluster_component_dict.pickle')
    dir = fs.output_network(p_hash)
    # remove files not in keep_filenames
    for item in list(set(os.listdir(dir)) - set(keep_filenames)):
        os.remove(os.path.join(dir, item))


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
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file != 'cluster_component_dict.pickle':
                file_list.append(os.path.join(root, file))
    with fileinput.input(files=(file_list),
                         inplace=True) as input:
        for line in input:
            line = line.rstrip()
            if not line:
                continue
            for f_key, f_value in filename_dict.items():
                if f_key in line:
                    line = line.replace(f_key, f_value)
            print(line)


def add_query_ref_status(fs: PoppunkFileStore,
                         p_hash: str,
                         filename_dict: dict) -> None:
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
    :param filename_dict: [dict that maps filehashes(keys) to
        corresponding filenames (values) of all query samples. We only need
        the filenames here.]
    """
    # list of query filenames
    query_names = list(filename_dict.values())
    # list of all component graph filenames
    file_list = glob.glob(
        fs.output_network(p_hash)+"/network_component_*.graphml")
    for path in file_list:
        xml_tree = ET.parse(path)
        graph = xml_tree.getroot()
        nodes = graph.findall(".//{http://graphml.graphdrawing.org/xmlns}node")
        for node in nodes:
            name = node.find("./").text
            child = ET.Element("data")
            child.set("key", "ref_query")
            child.text = 'query' if name in query_names else 'ref'
            node.append(child)
        ET.indent(xml_tree, space='  ', level=0)
        with open(path, 'wb') as f:
            xml_tree.write(f, encoding='utf-8')


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


def get_external_clusters_from_file(external_clusters_file: str,
                                    hashes_list: list) -> dict:
    """
    [Finds sample hashes defined by hashes_list in the given external clusters
    file and returns a dictionary of sample hash to external cluster name. If
    there are multiple external clusters listed for a sample, the lowest
    cluster number is returned]

    :param external_clusters_file: [filename of the project's external clusters
        file]
    :param hashes_list: [list of sample hashes to find samples for]
    :return dict: [dict of sample hash to lowest numbered-cluster for that
        sample]
    """
    # copy hashes list to keep track of hashes we still need to find samples
    # for
    remaining_hashes = hashes_list[:]
    result = {}
    with open(external_clusters_file) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            # We expect two columns in the external clusters csv: the
            # first contains the sample id (which will be the hash in
            # the case of our uploaded samples), and the second contains
            # all the external cluster numbers for the sample, separated
            # by semicolons
            sample_id = row[0]
            if sample_id in remaining_hashes:
                # Add lowest numeric cluster to dictionary
                if len(row) > 1:
                    cluster_no = get_lowest_cluster(row[1])
                    result[sample_id] = "GPSC{}".format(cluster_no)

                # Remove sample id from remaining hashes to find
                remaining_hashes.remove(sample_id)

                # Break if no hashes left to find
                if len(remaining_hashes) == 0:
                    break
    return result
