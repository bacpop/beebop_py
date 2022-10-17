from types import SimpleNamespace
import json
import csv
import xml.etree.ElementTree as ET
import fnmatch
import os
import re
import pickle
import fileinput


def get_args():
    with open("./beebop/resources/args.json") as a:
        args_json = a.read()
    return json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))


def generate_mapping(p_hash, fs):
    """
    PopPUNK generates one overall .graphml file covering all clusters/
    components. Furthermore, it generates one .graphml file per component,
    where the component numbers are arbitrary and do not match poppunk cluster
    numbers. To find the right component file by cluster number, we need to
    generate a mapping to be able to return the right component number based
    on cluster number. This function will generate that mapping by looking up
    the first filename from each component file in the csv file that holds all
    filenames and their corresponding clusters.
    """
    # dict to get cluster number from samplename
    with open(fs.network_output_csv(p_hash)) as f:
        next(f)  # Skip the header
        reader = csv.reader(f, skipinitialspace=True)
        samplename_cluster_dict = dict(reader)

    # list of all component graph filenames
    file_list = []
    for file in os.listdir(fs.output_network(p_hash)):
        if fnmatch.fnmatch(file, 'network_component_*.graphml'):
            file_list.append(file)

    # generate dict that maps cluster number to component number
    cluster_component_dict = {}
    for component_filename in file_list:
        component_number = re.findall(R'\d+', component_filename)[0]
        component_xml = ET.parse(fs.network_output_component(
            p_hash,
            component_number)).getroot()
        samplename = component_xml.find(
            ".//{http://graphml.graphdrawing.org/xmlns}node[@id='n0']/").text
        cluster_number = samplename_cluster_dict[samplename]
        cluster_component_dict[cluster_number] = component_number
    # save as pickle
    with open(fs.network_mapping(p_hash), 'wb') as mapping:
        pickle.dump(cluster_component_dict, mapping)
    return cluster_component_dict


def delete_component_files(cluster_component_dict, fs, assign_result, p_hash):
    """
    poppunk generates >1100 component graph files. We only need to store those
    files from the clusters our queries belong to.
    """
    queries_clusters = []
    queries_components = []
    for item in assign_result.values():
        queries_clusters.append(item['cluster'])
        queries_components.append(cluster_component_dict[str(item['cluster'])])
    components = set(queries_components)
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


def replace_filehashes(folder, filename_dict):
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


def add_query_ref_status(fs, p_hash, filename_dict):
    ET.register_namespace('', "http://graphml.graphdrawing.org/xmlns")
    ET.register_namespace('xsi', "http://www.w3.org/2001/XMLSchema-instance")
    # list of query filenames
    query_names = list(filename_dict.values())
    # list of all component graph filenames
    file_list = []
    for file in os.listdir(fs.output_network(p_hash)):
        if fnmatch.fnmatch(file, 'network_component_*.graphml'):
            file_list.append(file)
    for filename in file_list:
        path = os.path.join(fs.output_network(p_hash), filename)
        xml = ET.parse(path)
        graph = xml.getroot()
        nodes = graph.findall(".//{http://graphml.graphdrawing.org/xmlns}node")
        for node in nodes:
            name = node.find("./").text
            child = ET.Element("data")
            child.set("key", "ref_query")
            child.text = 'query' if name in query_names else 'ref'
            node.append(child)
        ET.indent(xml, space='  ', level=0)
        with open(path, 'wb') as f:
            xml.write(f, encoding='utf-8')
