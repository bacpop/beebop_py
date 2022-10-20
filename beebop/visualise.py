from PopPUNK.visualise import generate_visualisations
from rq import get_current_job
from redis import Redis
import fileinput
from fileinput import FileInput
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.utils import generate_mapping
from beebop.utils import delete_component_files, replace_filehashes


def microreact(p_hash, fs, db_paths, args, name_mapping):
    """
    generate files to use with microreact.
    Output files are .csv, .dot and .nwk
    (last one only for clusters with >3 isolates)

    p_hash: project hash to find input data (output from assignClusters)
    fs: PoppunkFilestore with paths to input data
    db_paths: location of database
    args: arguments for poppunk functions
    """

    # get results from previous job
    current_job = get_current_job(Redis())
    assign_result = current_job.dependency.result
    microreact_internal(assign_result,
                        p_hash,
                        fs,
                        db_paths,
                        args,
                        name_mapping)


def microreact_internal(assign_result,
                        p_hash,
                        fs,
                        db_paths,
                        args,
                        name_mapping):
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    queries_clusters = []
    for item in assign_result.values():
        queries_clusters.append(item['cluster'])
    for cluster_no in set(queries_clusters):
        wrapper.create_microreact(cluster_no)
        replace_filehashes(fs.output_microreact(p_hash, cluster_no),
                           name_mapping)


def network(p_hash, fs, db_paths, args, name_mapping):
    # get results from previous job
    current_job = get_current_job(Redis())
    assign_result = current_job.dependency.result
    network_internal(assign_result, p_hash, fs, db_paths, args, name_mapping)


def network_internal(assign_result, p_hash, fs, db_paths, args, name_mapping):
    """
    generate files to draw a network.
    Output files are .graphml and .csv
    p_hash: project hash to find input data (output from assignClusters)
    fs: PoppunkFilestore with paths to input data
    db_paths: location of database
    args: arguments for poppunk functions

    Since network component number and poppunk cluster number do not
    match, we need to generate a mapping to find the right component files.
    """
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    wrapper.create_network()
    cluster_component_dict = generate_mapping(p_hash, fs)
    delete_component_files(cluster_component_dict, fs, assign_result, p_hash)
    replace_filehashes(fs.output_network(p_hash), name_mapping)
