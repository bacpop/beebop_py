from PopPUNK.visualise import generate_visualisations
from rq import get_current_job
from redis import Redis
from beebop.poppunkWrapper import PoppunkWrapper


def microreact(p_hash, fs, db_paths, args):
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
    microreact_internal(assign_result, p_hash, fs, db_paths, args)


def microreact_internal(assign_result, p_hash, fs, db_paths, args):
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    queries_clusters = []
    for item in assign_result.values():
        queries_clusters.append(item['cluster'])
    for cluster_no in set(queries_clusters):
        wrapper.create_microreact(cluster_no)


def network(p_hash, fs, db_paths, args):
    """
    generate files to draw a network.
    Output files are .graphml and .csv
    p_hash: project hash to find input data (output from assignClusters)
    fs: PoppunkFilestore with paths to input data
    db_paths: location of database
    args: arguments for poppunk functions
    Currently poppunk does not allow to subset isolates in this mode.
    Ideally we'd want to only display clusters that have a new isolate added.
    """
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    wrapper.create_network()
