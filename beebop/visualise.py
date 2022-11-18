from rq import get_current_job
from redis import Redis
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.utils import generate_mapping, delete_component_files
from beebop.utils import replace_filehashes, add_query_ref_status
from beebop.filestore import PoppunkFileStore, DatabaseFileStore


def microreact(p_hash: str,
               fs: PoppunkFileStore,
               db_paths: DatabaseFileStore,
               args: dict,
               name_mapping: dict) -> None:
    """
    [generate files to use on microreact.org
    Output files are .csv, .dot and .nwk
    (last one only for clusters with >3 isolates).
    Also, a .microreact file is provided which can alternatively be uploaded.]

    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_paths: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
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
                        name_mapping) -> None:
    """
    :param assign_result: [result from assign_clusters() to get all cluster
        numbers that include query samples]
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_paths: [DatabaseFilestore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    """
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    queries_clusters = list(assign_result.keys())
    for cluster_no in queries_clusters:
        wrapper.create_microreact(cluster_no)
        replace_filehashes(fs.output_microreact(p_hash, cluster_no),
                           name_mapping)


def network(p_hash: str,
            fs: PoppunkFileStore,
            db_paths: DatabaseFileStore,
            args: dict,
            name_mapping: dict) -> None:
    """
    [Generate files to draw a network.
    Output files are .csv and .graphml (one overall and several component
    files, those that are not relevant for us get deleted).
    Since network component number and poppunk cluster number do not
    match, we need to generate a mapping to find the right component files.
    To highlight query samples in the network graph, a ref or query status is
    added to .graphml files.]

    :param p_hash: [project hash to find input data (output from
        assign_clusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_paths: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    """
    # get results from previous job
    current_job = get_current_job(Redis())
    assign_result = current_job.dependency.result
    network_internal(assign_result, p_hash, fs, db_paths, args, name_mapping)


def network_internal(assign_result,
                     p_hash,
                     fs,
                     db_paths,
                     args,
                     name_mapping) -> None:
    """
    :param assign_result: [result from assign_clusters() to get all cluster
        numbers that include query samples]
    :param p_hash: [project hash to find input data (output from
        assign_clusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_paths: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    """
    wrapper = PoppunkWrapper(fs, db_paths, args, p_hash)
    wrapper.create_network()
    cluster_component_dict = generate_mapping(p_hash, fs)
    delete_component_files(cluster_component_dict, fs, assign_result, p_hash)
    replace_filehashes(fs.output_network(p_hash), name_mapping)
    add_query_ref_status(fs, p_hash, name_mapping)
