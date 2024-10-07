from rq import get_current_job
from redis import Redis
from beebop.poppunkWrapper import PoppunkWrapper
from beebop.utils import generate_mapping, delete_component_files
from beebop.utils import replace_filehashes, add_query_ref_status
from beebop.utils import get_cluster_num
from beebop.utils import cluster_nums_from_assign
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
import pickle


def microreact(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: dict,
    name_mapping: dict,
    species: str,
) -> None:
    """
    [generate files to use on microreact.org
    Output files are .csv, .dot and .nwk
    (last one only for clusters with >3 isolates).
    Also, a .microreact file is provided which can alternatively be uploaded.]

    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_fs: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param species: [Type of species]
    """

    # get results from previous job
    current_job = get_current_job(Redis())
    assign_result = current_job.dependency.result
    external_to_poppunk_clusters = None

    try:
        with open(fs.external_to_poppunk_clusters(p_hash), "rb") as dict:
            external_to_poppunk_clusters = pickle.load(dict)
    except FileNotFoundError:
        print("no external cluster info found")

    microreact_internal(
        assign_result,
        p_hash,
        fs,
        db_fs,
        args,
        name_mapping,
        species,
        external_to_poppunk_clusters,
    )


def microreact_internal(
    assign_result,
    p_hash,
    fs,
    db_fs,
    args,
    name_mapping,
    species,
    external_to_poppunk_clusters: dict = None,
) -> None:
    """
    :param assign_result: [result from assign_clusters() to get all cluster
        numbers that include query samples]
    :param p_hash: [project hash to find input data (output from
        assignClusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_fs: [DatabaseFilestore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param external_to_poppunk_clusters: [dict of external to poppunk
        clusters, used to identify the include file to pass to poppunk]
    """
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)
    queries_clusters = []
    for item in assign_result.values():
        queries_clusters.append(item["cluster"])
    for assign_cluster in set(queries_clusters):
        cluster_no = get_cluster_num(assign_cluster)
        if external_to_poppunk_clusters:
            internal_cluster = external_to_poppunk_clusters[assign_cluster]
        else:
            internal_cluster = assign_cluster

        wrapper.create_microreact(cluster_no, internal_cluster)
        replace_filehashes(
            fs.output_microreact(p_hash, cluster_no), name_mapping
        )


def network(
    p_hash: str,
    fs: PoppunkFileStore,
    db_fs: DatabaseFileStore,
    args: dict,
    name_mapping: dict,
    species: str,
) -> None:
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
    :param db_fs: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param species: [Type of species]
    """
    # get results from previous job
    current_job = get_current_job(Redis())
    assign_result = current_job.dependency.result
    network_internal(
        assign_result, p_hash, fs, db_fs, args, name_mapping, species
    )
    return assign_result


def network_internal(
    assign_result,
    p_hash,
    fs,
    db_fs: DatabaseFileStore,
    args,
    name_mapping,
    species: str,
) -> None:
    """
    :param assign_result: [result from assign_clusters() to get all cluster
        numbers that include query samples]
    :param p_hash: [project hash to find input data (output from
        assign_clusters)]
    :param fs: [PoppunkFileStore with paths to input data]
    :param db_fs: [DatabaseFileStore with paths to db files]
    :param args: [arguments for poppunk functions]
    :param name_mapping: [dict that maps filehashes (keys) to
        corresponding filenames (values) of all query samples.]
    :param species: [Type of species]
    """
    wrapper = PoppunkWrapper(fs, db_fs, args, p_hash, species)
    wrapper.create_network()

    cluster_nums_to_map = cluster_nums_from_assign(assign_result)
    cluster_component_dict = generate_mapping(
        p_hash, cluster_nums_to_map, fs, db_fs.external_clustering
    )

    delete_component_files(cluster_component_dict, fs, assign_result, p_hash)
    replace_filehashes(fs.output_network(p_hash), name_mapping)
    add_query_ref_status(fs, p_hash, name_mapping)
