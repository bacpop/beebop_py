from flask import Flask, jsonify, request, abort, send_file
from flask.wrappers import Response
from flask_expects_json import expects_json
from waitress import serve
from redis import Redis
import redis.exceptions as redis_exceptions
from rq import Queue
from rq.job import Job
import os
from io import BytesIO
import zipfile
import json
import requests
import pickle
from datetime import datetime
import pandas as pd
from beebop import versions, assignClusters, visualise
from beebop.config import get_environment, get_args
from beebop.filestore import PoppunkFileStore, DatabaseFileStore
from beebop.utils import get_cluster_num, get_component_filepath
from PopPUNK.sketchlib import getKmersFromReferenceDatabase
import beebop.schemas
from beebop.dataClasses import SpeciesConfig, ResponseBody, ResponseError
from typing import Any, Union, Literal, Optional
import logging
from werkzeug.exceptions import BadRequest, NotFound, InternalServerError

#  setup environment
JOB_TIMEOUT = 1200
schemas = beebop.schemas.Schema()
storage_location, dbs_location, redis_host = get_environment()
args = get_args()
redis = Redis(host=redis_host)
# setup app
app = Flask(__name__)
# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def response_success(data: Any) -> ResponseBody:
    """
    :param data: [data to be stored in response object]
    :return dict: [response object for successful response holding data]
    """
    response = ResponseBody(status="success", errors=[], data=data)
    return response


def response_failure(error: ResponseError) -> ResponseBody:
    """
    :param error: [error object with error message and details]
    :return Response: [response object for error
    response holding error message]
    """
    response = ResponseBody(status="failure", errors=[error], data=[])
    return response


def check_connection(redis) -> None:
    """
    :param redis: [Redis instance]
    """
    try:
        redis.ping()
    except (redis_exceptions.ConnectionError, ConnectionRefusedError):
        raise InternalServerError(
            "Redis connection error. Please check if Redis is running."
        )


def generate_zip(
    fs: PoppunkFileStore, p_hash: str, type: str, cluster: str
) -> BytesIO:
    """
    [This generates a .zip folder with results data.]

    :param fs: [PoppunkFileStore with path to folder to be zipped]
    :param p_hash: [project hash]
    :param type: [can be either 'microreact' or 'network']
    :param cluster: [cluster assigned]
    :return BytesIO: [memory file]
    """
    memory_file = BytesIO()
    cluster_num = get_cluster_num(cluster)
    visualisations_folder = fs.output_visualisations(p_hash, cluster_num)
    network_files = get_network_files_for_zip(
        visualisations_folder, cluster_num
    )

    if type == "microreact":
        # microreact zip should include all files from the
        # visualisations folder except those which are
        # network files, hence set exclude to True
        add_files(
            memory_file, visualisations_folder, network_files, exclude=True
        )
    elif type == "network":
        add_files(
            memory_file, visualisations_folder, network_files, exclude=False
        )
    memory_file.seek(0)
    return memory_file


def get_network_files_for_zip(
    visualisations_folder: str, cluster_num: str
) -> list[str]:
    """
    [Get the network files for a given cluster number,
    that will be used for network zip generation.
    These are the graphml files and the csv file for cytoscape.]

    :param visualisations_folder: [path to visualisations folder]
    :param cluster_num: [cluster number]
    :return list[str]: [list of network files to be included in zip]
    """
    network_file_name = os.path.basename(
        get_component_filepath(visualisations_folder, cluster_num)
    )

    return [
        network_file_name,
        f"pruned_{network_file_name}",
        f"visualise_{cluster_num}_cytoscape.csv",
    ]


def add_files(
    memory_file: BytesIO,
    path_folder: str,
    file_list: list[str],
    exclude: bool,
) -> BytesIO:
    """
    [Add files in specified folder to a memory_file.
    If exclude is True, only files not in file_list are added.
    If exclude is False, only files in file_list are added.]

    :param memory_file: [empty memory file to add files to]
    :param path_folder: [path to folder with files to include]
    :param file_list: [list of files to include/exclude]
    :param: exclude: [whether to exclude the file list or not]
    :return BytesIO: [memory file with added files]
    """
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(path_folder):
            for file in files:
                if (not exclude and file in file_list) or (
                    exclude and file not in file_list
                ):
                    zipf.write(os.path.join(root, file), arcname=file)
    return memory_file


@app.errorhandler(500)
def internal_server_error(e) -> tuple[Response, Literal[500]]:
    """
    :param e: [error]
    :return Response: [error response object]
    """
    logger.warning(f"Internal Server Error: {e}")
    return (
        jsonify(
            error=response_failure(
                ResponseError(
                    error="Internal Server Error", detail=str(e.description)
                )
            )
        ),
        500,
    )


@app.errorhandler(400)
def bad_request(e) -> tuple[Response, Literal[400]]:
    """
    :param e: [error]
    :return Response: [error response object]
    """
    logger.warning(f"Bad Request: {e}")
    return (
        jsonify(
            error=response_failure(
                ResponseError(error="Bad Request", detail=str(e.description))
            )
        ),
        400,
    )


@app.errorhandler(404)
def not_found(e) -> tuple[Response, Literal[404]]:
    """
    :param e: [error]
    :return Response: [error response object]
    """
    logger.warning(f"Not found: {e}")
    return (
        jsonify(
            error=response_failure(
                ResponseError(
                    error="Resource not found", detail=str(e.description)
                )
            )
        ),
        404,
    )


@app.route("/version", methods=["GET"])
def report_version() -> Response:
    """
    [report version of beebop and poppunk (and ska in the future)
    wrapped in response object]

    :return Response: [response that stores version infos in 'data']
    """
    vers = versions.get_version()
    return jsonify(response_success(vers))


@app.route("/speciesConfig", methods=["GET"])
def get_species_config() -> Response:
    """
    Retrieves k-mer lists for all species specified in the arguments.
    This function extracts species arguments,
    fetches k-mers from the reference database for each species,
    and constructs a configuration dictionary
    containing the k-mers for each species.The result is then
    returned as a JSON response.

    :return Response: [JSON response containing a dictionary
        where each key is a species and the value is another
        dictionary with a list of k-mers for that species.]
    """
    all_species_args = vars(args.species)
    species_config = {
        species: get_species_kmers(args.refdb)
        for species, args in all_species_args.items()
    }
    return jsonify(response_success(species_config))


def get_species_kmers(species_db_name: str) -> dict:
    """
    Retrieve k-mer information from database for a given species.

    :param species_db_name: [The name of the species database.]
    :return dict: [A dictionary containing the maximum, minimum, and step
        k-mer values.]
    """
    kmers = getKmersFromReferenceDatabase(f"{dbs_location}/{species_db_name}")
    return {
        "kmerMax": int(kmers[-1]),
        "kmerMin": int(kmers[0]),
        "kmerStep": int(kmers[1] - kmers[0]),
    }


@app.route("/poppunk", methods=["POST"])
@expects_json(schemas.run_poppunk)
def run_poppunk() -> Response:
    """
    [run poppunks assing_query() and generate_visualisations().
    input: multiple sketches in json format together with project hash
    and filename mapping, schema can be found in spec/sketches.schema.json]

    :return Response: [response object with all job IDs stored in 'data']
    """
    sketches = request.json["sketches"].items()
    p_hash = request.json["projectHash"]
    name_mapping = request.json["names"]
    species = request.json["species"]
    amr_metadata = request.json["amrForMetadataCsv"]
    q = Queue(connection=redis)

    return run_poppunk_internal(
        sketches,
        p_hash,
        name_mapping,
        storage_location,
        redis,
        q,
        species,
        amr_metadata,
    )


def run_poppunk_internal(
    sketches: dict,
    p_hash: str,
    name_mapping: dict,
    storage_location: str,
    redis: Redis,
    q: Queue,
    species: str,
    amr_metadata: list[dict],
) -> Response:
    """
    [Runs all poppunk functions we are interested in on the provided sketches.
    These are clustering with poppunk_assign, and creating visualisations
    (microreact and network) with poppunk_visualise.]

    :param sketches: [all sketches in json format]
    :param p_hash: [project hash]
    :param name_mapping: [maps filehashes to filenames for all query
        samples]
    :param storage_location: [path to storage location]
    :param redis: [Redis instance]
    :param q: [redis queue]
    :param species: [type of species to be analyzed]
    :param amr_metadata: [AMR metadata for query samples]
    :return Response: [response object with all job IDs stored in 'data']
    """
    fs = PoppunkFileStore(storage_location)
    species_args = getattr(args.species, species, None)
    if not species_args:
        raise BadRequest(f"No database found for species: {species}")

    ref_db_fs, full_db_fs = setup_db_file_stores(species_args)

    # store json sketches in storage, and store an initial output_cluster file
    # to record sample hashes for the project
    hashes_list: list[str] = []
    initial_output: dict[int, dict[str, str]] = {}
    for i, (key, value) in enumerate(sketches):
        hashes_list.append(key)
        fs.input.put(key, value)
        initial_output[i] = {"hash": key}
    # setup output directory and save hashes
    fs.setup_output_directory(p_hash)
    with open(fs.output_cluster(p_hash), "wb") as f:
        pickle.dump(initial_output, f)

    check_connection(redis)
    # keep results forever
    queue_kwargs = {
        "job_timeout": JOB_TIMEOUT,
        "result_ttl": -1,
        "failure_ttl": -1,
    }
    # submit list of hashes to redis worker
    job_assign = q.enqueue(
        assignClusters.get_clusters,
        hashes_list,
        p_hash,
        fs,
        ref_db_fs,
        full_db_fs,
        args,
        species,
        **queue_kwargs,
    )
    redis.hset("beebop:hash:job:assign", p_hash, job_assign.id)

    # create visualisations
    add_amr_to_metadata(fs, p_hash, amr_metadata, ref_db_fs.metadata)
    # delete all previous visualise cluster job results for this project
    redis.delete(f"beebop:hash:job:visualise:{p_hash}")
    job_visualise = q.enqueue(
        visualise.visualise,
        args=(
            p_hash,
            fs,
            full_db_fs,
            args,
            name_mapping,
            species,
            redis_host,
            queue_kwargs,
        ),
        depends_on=job_assign,
        **queue_kwargs,
    )
    redis.hset("beebop:hash:job:visualise", p_hash, job_visualise.id)
    return jsonify(
        response_success(
            {
                "assign": job_assign.id,
                "visualise": job_visualise.id,
            }
        )
    )


def add_amr_to_metadata(
    fs: PoppunkFileStore,
    p_hash: str,
    amr_metadata: list[dict],
    metadata_file: Optional[str] = None,
) -> None:
    """
    [Create new metadata file with AMR metadata
    and existing metadata csv file]

    :param fs: [PoppunkFileStore with paths to in-/outputs]
    :param p_hash: [project hash]
    :param amr_metadata: [AMR metadata]
    :param metadata_file: [db metadata csv file]
    """
    if metadata_file is None:
        metadata = None
    else:
        metadata = pd.read_csv(metadata_file)
    amr_df = pd.DataFrame(amr_metadata)

    pd.concat([metadata, amr_df], ignore_index=True).to_csv(
        fs.tmp_output_metadata(p_hash), index=False
    )


def setup_db_file_stores(
    species_args: SpeciesConfig,
) -> tuple[DatabaseFileStore, DatabaseFileStore]:
    """
    [Initializes the reference and full database file stores
    with the given species arguments. If the full database
    does not exist, fallback to reference database.]

    :param species_args: [species arguments]
    :return tuple[DatabaseFileStore, DatabaseFileStore]: [reference and full
        database file stores]
    """
    ref_db_fs = DatabaseFileStore(
        f"{dbs_location}/{species_args.refdb}",
        species_args.external_clusters_file,
        species_args.db_metadata_file,
    )

    if os.path.exists(f"{dbs_location}/{species_args.fulldb}"):
        full_db_fs = DatabaseFileStore(
            f"{dbs_location}/{species_args.fulldb}",
            species_args.external_clusters_file,
            species_args.db_metadata_file,
        )
    else:
        full_db_fs = ref_db_fs

    return ref_db_fs, full_db_fs


@app.route("/status/<string:p_hash>")
def get_status(p_hash: str) -> Response:
    """
    [returns job statuses for all jobs with given project hash. Possible
    values are: queued, started, deferred, finished, stopped, canceled,
    scheduled and failed]

    :param p_hash: [project hash]
    :return Response: [response object with job statuses]
    """
    return get_status_response(p_hash, redis)


def get_status_response(p_hash: str, redis: Redis) -> Response:
    """
    [returns response of all job statuses for a project]

    :param p_hash: [project hash]
    :param redis: [Redis instance]
    :return Response: [response object with job statuses]
    """
    response = get_status_internal(p_hash, redis)
    return jsonify(response_success(response))


def get_status_internal(
    p_hash: str, redis: Redis
) -> Union[dict, ResponseError]:
    """
    [returns statuses of all jobs from a given project (cluster assignment,
    initial visualisations job that kicks off all other jobs
    ,and visualisations for all clusters)]

    :param p_hash: [project hash]
    :param redis: [Redis instance]
    :return: [dict with job statuses]
    """
    check_connection(redis)

    def get_status_job(job, p_hash, redis):
        id = redis.hget(f"beebop:hash:job:{job}", p_hash).decode("utf-8")
        return Job.fetch(id, connection=redis).get_status()

    try:
        status_assign = get_status_job("assign", p_hash, redis)
        if status_assign == "finished":
            visualise = get_status_job("visualise", p_hash, redis)
            visualise_cluster_statuses = {
                cluster.decode("utf-8"): Job.fetch(
                    status.decode("utf-8"), connection=redis
                ).get_status()
                for cluster, status in redis.hgetall(
                    f"beebop:hash:job:visualise:{p_hash}"
                ).items()
            }
        else:
            visualise = "waiting"
            visualise_cluster_statuses = {}

        return {
            "assign": status_assign,
            "visualise": visualise,  # visualise for all
            "visualiseClusters": visualise_cluster_statuses,
        }
    except AttributeError:
        raise NotFound("Unknown project hash")


@app.route("/results/networkGraphs/<string:p_hash>", methods=["GET"])
def get_network_graphs(
    p_hash: str,
) -> Response:
    """
    [returns all network pruned graphml files for a given project hash]

    :param p_hash: [project hash]
    :return Response: [response object with all graphml files stored in 'data']
    """
    fs = PoppunkFileStore(storage_location)
    try:
        cluster_result = get_cluster_assignments(p_hash, storage_location)
        graphmls = {}
        for cluster_info in cluster_result.values():
            cluster = cluster_info["cluster"]
            path = fs.pruned_network_output_component(
                p_hash,
                cluster_info["raw_cluster_num"],
                get_cluster_num(cluster),
            )
            with open(path, "r") as graphml_file:
                graph = graphml_file.read()
            graphmls[cluster] = graph
        return jsonify(response_success(graphmls))

    except KeyError:
        raise NotFound("Cluster not found for the given project hash")
    except FileNotFoundError:
        raise NotFound("GraphML files not found for the given project hash")


# get job result
@app.route("/results/<string:result_type>", methods=["POST"])
def get_results(result_type: str) -> Response:
    """
    [Route to get results for the specified type of analysis.
    Request object includes:
        project_hash
        type - only for 'zip' results, can be 'microreact' or 'network'
        cluster - for 'zip', 'microreact' and 'graphml' results
        api_token - only required for  'microreact' URL generation. This
        must be provided by the user in the frontend]

    :param result_type: [can be
        - 'assign' for clusters
        - 'zip' for visualisation results as zip folders (with the json
            property 'type' specifying whether 'microreact' or 'network'
            results are required)
        - 'microreact' for the microreact URL for a given cluster
    :return Response: [response object with result stored in 'data']
    """
    logger.info(f"Request for results of type: {result_type}")
    if request.json is None:
        raise BadRequest("Request body is missing or not in JSON format.")
    match result_type:
        case "assign":
            p_hash = request.json["projectHash"]
            return get_clusters_json(p_hash, storage_location)
        case "zip":
            p_hash = request.json["projectHash"]
            visualisation_type = request.json["type"]
            cluster = str(request.json["cluster"])
            return send_zip_internal(
                p_hash, visualisation_type, cluster, storage_location
            )
        case "microreact":
            microreact_api_new_url = (
                "https://microreact.org/api/projects/create"
            )
            p_hash = request.json["projectHash"]
            cluster = str(request.json["cluster"])
            api_token = str(request.json["apiToken"])
            return generate_microreact_url_internal(
                microreact_api_new_url,
                p_hash,
                cluster,
                api_token,
                storage_location,
            )
        case _:
            raise BadRequest("Invalid result type specified.")


def get_cluster_assignments(
    p_hash: str, storage_location: str
) -> dict[int, dict[str, str]]:
    """
    [returns cluster assignment results.
    Return of type:
    {idx: {hash: hash, cluster: cluster, raw_cluster_num: raw_cluster_num}}]

    :param p_hash: [project hash]
    :param storage_location: [storage location]
    :return dict: [cluster results]
    """
    fs = PoppunkFileStore(storage_location)
    with open(fs.output_cluster(p_hash), "rb") as f:
        cluster_result = pickle.load(f)
        return cluster_result


def get_clusters_json(p_hash: str, storage_location: str) -> Response:
    """
    [returns cluster assignment results as json response]

    :param p_hash: [project hash]
    :param storage_location: [storage location]
    :return Response: [response object with cluster results stored in 'data']
    """
    cluster_result = get_cluster_assignments(p_hash, storage_location)
    cluster_dict = {value["hash"]: value for value in cluster_result.values()}
    failed_samples = get_failed_samples_internal(p_hash, storage_location)

    return jsonify(response_success({**cluster_dict, **failed_samples}))


def send_zip_internal(
    p_hash: str, type: str, cluster: str, storage_location: str
) -> Response:
    """
    [Generates a zipfile with visualisation results and returns zipfile]

    :param p_hash: [project hash]
    :param type: [either 'microreact' or 'network']
    :param cluster: [cluster number]
    :param storage_location: [storage location]
    :return any: [zipfile]
    """
    fs = PoppunkFileStore(storage_location)
    # generate zipfile
    memory_file = generate_zip(fs, p_hash, type, cluster)
    return send_file(
        memory_file, download_name=type + ".zip", as_attachment=True
    )


def generate_microreact_url_internal(
    microreact_api_new_url: str,
    p_hash: str,
    cluster: str,
    api_token: str,
    storage_location: str,
) -> Response:
    """
    [Generates Microreact URL to a microreact project with the users data
    already being uploaded.]

    :param microreact_api_new_url: [URL where the microreact API can be
        accessed]
    :param p_hash: [project hash]
    :param cluster: [cluster number]
    :param api_token: [this ust be provided by the user. The new API does
        not allow generating a URL without a token.]
    :param storage_location: [storage location]
    :return Response: [response object with URL stored in 'data']
    """
    fs = PoppunkFileStore(storage_location)

    cluster_num = get_cluster_num(cluster)
    path_json = fs.microreact_json(p_hash, cluster_num)

    with open(path_json, "rb") as microreact_file:
        json_microreact = json.load(microreact_file)

    update_microreact_json(json_microreact, cluster_num)
    # generate URL from microreact API
    headers = {
        "Content-type": "application/json; charset=UTF-8",
        "Access-Token": api_token,
    }
    r = requests.post(
        microreact_api_new_url,
        data=json.dumps(json_microreact),
        headers=headers,
    )
    match r.status_code:
        case 200:
            url = r.json()["url"]
            return jsonify(response_success({"cluster": cluster, "url": url}))
        case 500:
            raise InternalServerError(
                "Microreact reported Internal Server Error. "
                "Most likely Token is invalid!"
            )
        case 404:
            raise NotFound("Cannot reach Microreact API")
        case _:
            raise InternalServerError(
                f"Microreact API returned status code {r.status_code}. "
                f"Response text: {r.text}."
            )


def update_microreact_json(json_microreact: dict, cluster_num: str) -> None:
    """
    [Updates the title of the microreact json file.]

    :param json_microreact: [microreact json]
    :param cluster_num: [cluster number]
    """
    # update title
    json_microreact["meta"][
        "name"
    ] = f"Cluster {cluster_num} - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    # default columns to show with widths sorted by queries first
    default_cols_to_add = [
        {"field": "Status", "width": 103, "sort": "asc"},
        {"field": "Penicillin Resistance", "width": 183},
        {"field": "Chloramphenicol Resistance", "width": 233},
        {"field": "Erythromycin Resistance", "width": 209},
        {"field": "Tetracycline Resistance", "width": 202},
        {"field": "Cotrim Resistance", "width": 169},
    ]
    json_microreact["tables"]["table-1"]["columns"] += default_cols_to_add


@app.route("/project/<string:p_hash>", methods=["GET"])
def get_project(p_hash: str) -> Response:
    """
    [Loads all project data for a given project hash so the project can be
    re-opened in beebop.]

    :param p_hash: [identifying hash for the project]
    :return: [project data]
    """
    job_id = redis.hget("beebop:hash:job:assign", p_hash)
    if job_id is None:
        raise NotFound("Project hash does not have an associated job")

    status = get_status_internal(p_hash, redis)

    clusters_result = get_cluster_assignments(p_hash, storage_location)
    failed_samples = get_failed_samples_internal(p_hash, storage_location)

    fs = PoppunkFileStore(storage_location)
    passed_samples = {}
    for value in clusters_result.values():
        sample_hash = value["hash"]
        sketch = fs.input.get(sample_hash)
        passed_samples[sample_hash] = {
            "hash": sample_hash,
            "sketch": sketch,
        }
        # Cluster may not have been assigned yet
        passed_samples[sample_hash]["cluster"] = value.get("cluster")

    return jsonify(
        response_success(
            {
                "hash": p_hash,
                "samples": {**passed_samples, **failed_samples},
                "status": status,
            }
        )
    )


def get_failed_samples_internal(
    p_hash: str, storage_location: str
) -> dict[str, dict]:
    """
    [Returns a dictionary of failed samples for a given project hash]

    :param p_hash (str): The hash of the samples to retrieve.
    :param storage_location (str): The location of the storage.

    :return dict[str, dict]: failed samples
    containing hash and reasons for failure.
    """
    fs = PoppunkFileStore(storage_location)
    qc_report_file_path = fs.output_qc_report(p_hash)
    failed_samples = {}
    if os.path.exists(qc_report_file_path):
        with open(fs.output_qc_report(p_hash), "r") as f:
            for line in f:
                hash, reasons = line.strip().split("\t")
                failed_samples[hash] = {
                    "failReasons": reasons.split(","),
                    "hash": hash,
                }
    return failed_samples


if __name__ == "__main__":
    serve(app)  # pragma: no cover
