import logging
from types import SimpleNamespace

from flask import Flask, Response, current_app, jsonify, request, send_file
from flask_expects_json import expects_json
from redis import Redis
from werkzeug.exceptions import BadRequest, NotFound

from beebop.config import Schema
from beebop.models import PoppunkFileStore
from beebop.services.cluster_service import get_cluster_num
from beebop.services.file_service import (
    get_cluster_assignments,
    get_failed_samples_internal,
)
from beebop.services.global_service import get_species_kmers, get_version
from beebop.services.job_service import get_project_status
from beebop.services.run_PoPUNK import run_PopPUNK_jobs
from beebop.services.result_service import (
    generate_microreact_url_internal,
    generate_zip,
    get_clusters_results,
)

from .api_utils import response_success

logger = logging.getLogger(__name__)


def register_routes(app: Flask):
    with app.app_context():
        args: SimpleNamespace = current_app.config["args"]
        dbs_location: str = current_app.config["dbs_location"]
        redis: Redis = current_app.config["redis"]
        storage_location: str = current_app.config["storage_location"]
        schemas: Schema = current_app.config["schemas"]
        fs = PoppunkFileStore(storage_location)

    @app.route("/version", methods=["GET"])
    def report_version() -> Response:
        """
        [report version of beebop and poppunk (and ska in the future)
        wrapped in response object]

        :return Response: [response that stores version infos in 'data']
        """
        vers = get_version()
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
            species: get_species_kmers(f"{dbs_location}/{args.refdb}")
            for species, args in all_species_args.items()
        }
        return jsonify(response_success(species_config))

    @app.route("/status/<string:p_hash>")
    def get_status(p_hash: str) -> Response:
        """
        [returns job statuses for all jobs with given project hash. Possible
        values are: queued, started, deferred, finished, stopped, canceled,
        scheduled and failed]

        :param p_hash: [project hash]
        :return Response: [response object with job statuses]
        """
        response = get_project_status(p_hash, redis)
        return jsonify(response_success(response))

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

        status = get_project_status(p_hash, redis)

        clusters_result = get_cluster_assignments(p_hash, fs)
        failed_samples = get_failed_samples_internal(p_hash, fs)

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

    @app.route("/results/networkGraphs/<string:p_hash>", methods=["GET"])
    def get_network_graphs(
        p_hash: str,
    ) -> Response:
        """
        [returns all network pruned graphml files for a given project hash]

        :param p_hash: [project hash]
        :return Response: [response object with all graphml files stored in 'data']
        """
        try:
            cluster_result = get_cluster_assignments(p_hash, fs)
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
            raise NotFound(
                "GraphML files not found for the given project hash"
            )

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
                cluster_results = get_clusters_results(p_hash, fs)
                return jsonify(response_success(cluster_results))
            case "zip":
                p_hash = request.json["projectHash"]
                visualisation_type = request.json["type"]
                cluster = str(request.json["cluster"])
                zip_file = generate_zip(
                    fs, p_hash, visualisation_type, cluster
                )
                return send_file(
                    zip_file,
                    download_name=visualisation_type + ".zip",
                    as_attachment=True,
                )
            case "microreact":
                microreact_api_new_url = (
                    "https://microreact.org/api/projects/create"
                )
                p_hash = request.json["projectHash"]
                cluster = str(request.json["cluster"])
                api_token = str(request.json["apiToken"])
                url = generate_microreact_url_internal(
                    microreact_api_new_url,
                    p_hash,
                    cluster,
                    api_token,
                    fs,
                )
                return jsonify(
                    response_success({"cluster": cluster, "url": url})
                )
            case _:
                raise BadRequest("Invalid result type specified.")

    @app.route("/poppunk", methods=["POST"])
    @expects_json(schemas.run_poppunk)
    def run_PopPUNK() -> Response:
        """
        [run poppunks assing_query() and generate_visualisations().
        input: multiple sketches in json format together with project hash
        and filename mapping, schema can be found in spec/sketches.schema.json]

        :return Response: [response object with all job IDs stored in 'data']
        """
        if request.json is None:
            raise BadRequest("Request body is missing or not in JSON format.")
        sketches = request.json["sketches"].items()
        p_hash = request.json["projectHash"]
        name_mapping = request.json["names"]
        species = request.json["species"]
        amr_metadata = request.json["amrForMetadataCsv"]

        job_ids = run_PopPUNK_jobs(
            sketches, p_hash, name_mapping, species, amr_metadata
        )
        return jsonify(response_success(job_ids))
