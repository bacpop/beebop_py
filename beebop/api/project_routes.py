import logging
from typing import Literal

from flask import (
    Blueprint,
    Flask,
    Response,
    current_app,
    request,
    send_file,
)
from flask_expects_json import expects_json
from werkzeug.exceptions import BadRequest, NotFound

from beebop.config import PoppunkFileStore, Schema
from beebop.db import RedisManager
from beebop.services.cluster_service import get_cluster_num
from beebop.services.file_service import (
    get_cluster_assignments,
    get_failed_samples_internal,
)
from beebop.services.job_service import get_project_status
from beebop.services.result_service import (
    generate_microreact_url_internal,
    generate_zip,
    get_clusters_results,
)
from beebop.services.run_PopPUNK import run_PopPUNK_jobs

from .api_utils import response_success


class ProjectRoutes:
    """
    Class to handle configuration-related routes in the Flask application.
    This class encapsulates the logic for
    handling configuration-related API endpoints.
    """

    def __init__(self, app: Flask):
        self.logger = logging.getLogger(__name__)
        self.project_bp = Blueprint("project_bp", __name__)

        with app.app_context():
            self.redis_manager = RedisManager(current_app.config["redis"])
            self.storage_location: str = current_app.config["storage_location"]
            self.schemas: Schema = current_app.config["schemas"]
            self.fs = PoppunkFileStore(self.storage_location)
        self._setup_routes()

    def _setup_routes(self):
        @self.project_bp.route("/poppunk", methods=["POST"])
        @expects_json(self.schemas.run_poppunk)
        def run_PopPUNK() -> Response:
            """
            [run poppunks assing_query() and generate_visualisations().
            input: multiple sketches in json format together with project hash
            and filename mapping, schema can be
            found in spec/sketches.schema.json]

            :return Response: [response object with all
            job IDs stored in 'data']
            """
            if request.json is None:
                raise BadRequest("Request body is missing or not in JSON format.")
            sketches = request.json["sketches"].items()
            p_hash = request.json["projectHash"]
            name_mapping = request.json["names"]
            species = request.json["species"]
            amr_metadata = request.json["amrForMetadataCsv"]

            job_ids = run_PopPUNK_jobs(sketches, p_hash, name_mapping, species, amr_metadata)
            return response_success(job_ids)

        @self.project_bp.route("/status/<string:p_hash>", methods=["GET"])
        def get_status(p_hash: str) -> Response:
            """
            [returns job statuses for all jobs with given project hash.
            Possible values are: queued, started, deferred,
            finished, stopped, canceled, scheduled and failed]

            :param p_hash: [project hash]
            :return Response: [response object with job statuses]
            """
            response = get_project_status(p_hash, self.redis_manager)
            return response_success(response)

        @self.project_bp.route("/project/<string:p_hash>", methods=["GET"])
        def get_project(p_hash: str) -> Response:
            """
            [Loads all project data for a given project hash so the project can
            be re-opened in beebop.]

            :param p_hash: [identifying hash for the project]
            :return: [project data]
            """
            job_id = self.redis_manager.get_job_status("assign", p_hash)
            if job_id is None:
                raise NotFound("Project hash does not have an associated job")

            status = get_project_status(p_hash, self.redis_manager)

            clusters_result = get_cluster_assignments(p_hash, self.fs)
            failed_samples = get_failed_samples_internal(p_hash, self.fs)

            passed_samples = {}
            for value in clusters_result.values():
                sample_hash = value["hash"]
                sketch = self.fs.input.get(sample_hash)
                passed_samples[sample_hash] = {
                    "hash": sample_hash,
                    "sketch": sketch,
                }
                # Cluster may not have been assigned yet
                passed_samples[sample_hash]["cluster"] = value.get("cluster")

            return response_success(
                {
                    "hash": p_hash,
                    "samples": {**passed_samples, **failed_samples},
                    "status": status,
                }
            )

        @self.project_bp.route("/results/networkGraphs/<string:p_hash>", methods=["GET"])
        def get_network_graphs(
            p_hash: str,
        ) -> Response:
            """
            [returns all network pruned graphml files for a given project hash]

            :param p_hash: [project hash]
            :return Response: [response object with all
            graphml files stored in 'data']
            """
            try:
                cluster_result = get_cluster_assignments(p_hash, self.fs)
                graphmls = {}
                for cluster_info in cluster_result.values():
                    cluster = cluster_info["cluster"]
                    path = self.fs.pruned_network_output_component(
                        p_hash,
                        cluster_info["raw_cluster_num"],
                        get_cluster_num(cluster),
                    )
                    with open(path, "r") as graphml_file:
                        graph = graphml_file.read()
                    graphmls[cluster] = graph
                return response_success(graphmls)

            except KeyError as e:
                raise NotFound("Cluster not found for the given project hash") from e
            except FileNotFoundError as e:
                raise NotFound("GraphML files not found for the given project hash") from e

        @self.project_bp.route("/results/<string:result_type>", methods=["POST"])
        def get_results(result_type: Literal["assign", "zip", "microreact", "sub_lineage_assign"]) -> Response:
            """
            [Route to get results for the specified type of analysis.
            Request object includes:
                project_hash
                type - only for 'zip' results, can be 'microreact' or 'network'
                cluster - for 'zip', 'microreact' and 'graphml' results
                api_token - only required for  'microreact' URL generation.
                This must be provided by the user in the frontend]

            :param result_type: [can be
                - 'assign' for clusters
                - 'zip' for visualisation results as zip folders
                    (with the json property 'type' specifying whether
                    'microreact' or 'network' results are required)
                - 'microreact' for the microreact URL for a given cluster
            :return Response: [response object with result stored in 'data']
            """
            self.logger.info(f"Request for results of type: {result_type}")
            if request.json is None:
                raise BadRequest("Request body is missing or not in JSON format.")
            match result_type:
                case "assign":
                    p_hash = request.json["projectHash"]
                    cluster_results = get_clusters_results(p_hash, self.fs)
                    return response_success(cluster_results)
                case "zip":
                    p_hash = request.json["projectHash"]
                    visualisation_type = request.json["type"]
                    cluster = str(request.json["cluster"])
                    zip_file = generate_zip(self.fs, p_hash, visualisation_type, cluster)
                    return send_file(
                        zip_file,
                        download_name=visualisation_type + ".zip",
                        as_attachment=True,
                    )
                case "microreact":
                    microreact_api_new_url = "https://microreact.org/api/projects/create"
                    p_hash = request.json["projectHash"]
                    cluster = str(request.json["cluster"])
                    api_token = str(request.json["apiToken"])
                    url = generate_microreact_url_internal(
                        microreact_api_new_url,
                        p_hash,
                        cluster,
                        api_token,
                        self.fs,
                    )
                    return response_success({"cluster": cluster, "url": url})

                case _:
                    raise BadRequest("Invalid result type specified.")

    def get_blueprint(self) -> Blueprint:
        """
        Returns the Flask Blueprint for the project routes.
        This method is used to register the blueprint
        with the Flask application.

        :return: Flask Blueprint for project routes
        """
        return self.project_bp
