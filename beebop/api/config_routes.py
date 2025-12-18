import logging
from types import SimpleNamespace

from flask import Blueprint, Flask, current_app
from flask.wrappers import Response
from PopPUNK import __version__ as poppunk_version
from PopPUNK.sketchlib import getKmersFromReferenceDatabase

from beebop import __version__ as beebop_version

from .api_utils import response_success


class ConfigRoutes:
    """
    Class to handle configuration-related routes in the Flask application.
    This class encapsulates the
    logic for handling configuration-related API endpoints.
    """

    def __init__(self, app: Flask):
        self.logger = logging.getLogger(__name__)
        self.config_bp = Blueprint("config_bp", __name__)

        with app.app_context():
            self.args: SimpleNamespace = current_app.config["args"]
            self.dbs_location: str = current_app.config["dbs_location"]
        self._setup_routes()

    def _setup_routes(self):
        @self.config_bp.route("/version", methods=["GET"])
        def report_version() -> Response:
            """
            [report version of beebop and poppunk (and ska in the future)
            wrapped in response object]

            :return Response: [response that stores version infos in 'data']
            """
            versions = [
                {"name": "beebop", "version": beebop_version},
                {"name": "poppunk", "version": poppunk_version},
            ]
            return response_success(versions)

        @self.config_bp.route("/speciesConfig", methods=["GET"])
        def get_species_config() -> Response:
            """
            Retrieves k-mer lists for all species specified in the arguments.
            This function extracts species arguments,
            fetches k-mers from the reference database for each species,
            and constructs a configuration dictionary
            containing the k-mers for each species.The result is then
            returned as a JSON response.
            Also indicates whether sub-lineages are supported for each species.

            :return Response: [JSON response containing a dictionary
                where each key is a species and the value is another
                dictionary with a list of k-mers for that species and a flag indicating sub-lineage support.]
            """
            all_species_args = vars(self.args.species)
            species_config = {
                species: {
                    **self._get_kmer_info(f"{self.dbs_location}/{species_args.refdb}"),
                    "hasSublineages": species_args.sublineages_db is not None,
                }
                for species, species_args in all_species_args.items()
            }
            return response_success(species_config)

    def _get_kmer_info(self, db_path: str) -> dict:
        """
        Retrieve k-mer information from database for a given species.

        :param species_db_name: [The name of the species database.]
        :return dict: [A dictionary containing the maximum, minimum, and step
            k-mer values.]
        """
        kmers = getKmersFromReferenceDatabase(db_path)
        return {
            "kmerMax": int(kmers[-1]),
            "kmerMin": int(kmers[0]),
            "kmerStep": int(kmers[1] - kmers[0]),
        }

    def get_blueprint(self) -> Blueprint:
        """
        Returns the Flask Blueprint for the configuration routes.

        :return Blueprint: [The Blueprint containing the configuration routes.]
        """
        return self.config_bp
