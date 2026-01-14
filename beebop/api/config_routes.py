import json
import logging
from pathlib import PurePath
from types import SimpleNamespace

from flask import Blueprint, Flask, current_app
from flask.wrappers import Response
from PopPUNK import __version__ as poppunk_version
from PopPUNK.sketchlib import getKmersFromReferenceDatabase
from werkzeug.exceptions import NotFound

from beebop import __version__ as beebop_version
from beebop.models import LocationMetadata

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
                    "kmerInfo": self._get_kmer_info(f"{self.dbs_location}/{species_args.refdb}"),
                    "hasSublineages": species_args.sublineages_db is not None,
                    "hasLocationMetadata": species_args.location_metadata_file is not None,
                }
                for species, species_args in all_species_args.items()
            }
            return response_success(species_config)

        @self.config_bp.route("/locationMetadata/<species>", methods=["GET"])
        def get_location_metadata(species: str) -> Response:
            """
            Retrieves location metadata for a given species.
            This function extracts the location metadata file path from the arguments,
            reads the metadata from the file, and returns it as a JSON response.

            :param species: The species for which to retrieve location metadata.
            :return Response: JSON response containing location metadata information.
            """
            species_args = getattr(self.args.species, species, None)
            if species_args is None or species_args.location_metadata_file is None:
                raise NotFound(f"No location metadata configured for species: {species}")

            location_metadata = self._get_location_metadata_info(species_args.location_metadata_file)
            return response_success(location_metadata)

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

    def _get_location_metadata_info(self, location_metadata_file: str) -> list[LocationMetadata]:
        """
        Retrieve location metadata information from a JSON file.

        :param location_metadata_file: [Path to the location metadata JSON file.]
        :return dict: [A list containing location metadata information.]
        """
        with open(PurePath("beebop", "resources", location_metadata_file), "r") as f:
            location_metadata = json.load(f)
        return location_metadata

    def get_blueprint(self) -> Blueprint:
        """
        Returns the Flask Blueprint for the configuration routes.

        :return Blueprint: [The Blueprint containing the configuration routes.]
        """
        return self.config_bp
