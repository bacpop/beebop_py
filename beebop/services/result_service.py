import datetime
import json
from io import BytesIO

import requests
from werkzeug.exceptions import InternalServerError, NotFound

from beebop.config import PoppunkFileStore

from .cluster_service import get_cluster_num
from .file_service import (
    add_files,
    get_cluster_assignments,
    get_failed_samples_internal,
    get_network_files_for_zip,
)


def get_clusters_results(p_hash: str, fs: PoppunkFileStore) -> dict:
    """
    [returns cluster assignment results]

    :param p_hash: [project hash]
    :param fs: [PoppunkFileStore instance]
    :return dict: [dictionary with cluster results]
    """
    cluster_result = get_cluster_assignments(p_hash, fs)
    cluster_dict = {value["hash"]: value for value in cluster_result.values()}
    failed_samples = get_failed_samples_internal(p_hash, fs)

    return {**cluster_dict, **failed_samples}


def generate_zip(fs: PoppunkFileStore, p_hash: str, result_type: str, cluster: str) -> BytesIO:
    """
    [This generates a .zip folder with results data.]

    :param fs: [PoppunkFileStore with path to folder to be zipped]
    :param p_hash: [project hash]
    :param result_type: [can be either 'microreact' or 'network']
    :param cluster: [cluster assigned]
    :return BytesIO: [memory file]
    """
    memory_file = BytesIO()
    cluster_num = get_cluster_num(cluster)
    visualisations_folder = fs.output_visualisations(p_hash, cluster_num)
    network_files = get_network_files_for_zip(visualisations_folder, cluster_num)

    if result_type == "microreact":
        # microreact zip should include all files from the
        # visualisations folder except those which are
        # network files, hence set exclude to True
        add_files(memory_file, visualisations_folder, network_files, exclude=True)
    elif result_type == "network":
        add_files(memory_file, visualisations_folder, network_files, exclude=False)
    memory_file.seek(0)
    return memory_file


def generate_microreact_url_internal(
    microreact_api_new_url: str,
    p_hash: str,
    cluster: str,
    api_token: str,
    fs: PoppunkFileStore,
) -> str:
    """
    [Generates Microreact URL to a microreact project with the users data
    already being uploaded.]

    :param microreact_api_new_url: [URL where the microreact API can be
        accessed]
    :param p_hash: [project hash]
    :param cluster: [cluster number]
    :param api_token: [this ust be provided by the user. The new API does
        not allow generating a URL without a token.]
    :param fs: [PoppunkFileStore instance]
    :return Response: [response object with URL stored in 'data']
    """
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
            return r.json()["url"]
        case 500:
            raise InternalServerError("Microreact reported Internal Server Error. Most likely Token is invalid!")
        case 404:
            raise NotFound("Cannot reach Microreact API")
        case _:
            raise InternalServerError(f"Microreact API returned status code {r.status_code}. Response text: {r.text}.")


def update_microreact_json(json_microreact: dict, cluster_num: str) -> None:
    """
    [Updates the title of the microreact json file.]

    :param json_microreact: [microreact json]
    :param cluster_num: [cluster number]
    """
    # update title
    json_microreact["meta"]["name"] = (
        f"Cluster {cluster_num} - {datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M')}"
    )

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
