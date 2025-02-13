import subprocess
import beebop.schemas
from beebop import assignClusters
from beebop import visualise
from tests import hdf5_to_json
import json
from beebop.filestore import PoppunkFileStore, FileStore, DatabaseFileStore
from beebop.utils import get_args
import pandas as pd

schemas = beebop.schemas.Schema()


def generate_json_pneumo():
    # generate hdf5 sketch from fasta file using pp-sketchlib
    subprocess.run(
        "sketchlib sketch -l sketchlib_input/rfile.txt -o pneumo_sample -s 9984 --cpus 4 -k 14,29,3",  # noqa
        shell=True,
        cwd="tests/results",
    )

    # translate hdf5 into json
    filepath = "tests/results/pneumo_sample.h5"
    sketches_json = json.loads(hdf5_to_json.h5_to_json(filepath))

    return json.dumps(sketches_json)


storage_location = "./tests/results"
fs = PoppunkFileStore(storage_location)

expected_assign_result = {
    0: {
        "cluster": "GPSC16",
        "hash": "02ff334f17f17d775b9ecd69046ed296",
        "raw_cluster_num": "16",
    },
    1: {
        "cluster": "GPSC29",
        "hash": "9c00583e2f24fed5e3c6baa87a4bfa4c",
        "raw_cluster_num": "29",
    },
    2: {
        "cluster": "GPSC8",
        "hash": "99965c83b1839b25c3c27bd2910da00a",
        "raw_cluster_num": "8",
    },
}

name_mapping = {
    "02ff334f17f17d775b9ecd69046ed296": "name1.fa",
    "9c00583e2f24fed5e3c6baa87a4bfa4c": "name2.fa",
}

ref_db_fs = DatabaseFileStore(
    "./storage/dbs/GPS_v9_ref", "GPS_v9_external_clusters.csv"
)

args = get_args()
species = "Streptococcus pneumoniae"
species_db_name = "GPS_v9_ref"
output_folder = "./tests/results/poppunk_output/"


def do_assign_clusters(p_hash: str):
    # setup output directory
    fs.setup_output_directory(p_hash)
    # setup metadata csv file for microreact
    pd.DataFrame(amr_for_metadata_csv).to_csv(
        fs.tmp_output_metadata(p_hash), index=False
    )
    hashes_list = [
        "02ff334f17f17d775b9ecd69046ed296",
        "9c00583e2f24fed5e3c6baa87a4bfa4c",
        "99965c83b1839b25c3c27bd2910da00a",
    ]

    return assignClusters.get_clusters(
        hashes_list, p_hash, fs, ref_db_fs, ref_db_fs, args, species
    )


amr_for_metadata_csv = [
    {
        "ID": "02ff334f17f17d775b9ecd69046ed296",
        "Penicillin Resistance": "Highly unlikely",
        "Chloramphenicol Resistance": "Unsure",
        "Erythromycin Resistance": "Highly unlikely",
        "Tetracycline Resistance": "Almost certainly",
        "Cotrim Resistance": "Highly likely",
    },
    {
        "ID": "9c00583e2f24fed5e3c6baa87a4bfa4c",
        "Penicillin Resistance": "Highly unlikely",
        "Chloramphenicol Resistance": "Highly unlikely",
        "Erythromycin Resistance": "Highly unlikely",
        "Tetracycline Resistance": "Highly unlikely",
        "Cotrim Resistance": "Unlikely",
    },
]

output_folder = "./tests/results/poppunk_output/"
