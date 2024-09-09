import subprocess
import beebop.schemas
from beebop import assignClusters
from beebop import visualise
from tests import hdf5_to_json
import json
from beebop.filestore import PoppunkFileStore, FileStore, DatabaseFileStore
from beebop.utils import get_args

schemas = beebop.schemas.Schema()


def generate_json():
    # generate hdf5 sketch from fasta file using pp-sketchlib
    subprocess.run(
        "sketchlib sketch -l sketchlib_input/rfile.txt -o pneumo_sample -s 9984 --cpus 4 -k 14,29,3",  # noqa
        shell=True,
        cwd='tests/results'
    )

    # translate hdf5 into json
    filepath = 'tests/results/pneumo_sample.h5'
    sketches_json = json.loads(hdf5_to_json.h5_to_json(filepath))

    return json.dumps(sketches_json)


storage_location = './tests/results'
fs = PoppunkFileStore(storage_location)

expected_assign_result = {
     0: {'cluster': 'GPSC16', 'hash': '02ff334f17f17d775b9ecd69046ed296'},
     1: {'cluster': 'GPSC29', 'hash': '9c00583e2f24fed5e3c6baa87a4bfa4c'},
     2: {'cluster': 'GPSC8', 'hash': '99965c83b1839b25c3c27bd2910da00a'}
}

name_mapping = {
    "02ff334f17f17d775b9ecd69046ed296": "name1.fa",
    "9c00583e2f24fed5e3c6baa87a4bfa4c": "name2.fa"
}

db_paths = DatabaseFileStore('./storage/GPS_v9')
args = get_args()


def do_assign_clusters(p_hash: str):
    hashes_list = [
            '02ff334f17f17d775b9ecd69046ed296',
            '9c00583e2f24fed5e3c6baa87a4bfa4c',
            '99965c83b1839b25c3c27bd2910da00a']

    return assignClusters.get_clusters(
        hashes_list,
        p_hash,
        fs,
        db_paths,
        args)


def do_network_internal(p_hash: str):
    do_assign_clusters(p_hash)
    visualise.network_internal(expected_assign_result,
                               p_hash,
                               fs,
                               db_paths,
                               args,
                               name_mapping)
