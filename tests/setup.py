import subprocess
import beebop.schemas
from tests import hdf5_to_json
import json

schemas = beebop.schemas.Schema()


def generate_json():
    # generate hdf5 sketch from fasta file using pp-sketchlib
    subprocess.run(
        "poppunk_sketch --sketch --rfile rfile.txt --ref-db pneumo_sample --sketch-size 9984 --cpus 4 --min-k 14 --k-step 3",  # noqa
        shell=True,
        cwd='tests/files'
    )

    # translate hdf5 into json
    filepath = 'tests/files/pneumo_sample.h5'
    sketches_json = json.loads(hdf5_to_json.h5_to_json(filepath))

    return json.dumps(sketches_json)
