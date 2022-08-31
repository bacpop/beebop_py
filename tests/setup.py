import subprocess
import beebop.schemas
from tests import hdf5_to_json
import json

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
