import subprocess
import beebop.schemas
from tests import hdf5_to_json
import jsonschema
import json
import re

schemas = beebop.schemas.Schema()


def generate_json():
    # generate hdf5 sketch from fasta file using pp-sketchlib
    subprocess.run(
        "poppunk_sketch --sketch --rfile rfile.txt --ref-db pneumo_sample --sketch-size 9984 --cpus 4 --min-k 14 --k-step 3",  # noqa
        shell=True,
        cwd='tests/files'
    )

    # translate hdf5 into json and validate against schema
    filepath = 'tests/files/pneumo_sample.h5'
    sketches_json = json.loads(hdf5_to_json.h5_to_json(filepath))
    for sketch_name in list(sketches_json.keys()):
        if (type(sketches_json[sketch_name]['14'][0]) == str and
                re.match('0x.*', sketches_json[sketch_name]['14'][0])):
            for x in range(14, 30, 3):
                sketches_json[sketch_name][str(x)] = list(
                    map(lambda x: int(x, 16),
                        sketches_json[sketch_name][str(x)]))

    return json.dumps(sketches_json)
