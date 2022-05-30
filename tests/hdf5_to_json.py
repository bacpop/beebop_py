import h5py
import json
import numpy as np


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.uint64):  # this only applies to attributes
            return int(obj)
        if isinstance(obj, np.ndarray):
            if type(obj[0]) == np.uint64:  # this applies to the datasets
                obj_hex = np.array([hex(x) for x in obj])
                return obj_hex.tolist()
            else:  # this applies to base_freq
                return obj.tolist()
        return super(NpEncoder, self).default(obj)


class MySketch:
    def __init__(self, bases, bbits, codon_phased, length, missing_bases,
                 sketchsize64, sketch_version):
        self.bases = bases
        self.bbits = bbits
        self.codon_phased = codon_phased
        self.densified = False
        self.length = length
        self.missing_bases = missing_bases
        self.sketchsize64 = sketchsize64
        self.version = sketch_version


def h5_to_obj(element, sketch_version, codon_phased):
    sketch = MySketch(
        element.attrs['base_freq'],
        element.attrs['bbits'],
        codon_phased,
        element.attrs['length'],
        element.attrs['missing_bases'],
        element.attrs['sketchsize64'],
        sketch_version)
    for x in list(element.attrs['kmers']):
        setattr(sketch, str(x), element[str(x)][:])
    return sketch


def obj_to_json(sketch, filename=None):
    jsonStr = json.dumps(sketch.__dict__, cls=NpEncoder)
    if filename:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(sketch.__dict__, f, ensure_ascii=False,
                      indent=4, cls=NpEncoder)
    return jsonStr


def h5_to_json(input_file, output_folder=None):
    f = h5py.File(input_file, 'r')
    sketches = f['sketches']
    # extract top level attributes
    sketch_version = sketches.attrs['sketch_version']
    codon_phased = bool(sketches.attrs['codon_phased'])
    # loop through all elements and add them to a dict
    sketches_dict = {}
    for element_name in list(sketches.keys()):
        element = sketches[element_name]
        sketch = h5_to_obj(element, sketch_version, codon_phased)
        sketch_encoded = json.loads(obj_to_json(sketch))
        sketches_dict[element_name] = sketch_encoded
    if output_folder:
        with open(output_folder+'/test_sketches.json', 'w',
                  encoding='utf-8') as f:
            json.dump(sketches_dict, f, indent=4)
    return json.dumps(sketches_dict)