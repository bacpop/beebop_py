from types import SimpleNamespace
import json
import subprocess
from pathlib import PurePath


def get_args():
    with open("./beebop/resources/args.json") as a:
        args_json = a.read()
    return json.loads(args_json, object_hook=lambda d: SimpleNamespace(**d))


def get_sample_name(project_hash, cluster, storage_location):
    """
    extract one sample name for a specified cluster, to be used to find
    the right network component file
    """
    path = (f"{storage_location}/poppunk_output/"
            f"{project_hash}/include{cluster}.txt")
    try:
        with open(path, "r") as file:
            sample_name = file.readline()
        return sample_name.rstrip()
    except (FileNotFoundError):
        raise FileNotFoundError


def find_component_file(project_hash, sample_name, storage_location):
    """
    since network components and poppunk clusters are not matching
    we need to find the right component file by searching for a sample name
    that is included in the required cluster
    """
    file = subprocess.Popen(
        f"grep --exclude=\*.csv --exclude=\*e.graphml -lr './poppunk_output/{project_hash}/network' -e '{sample_name}'",   # noqa
        shell=True,
        stdout=subprocess.PIPE,
        cwd=storage_location
    ).stdout.read().decode("utf-8")

    if (file.strip() == ''):
        raise FileNotFoundError
    return f"./{PurePath(storage_location, file)}".rstrip()
