from beebop import __version__ as beebop_version
from PopPUNK import __version__ as poppunk_version
from PopPUNK.sketchlib import getKmersFromReferenceDatabase


def get_version() -> list:
    """
    [report version numbers for all components]

    :return : [list with jsons according to 'version' schema]
    """

    versions = [
        {"name": "beebop", "version": beebop_version},
        {"name": "poppunk", "version": poppunk_version},
    ]
    return versions


def get_species_kmers(db_path: str) -> dict:
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
