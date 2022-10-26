from beebop import __version__ as beebop_version
from PopPUNK import __version__ as poppunk_version


def get_version() -> list:
    """
    [report version numbers for all components]

    :return : [list with jsons according to 'version' schema]
    """
    versions = [{"name": "beebop", "version": beebop_version},
                {"name": "poppunk", "version": poppunk_version}]
    return versions
