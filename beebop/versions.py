from beebop import __version__ as beebop_version


def get_version():
    """
    report version numbers for all components

    output : json according to 'version' schema
    """
    arr = [{"name": "beebop", "version": beebop_version}]
    return arr
