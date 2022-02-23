import json

from beebop import __version__


def get_version(components):
    """
    report version numbers for all components requested

    input : list with components like beebop (also poppunk & ska in the future)

    output : json according to 'version' schema
    """

    arr = []
    for x in components:
        entry = {
            "name": x,
            "version": __version__  # needs to be adapted if including other components
        }
        arr.append(entry)

    result = json.dumps(arr)
    return result
