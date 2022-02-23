import json

from beebop import __version__

def get_version(components):
    """
    report version numbers for all components requested

    input : array with required compnents like beebop (& poppunk & ska)

    output:json matching 'version' schema
    """
    
    arr=[]
    for x in components:
        entry={
            "name": x,
            "version": __version__ #needs to be adapted in the future for other components
        }
        arr.append(entry)

    result=json.dumps(arr)
    return result
