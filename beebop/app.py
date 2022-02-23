from flask import Flask

from beebop import __version__, versions

app = Flask(__name__)


@app.route('/version')
def report_version():
    """
    report version of beebop (and poppunk,ska in the future)
    """
    vers_json=versions.get_version(["beebop"])
    return vers_json
