'''Flask API for beebop'''

from flask import Flask, jsonify

from beebop import __version__, versions

app = Flask(__name__)


@app.route('/version')
def report_version():
    """
    report version of beebop (and poppunk,ska in the future)
    """
    vers = versions.get_version()
    vers_json = jsonify(vers)
    return vers_json
