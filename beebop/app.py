from flask import Flask, jsonify
from waitress import serve

from beebop import versions

app = Flask(__name__)


def response_success(data):
    response = {
        "status": "success",
        "errors": [],
        "data": data
    }
    return response


@app.route('/version')
def report_version():
    """
    report version of beebop (and poppunk,ska in the future)
    wrapped in response object
    """
    vers = versions.get_version()
    response = response_success(vers)
    response_json = jsonify(response)
    return response_json


if __name__ == "__main__":
    serve(app)
