![Python application CI](https://github.com/bacpop/beebop_py/actions/workflows/python-app.yml/badge.svg)

# beebop_py
## Python API for beebop

### Usage

#### Clone the repository
```
git clone git@github.com:bacpop/beebop_py.git
```
#### Get the database

You will need the GPS_v4 database, please download and extract it into `/storage`:

```
./scripts/download_db --small storage
```

(omit the `--small` to download the full 10GB database, which is more than you'll typically want for development)

#### Install dependencies
##### Poetry
You will need Python installed, as well as [Poetry](https://python-poetry.org/), which you can get on Linux with 
```
curl -sSL https://install.python-poetry.org | python3 -
```

##### PopPUNK
Since PopPUNK is not available via pip yet, it must be installed separately.


First, create a new conda environment: `conda create --name beebop_py` and activate it with `conda activate beebop_py`


Then clone the following branch of poppunk to your computer: `git clone -b fix-json-serialisation git@github.com:bacpop/PopPUNK.git`
Please make sure you have all required dependencies installed as specified [here](https://poppunk.readthedocs.io/en/latest/installation.html#clone-the-code):
```
conda install pybind11 pp-sketchlib dendropy hdbscan matplotlib graph-tool pandas sharedmem cmake tqdm networkx flask_cors flask-apscheduler requests
```


Next, install the package with:
```
python setup.py install
python -m pip install . --no-deps --ignore-installed --no-cache-dir -vvv
```
##### Other dependencies
To install all other required dependencies go into the beebop_py project folder and run (having the conda environment 'beebop_py' activated)
```
poetry install
```
#### Run the app
Start the flask app with
```
./scripts/run_app
```
In a second terminal, you can now query the flask endpoints, e.g.:
- /version
  ```
  curl http://127.0.0.1:5000/version
  ```
### Testing
Before testing, Redis and rqworker must be running. From the root of beebop_py, run (with 'beebop_py' env activated)
```
docker pull redis
docker run --rm -d --name=redis -p 6379:6379 redis
rqworker
```
Testing can be done in a second terminal (make sure to activate 'beebop_py') by running 
```
TESTING=True poetry run pytest
```
