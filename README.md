![Python application CI](https://github.com/bacpop/beebop_py/actions/workflows/python-app.yml/badge.svg)

# beebop_py
## Python API for beebop

<!-- TODO: update readme!!! remove all references to v8 also -->
#### A note on Assign Cluster Quality Control
The app assigns clusters with quality control (qc) on. This is to enable the `--run-qc` flag as per [here](https://poppunk.bacpop.org/qc.html).
The arguments along with `--qc-run` can be found at `args.json` in the `qc_dict` json object. The values are [defaults](https://github.com/bacpop/PopPUNK/blob/master/PopPUNK/__main__.py) from PopPunk and are associated with the V8 database.
### Usage

#### Clone the repository
```
git clone git@github.com:bacpop/beebop_py.git
```
#### Get the database

You will need the GPS_v8 database, please download and extract it into `/storage`:

```
./scripts/download_db
```

#### Install dependencies
##### Poetry
You will need Python installed, as well as [Poetry](https://python-poetry.org/), which you can get on Linux with 
```
curl -sSL https://install.python-poetry.org | python3 -
```

##### PopPUNK
To install PopPUNK, follow these steps:


First, create a new conda environment: `conda create --name beebop_py python=3.9` and activate it with `conda activate beebop_pyonda activate beebop_py`


Then install PopPUNK to your computer - either the latest version:
```
pip3 install git+https://github.com/bacpop/PopPUNK#egg=PopPUNK
```

..or a specific version:
```
pip3 install git+https://github.com/bacpop/PopPUNK@v2.6.7#egg=PopPUNK  # for PopPUNK v2.6.7. replace with desired version
```

If there are problems installing PopPUNK, you may need to install one or more of the following packages with `sudo apt get install`:
- cmake
- libhdf5-dev
- libeigen3-dev
- libopenblas-dev

Please install the dependencies for PopPUNK with:
```
conda install requests pandas networkx pp-sketchlib scikit-learn hdbscan biopython tqdm treeswift rapidnj 
```

The `graph-tool` and `mandrake` packages must be installed from `conda-forge`:
```
conda install -c conda-forge graph-tool mandrake
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
./scripts/run_test_dependencies
```
Testing can be done in a second terminal (make sure to activate 'beebop_py') by running 
```
TESTING=True poetry run pytest
```
