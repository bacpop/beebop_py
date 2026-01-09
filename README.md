![Python application CI](https://github.com/bacpop/beebop_py/actions/workflows/python-app.yml/badge.svg)

# beebop_py
## Python API for beebop

### Usage

#### Databases

To update the database being used for a species, update the `dbname` json property in `args.json`.
Note: for development you may prefer to use the *ref* database to speed up PopPUNK runs.

#### Adding a new species

The current species can be seen in `args.json` in the `species` object. To add a new species do the following:

1. Add new database to [mrcdata](https://mrcdata.dide.ic.ac.uk/beebop).
2. Add new species to `args.json` in *beebop_py* with properties.

#### A note on Assign Cluster Quality Control

The app assigns clusters with quality control (qc) on. This is to enable the `--run-qc` flag as per [here](https://poppunk.bacpop.org/qc.html).
The arguments along with `--qc-run` can be found at `args.json` in the `qc_dict` json object. These are species dependent and can be changed as per the species requirements.

#### Clone the repository

```
git clone git@github.com:bacpop/beebop_py.git
```

#### Get the database

You will need species databases, please download and extract it into `/storage`. You can download all databases with the following command:

```
./scripts/download_databases
```

or just the reference databases with:

```
./scripts/download_databases --refs
```

#### Install dependencies

##### Poetry

You will need Python installed, as well as [Poetry](https://python-poetry.org/), which you can get on Linux with
```
curl -sSL https://install.python-poetry.org | python3 -
```

##### PopPUNK

To install PopPUNK, follow these steps:


First, create a new conda environment: `conda create --name beebop_py python=3.10` and activate it with `conda activate beebop_py`


Then install PopPUNK to your computer - either the latest version:
```
pip3 install git+https://github.com/bacpop/PopPUNK#egg=PopPUNK
```

..or a specific version:
```
pip3 install git+https://github.com/bacpop/PopPUNK@v2.7.5#egg=PopPUNK  # for PopPUNK v2.7.5. replace with desired version
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

  ```bash
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

### Linting and Formatting

To run the linter and formatter, you can use the following commands:

- **Linting**:
  ```
  poetry run ruff check
  ```
- **Linting with Fix**:
  ```
  poetry run ruff check --fix
  ```
- **Formatting**:
  ```
  poetry run ruff format
  ```

### Diagrams

- There is a .drawio graph in the `diagrams` folder illustrating the process of running a analysis. This includes
all the files created and how they are used in each job. You can open and view the diagram at [draw.io](https://draw.io).

## Use/Deploy specific version of PopPUNK

To use a specific version, commit or branch of PopPUNK in a beebop_py deployment, you can update `POPPUNK_VERSION` in `common`.

The new dev images built with `/docker/build --with-dev` will have a *-dev* postfix.

### Local Development

You can build the image with `/docker/build --with-dev`, this new image can now be used by Beebop.

### Deployment

A pull request can be created so GHA pushes the images to the docker hub. Add `--with-dev` to the build & push commands `build_and_push.yaml`.
**Ensure to remove the `--with-dev` flag before merging the PR.**
Then on the `beebop-deploy` the api image can be updated with the new dev image.

### GPU

To use GPU, ensure the following conditions are met:

- The host machine has a GPU and the NVIDIA drivers and cuda-toolkit are installed with correct versions. [Check the NVIDIA documentation](https://docs.nvidia.com/cuda/cuda-installation-guide-linux)
- The necessary libraries for GPU support are installed in your environment.[RAPIDS](https://rapids.ai/),[numba](https://numba.readthedocs.io/en/stable/user/installing.html), [cupy](https://docs.cupy.dev/en/stable/install.html)
- Ensure any new PopPUNK databases have *graph.csv.gz* file. If not run script in scripts folder: `python gt-to-csv-gz.py` with
  - `--input` the path to the graph .gt file

**Note:** If you installed **pp-sketchlib, PopPUNK, and mandrake** before installing CUDA, you will need to reinstall them to ensure CUDA-enabled versions are installed.

### Metadata

Species databases can include metadata for reference samples. This metadata enhances visualizations and enables location-based mapping.

#### Metadata Files

Metadata is stored in the `beebop/resources` folder as CSV files named `<species_database>_metadata.csv`. These files are used during visualization (passed as the `info_csv` parameter) to add metadata to reference samples in Microreact visualizations.

#### Location Data

The script `metadata_csv-to-location_json.py` converts metadata CSV files to JSON format containing location data. This JSON is returned from the `/speciesConfig` endpoint as the `locationMetadata` property, which the frontend uses to plot reference sample locations on a map.

#### Example: Pneumoniae Database

For the pneumoniae database (GPS), metadata was obtained from the [Sanger GPS Project](https://data-viewer.monocle.sanger.ac.uk/project/gps).

**Note:** Not all species databases have metadata available. Contact @nickjcroucher for availability.