import shutil
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from beebop.app import create_app
from beebop.models import ClusteringConfig


@pytest.fixture()
def app():
    app = create_app()
    app.config.update(
        {
            "TESTING": True,
        }
    )

    yield app


@pytest.fixture()
def client(app):
    # Establish an application context before running the tests.
    ctx = app.app_context()
    ctx.push()

    return app.test_client()


@pytest.fixture(scope="session", autouse=True)
def copy_files():
    storageLocation = "./tests/results"
    shutil.copytree("./tests/files", storageLocation, dirs_exist_ok=True)


@pytest.fixture
def sample_clustering_csv(tmp_path):
    # Create data as dictionary
    data = {
        "sample": ["sample1", "sample2", "sample3", "sample4", "sample5"],
        "Cluster": ["10", "309;20;101", "30", "40", None],  # Using None for NA
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Define path and save CSV
    csv_path = tmp_path / "samples.csv"
    df.to_csv(csv_path, index=False)

    return str(csv_path)


@pytest.fixture
def clustering_config():
    return ClusteringConfig(
        "species",
        "p_hash",
        SimpleNamespace(),
        "prefix",
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        "outdir",
    )
