import os
import time
import uuid
import pytest
import docker
import json
import tempfile
from minio import Minio
from minio.error import S3Error
from requests.exceptions import ConnectionError
import subprocess
from .utils.make_test_data import make_test_netcdf_with_coords
import logging


MINIO_IMAGE = "quay.io/minio/minio:latest"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"


def _cleanup_old_minio():
    """Remove any leftover MinIO containers from previous runs."""
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", "name=minio-test", "--format", "{{.ID}}"],
            check=False,
            capture_output=True,
            text=True,
        )
        ids = result.stdout.strip().splitlines()
        if ids:
            subprocess.run(["docker", "rm", "-f"] + ids, check=False)
    except Exception as e:
        print(f"Warning: cleanup failed: {e}")

@pytest.fixture(autouse=True)
def silence_noisy_loggers():
    """Silence chatty third-party loggers"""
    logging.getLogger('cfdm').setLevel(logging.WARNING)
    logging.getLogger('cfdm.read_write.netcdf.netcdfwrite').setLevel(logging.WARNING)

@pytest.fixture(scope="session")
def minio_service():
    """Run a temporary MinIO server in Docker and return a configured client."""
    _cleanup_old_minio()

    client = docker.from_env()

    container = client.containers.run(
        MINIO_IMAGE,
        command=["server", "/data", "--console-address", ":9001"],
        environment={
            "MINIO_ROOT_USER": ACCESS_KEY,
            "MINIO_ROOT_PASSWORD": SECRET_KEY,
        },
        ports={"9000/tcp": 9000, "9001/tcp": 9001},
        detach=True,
        remove=True,
        name=f"minio-test-{uuid.uuid4()}",
    )

    # Wait until MinIO is ready
    endpoint = "localhost:9000"
    minio_client = Minio(endpoint, ACCESS_KEY, SECRET_KEY, secure=False)

    for _ in range(30):  # up to ~15 seconds
        try:
            minio_client.list_buckets()
            break
        except ConnectionError:
            time.sleep(0.5)
        except Exception:
            time.sleep(0.5)
    else:
        logs = container.logs().decode()
        container.stop()
        pytest.fail(f"MinIO did not start in time. Logs:\n{logs}")

    yield minio_client

    # Teardown
    container.stop()

@pytest.fixture
def temp_bucket(minio_service):
    """Create a temporary unique bucket for each test."""
    bucket_name = f"test-{uuid.uuid4()}"
    minio_service.make_bucket(bucket_name)
    yield bucket_name
    # Cleanup
    try:
        for obj in minio_service.list_objects(bucket_name, recursive=True):
            minio_service.remove_object(bucket_name, obj.object_name)
        minio_service.remove_bucket(bucket_name)
    except S3Error:
        pass

@pytest.fixture
def fake_mc_config(monkeypatch):
    """Simulate a valid ~/.mc/config.json for s3core.get_user_config()."""
    cfg = {
        "version": "10",
        "aliases": {
            "fake-alias": {
                "url": "http://localhost:9000",
                "accessKey": "minioadmin",
                "secretKey": "minioadmin",
                "api": "S3v4",
                "lookup": "auto"
            }
        }
    }

    tmpdir = tempfile.mkdtemp(prefix="mc_cfg_")
    cfg_file = os.path.join(tmpdir, "config.json")
    with open(cfg_file, "w") as f:
        json.dump(cfg, f)

    # Monkeypatch the HOME so s3core looks in tmpdir/.mc/config.json
    mc_dir = os.path.join(tmpdir, ".mc")
    os.makedirs(mc_dir, exist_ok=True)
    new_path = os.path.join(mc_dir, "config.json")
    os.rename(cfg_file, new_path)
    monkeypatch.setenv("HOME", tmpdir)

    yield new_path

@pytest.fixture
def sample_netcdf(tmp_path):
    return make_test_netcdf_with_coords(tmp_path)