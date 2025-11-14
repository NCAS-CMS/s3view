import io
import os
import tempfile
import pytest
from pathlib import Path

from cfs3 import s3core
from cfs3.s3up import Uploader


def test_get_client_and_basic_ops(fake_mc_config, minio_service, temp_bucket):
    """Test that get_client can create a MinIO client and perform simple ops."""

    # make sure the mocking has worked
    config_file = Path.home()/'.mc/config.json'
    with open(config_file,'r') as f:
        config = f.read()

    assert 'fake-alias' in config

    # now test get_client
    client = s3core.get_client("fake-alias")
    assert isinstance(client, type(minio_service))

    # Verify bucket exists
    buckets = [b.name for b in client.list_buckets()]
    assert temp_bucket in buckets

    # Upload and read back
    data = b"minio test"
    obj_name = "data/test.txt"
    client.put_object(temp_bucket, obj_name, io.BytesIO(data), len(data))

    downloaded = client.get_object(temp_bucket, obj_name).read()
    assert downloaded == data


def test_uploader_put_and_get(fake_mc_config, minio_service, temp_bucket):
    """Integration test of the Uploader class upload/download cycle."""


    uploader = Uploader(alias="fake-alias", default_bucket=temp_bucket)
    assert uploader.client is not None

    # Create a temporary file
    fd, tmpfile = tempfile.mkstemp(prefix="upload_", suffix=".txt")
    with os.fdopen(fd, "wb") as f:
        f.write(b"Hello MinIO world")

    obj_name = os.path.basename(tmpfile)

    # Perform upload
    uploader.client.fput_object(temp_bucket, obj_name, tmpfile)
    objs = list(uploader.client.list_objects(temp_bucket))
    assert any(o.object_name == obj_name for o in objs)

    # Fetch and check content
    data = uploader.client.get_object(temp_bucket, obj_name).read()
    assert data == b"Hello MinIO world"

    # Cleanup
    uploader.client.remove_object(temp_bucket, obj_name)
    os.remove(tmpfile)


def test_error_handling_invalid_alias(monkeypatch):
    """Check that get_user_config raises properly for missing alias."""
    monkeypatch.setattr(
        s3core,
        "get_locations",
        lambda cfg: {"known": {"api": "Cfs34"}}
    )

    with pytest.raises(ValueError):
        s3core.get_user_config("missing", config_file="dummy")


def test_large_file_upload(fake_mc_config, minio_service, temp_bucket):
    """Test uploading a large file (>5MB) to verify multipart handling."""
  
    uploader = Uploader(alias="fake-alias", default_bucket=temp_bucket)

    # Create a large temp file
    fd, tmpfile = tempfile.mkstemp(prefix="large_", suffix=".bin")
    size = 6 * 1024 * 1024  # 6 MB
    with os.fdopen(fd, "wb") as f:
        f.write(os.urandom(size))

    obj_name = os.path.basename(tmpfile)
    uploader.client.fput_object(temp_bucket, obj_name, tmpfile)
    objs = list(uploader.client.list_objects(temp_bucket))
    assert any(o.object_name == obj_name for o in objs)

    # Clean up
    uploader.client.remove_object(temp_bucket, obj_name)
    os.remove(tmpfile)