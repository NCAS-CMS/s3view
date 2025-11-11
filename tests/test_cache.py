import pytest
import time
from unittest.mock import MagicMock
from cfs3.s3cache import PersistentCachedMinio, CacheMode, CachedObject

@pytest.fixture
def mock_minio():
    mock = MagicMock()
    mock.stat_object.return_value = MagicMock(
        object_name="test.txt",
        size=100,
        last_modified=None,
        etag="12345",
        metadata={"Content-Type": "text/plain"},
    )
    mock.get_object_tags.return_value = MagicMock(to_dict=lambda: {"tag1": "value"})
    mock.list_objects.return_value = [
        MagicMock(object_name="file1.txt"),
        MagicMock(object_name="file2.txt"),
    ]
    return mock

@pytest.fixture
def cached_client(mock_minio):
    return PersistentCachedMinio(mock_minio, db_path=":memory:", ttl=60)

def test_stat_object_caches_response(cached_client, mock_minio):
    result = cached_client.stat_object("bucket", "test.txt")
    assert isinstance(result, CachedObject)
    assert result.cached is True
    mock_minio.stat_object.assert_called_once()

def test_stat_object_uses_cache(cached_client, mock_minio):
    cached_client.stat_object("bucket", "test.txt")
    # Second call should not hit S3
    cached_client.stat_object("bucket", "test.txt")
    mock_minio.stat_object.assert_called_once()

def test_bypass_cache(cached_client, mock_minio):
    cached_client.stat_object("bucket", "test.txt")
    cached_client.stat_object("bucket", "test.txt", cache_mode=CacheMode.BYPASS)
    assert mock_minio.stat_object.call_count == 2

def test_cache_only_empty(cached_client):
    result = cached_client.stat_object("bucket", "missing.txt", cache_mode=CacheMode.CACHE_ONLY)
    assert result.obj is None
    assert result.cached is False

def test_list_objects_cache_pagination(cached_client, mock_minio):
    # First call populates cache
    objs = list(cached_client.list_objects("bucket", prefix="", limit=1))
    assert len(objs) == 1
    # Second call should use cache if limit is same
    objs2 = list(cached_client.list_objects("bucket", prefix="", limit=1))
    assert len(objs2) == 1
    assert mock_minio.list_objects.call_count == 1

def test_remove_object_invalidates_cache(cached_client, mock_minio):
    cached_client.stat_object("bucket", "test.txt")
    cached_client.remove_object("bucket", "test.txt")
    row = cached_client._get_row("bucket", "test.txt")
    assert row is None

def test_get_object_tags_caches(cached_client, mock_minio):
    cached_client.stat_object("bucket", "test.txt")
    result = cached_client.get_object_tags("bucket", "test.txt")
    assert result.tags == {"tag1": "value"}
    # Second call should not call S3 again
    cached_client.get_object_tags("bucket", "test.txt")
    mock_minio.get_object_tags.assert_called_once()