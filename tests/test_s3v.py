import pytest
from unittest.mock import MagicMock
from s3v.s3core import get_client, get_locations
from s3v.s3cmd import s3cmd
import time
import json

dummy_config = '{"loc1":{"url":"https://blah.com","accessKey":"a key","secretKey":"b key","api":"S3v3"}}'


# Fixture to initialize s3v class with mocked dependencies
@pytest.fixture
def mock_s3v(mocker):
    mocker.patch('s3v.s3core.get_client')
    mocker.patch('s3v.s3core.get_locations', return_value=json.loads(dummy_config))
    app = s3cmd(path='loc1')
    app.client = MagicMock()
    app.client.list_buckets.return_value = [MagicMock(name='bucket1'), MagicMock(name='bucket2')]
    app.client.list_objects.return_value = [MagicMock(name='file1', size=2^16, isdir=False, last_modified=time.time()), 
                                            MagicMock(name='file2',size=2^8, isdir=False, last_modified=time.time()-86400)]
 
    return app

# Example test for the initialization
def test_s3v_init(mock_s3v):
    assert mock_s3v.alias == 'loc1'
    assert mock_s3v.bucket is None
    assert mock_s3v.path is None
    # Add more assertions based on the expected initial state of s3v

# Add more test functions for other methods in the s3v class


# Test for the do_cb method
def test_do_cb_changes_bucket(capsys, mock_s3v):
    
    # Assume 'bucket1' is a valid bucket name from my mocked list of buckets

    new_bucket = 'bucket1'
    
    # Call the method under test
    mock_s3v.do_cb(new_bucket)
    captured = capsys.readstderr()

    print(captured.out)
    