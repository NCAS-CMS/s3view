import pytest
from unittest.mock import MagicMock
from cfs3.s3core import get_client, get_locations
from cfs3.s3cmd import s3cmd
import time
import json
import cmd2
import io


cmd2.ansi.allow_style = cmd2.ansi.AllowStyle.NEVER

dummy_config = '{"aliases":{"loc1":{"url":"https://blah.com","accessKey":"a key","secretKey":"b key","api":"S3v4"}}}'

# Fixture to initialize cfs3 class with mocked dependencies
@pytest.fixture
def mock_cfs3(mocker):
    mocker.patch('cfs3.s3cmd.get_client')
    mocker.patch('cfs3.s3cmd.get_locations', 
                    return_value=json.loads(dummy_config)['aliases']['loc1'])
    app = s3cmd(path='loc1')
    app.client = MagicMock()
    app.client.list_buckets.return_value = [MagicMock(name='bucket1'), MagicMock(name='bucket2')]
    app.client.list_objects.return_value = [MagicMock(name='file1', size=2^16, isdir=False, last_modified=time.time()), 
                                            MagicMock(name='file2',size=2^8, isdir=False, last_modified=time.time()-86400)]
 
    
    # Redirect stdout/stderr to StringIO to prevent cmd2 errors
    app.stdout = io.StringIO()
    app.stderr = io.StringIO()
    
    return app


# Example test for the initialization
def test_cfs3_init(mock_cfs3):
    assert mock_cfs3.alias == 'loc1'
    assert mock_cfs3.bucket is None
    assert mock_cfs3.path is None
    # Add more assertions based on the expected initial state of cfs3

# Test for the do_cb method
def test_do_cb_changes_bucket(capsys, mock_cfs3):
    
    # Assume 'bucket1' is a valid bucket name from my mocked list of buckets

    new_bucket = 'bucket1'
    
    # Call the method under test
    mock_cfs3.do_cb(new_bucket)
    captured = capsys.readouterr()

    print(captured.out)
    
