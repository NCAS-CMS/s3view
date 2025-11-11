from cfs3.p5inspect import p5view
from cfs3.s3core import Capturing
from pyfive.inspect import p5ncdump
from cfs3.s3up import Uploader
from pathlib import Path

def test_remote_p5dump(fake_mc_config, temp_bucket):
    
    test_file = 'tests/data/common_cl_a_copy.nc'
    objName = Path(test_file).name

    with Capturing() as local_output:
        p5ncdump(test_file)

    print(local_output[0:5])
 
    alias = 'fake-alias'
    upper = Uploader(alias)
    upper.upload_file(test_file, bucket=temp_bucket,object_name=objName)

    path = ''
    remote_output = p5view(alias, temp_bucket, path, objName)    

    print(remote_output[0:5])

    for i in range(1,5):
        assert remote_output[i].strip() == local_output[i].strip()

    
   
