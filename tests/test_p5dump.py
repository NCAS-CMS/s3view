from s3v.p5inspect import test_s3, p5view
from s3v.s3core import Capturing
from pyfive.inspect import p5ncdump

def test_s3dumping():
    test_file = 'common_cl_a_copy.nc'

    with Capturing() as local_output:
        p5ncdump(test_file)
     
    output = test_s3()

    print(output[0:5])
    print(local_output[0:5])

    assert output[1:] == local_output[1:]


