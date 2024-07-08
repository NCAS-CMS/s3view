from io import StringIO 
import sys
import cf
from s3v.s3core import get_user_config

class Capturing(list):
    """ 
    Used to capture output from science functions that have internal print statements.
    Usage for calling my_function:
        with Capturing() as output_list:
            my_function(my_arguments)
    output_list will be a list of output strings
    """
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

def cfread(alias, bucket, path, object):
    """ 
    Read and lazy load cf fields from a particular path via S3
    """
    credentials = get_user_config(alias)
    storage_options = {
                'key':credentials['accessKey'],
                'secret':credentials['secretKey'],  
                'endpoint_url':credentials['url']
    }
    if path == '' or path=='/':
        bits = [bucket,object]
    else:
        bits = [bucket,path,object]

    fstart = credentials['url'].replace('http','s3')+'/'
    fstart = fstart.replace('s3s:','s3:')
    fpath = fstart +'/'.join(bits)
    with Capturing() as output:
        print('going', fpath)
        flist = cf.read(fpath,storage_options=storage_options)
        for f in flist:
            print(f)
    return flist, output

    
def test_s3():
    alias = 'hpos'
    bucket ='bnl'
    path = ''
    object = 'common_cl_a.nc'
    flist, output = cfread(alias, bucket, path, object)
    print(output)


if __name__=="__main__":
    test_s3()
