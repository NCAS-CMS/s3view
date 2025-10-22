from io import StringIO 
import sys
from time import time
e1 = time()
import cfdm as cf
ed = time()-e1
print(f'CFDM import {ed:.2}s')
from s3v.s3core import get_user_config
import pyfive
import s3fs

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

def cfread(alias, bucket, path, object, short=False, complete=False):
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
        flist = cf.read(fpath,cache=complete, storage_options=storage_options)
        if complete:
            for f in flist:
                print(f.dump())
        elif short:
            print(flist)
        else:
            for f in flist:
                print(f)
    return flist, output

def pyread(alias, bucket, path, object, short=False, complete=False):
    """ 
    Read and lazy load fields from a particular path via S3
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

    file_uri = '/'.join(bits)

    fs = s3fs.S3FileSystem(**storage_options)
    with Capturing() as output:
        with fs.open(file_uri) as s3file:
            pfile = pyfive.File(s3file)
            keys = pfile.keys()
            for k in keys:
                v = pfile[k]
                if 'DIMENSION_LIST' in v.attrs:
                    print('---')
                    print(v, v.shape, v.attrs)
    return pfile, output

    
def test_s3(cf=True):
    alias = 'hpos'
    bucket ='bnl'
    path = ''
    object = 'common_cl_a.nc'
    if cf:
        flist, output = cfread(alias, bucket, path, object)
    else:
        flist, output = pyread(alias, bucket, path, object)
    for x in output:
        print(x)


if __name__=="__main__":

    import random, os
    # wasn't getting randomness without this:
    random.seed(os.urandom(16))
    opt = random.choice([True,False])

    e1 = time()
    test_s3(cf=opt)
    e2 = time()
    ea = e2-e1
    print(opt,ea)
    opt = not opt
    test_s3(cf=opt)
    e3 = time()
    eb = e3-e2
    print(opt,eb)
    results = {opt: eb, not opt: ea}
    cft = results[True]
    pyt = results[False]


    print(f'Timings - {opt} cf {cft:.2}s, p5 {pyt:.2}s')