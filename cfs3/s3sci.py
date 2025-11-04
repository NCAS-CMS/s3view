import cf
from cfs3.s3core import get_user_config, Capturing



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
        flist = cf.read(fpath,storage_options=storage_options)
        if complete:
            for f in flist:
                print(f.dump())
        elif short:
            print(flist)
        else:
            for f in flist:
                print(f)
    return flist, output



    
def test_s3():
    alias = 'hpos'
    bucket ='bnl'
    path = ''
    object = 'common_cl_a.nc'
    flist, output = cfread(alias, bucket, path, object)
    for o in output:
        print(o)


if __name__=="__main__":
    test_s3()
