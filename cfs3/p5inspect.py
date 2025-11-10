from pyfive.inspect import p5ncdump
from cfs3.s3core import get_user_config, Capturing
import s3fs

def p5view(alias, bucket, path, object, special=False):
    """ 
    Approximate the use of ncdump -h on the object at path in bucket
    """
    MB = 2**20
    credentials = get_user_config(alias)
    storage_options = {
                'key':credentials['accessKey'],
                'secret':credentials['secretKey'],  
                'endpoint_url':credentials['url'],
                'default_cache_type':'readahead',
                'default_block_size': 1 * MB 
    }
    if path == '' or path=='/':
        bits = [bucket,object]
    else:
        bits = [bucket,path,object]

    file_uri = '/'.join(bits)

    fs = s3fs.S3FileSystem(**storage_options)

    with Capturing() as output:
        if True:
            with fs.open(file_uri) as s3file:
                p5ncdump(s3file, special=True)
        if special:
            output.append('(Note that support for the special option is not yet implemented.)')
    return output
