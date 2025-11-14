from pathlib import Path
from io import StringIO
import json
from minio import Minio
from urllib.parse import quote, unquote
import sys
import warnings

def get_locations(config_file=None):
    """ 
    Read config file and find usable locations
    """
    if config_file is None:
        config_file = Path.home()/'.mc/config.json'

    with open(config_file,'r') as jfile:
        jdata = json.load(jfile)
    jd = jdata['aliases']
    locations = {}
    for k,v in jd.items():
        api = v.get('api')
        if api != 'S3v4':
            warnings.warn(f'WARNING: Found unexpected S3 API {api} for {k} in configuration file {config_file}')
        else:
            locations[k]=v
    return locations


def get_user_config(target, config_file=None):
    """
    Obtain credentials from user configuration file
    """
    if config_file is None:
        config_file = Path.home()/'.mc/config.json'
    options = get_locations(config_file)
    try:
        options = get_locations(config_file)
        return options[target]
    except KeyError:

        raise ValueError(f'Minio target [{target}] not found in ~/{config_file}')


def get_client(alias, config_file=None):
    """
    Get Minio client from the configuration alias, and patch the 
    client with that alias name
    """
    credentials = get_user_config(alias, config_file=config_file)
    secure = False
    if credentials['url'].startswith('https'):
        secure = True
    try:
        api = {'endpoint':'url','access_key':'accessKey','secret_key':'secretKey'}
        try:
            kw = {k:credentials[v] for k,v in api.items()}
        except KeyError:
            raise KeyError(f"Cannot find {v} in credentials supplied")
        kw['secure'] = secure
        endpoint = kw['endpoint']
        slashes = endpoint.find('//')
        if slashes > -1:
            kw['endpoint'] = endpoint[slashes+2:]
        client = Minio(**kw) 
    except:
        raise 
    # nasty monkey patch, but I want to carry this around
    client.alias_name = alias
    return client

def lswild(client, bucket, pattern='*', objects=False):
    """ 
    Do an ls on a bucket visible on the minio client which matches pattern
    This isn't quite a perfect glob! So be careful. Also, we're being cunning
    in trying to get the server to try and do some of the matching, at least
    for simple cases.
    If objects is False, return just names, oterwise return the objects
    for later processing
    """
    
    asterix = pattern.find('*')
    
    if asterix > 0:
        prefix = pattern[0:asterix]
        pattern = pattern[asterix:]
    else:
        prefix = None
    
    objects = client.list_objects(bucket, prefix=prefix)
    
    if objects:
        olist = [o for o in objects if Path(o.object_name).match(pattern)]
        return olist
    else:
        object_names = [o.object_name for o in objects]
        flist = [p for p in object_names if Path(p).match(pattern)]
        return flist

def sanitise_metadata(indict):
    """ 
    Given a dictionary sanitise the keys and values for 
    encoding in object store metadata. Note AWS
    encodes keys in lower case, can't do anhyting about
    that.
    """
    outdict = {}
    for k,value in indict.items():
        kk = k.replace(' ', '-').replace('_', '-').lower()
        if isinstance(value,list):
            sanitised_value = "json_"+quote(json.dumps(value))
            outdict[kk] = sanitised_value
        else: 
            outdict[kk]=quote(value)
    return outdict

def desanitise_metadata(indict):
    """ 
    Given a dictionary which has had values which 
    have been encoded for an object store, undo
    that sanitation.
    """
    outdict = {}
    for k,v in indict.items():
        if v.startswith('json_'):
            outdict[k]=json.loads(unquote(v[5:]))
        else:
            outdict[k]=unquote(v)
    return outdict

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
