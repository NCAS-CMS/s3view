from cfs3.skin import _i, _e
from cfs3.s3core import Capturing

def __get_collapsed(collapse, drs=None):
    """ 
    Go from user string to usable list. 
    Expecting a commma seperated string with each element in the DRS (if there is one)
    """
    collapsed = []

    if collapse != '':
        try:
            collapsed = collapse.split(',')
        except Exception:
            raise ValueError(f'Collapse option - {collapsed} - is not a python list in a string!')
        
    if drs:
        for x in collapsed: 
            if x not in drs:
                raise ValueError(f'Cannot collapse non-existent{x} from {drs}')
        
    return collapsed


def parse_filename_to_drs_components(filename, drs=None) -> dict:
    """
    Return dictionary of filenanme components or raise a ValueError
    if the filename doesn't match the DRS.

    If DRS is none, then simply return an enumeration
    of parts as a dictionary.

    """
    parts = filename.split('.')[0].split('_')
    if drs is None:
        return {i:p for i,p in enumerate(parts)}
    else:
        if len(parts) != len(drs):
            raise ValueError('Filename does not match DRS')
        else:
            return {k:p for k,p in zip(drs,parts)}



def drs_view(myfiles, drs, selects={}, collapse=''):
    """ 
    Provide a lightweight view of the contents of a directory.
    
    The input is a list of filenames, and a list which outlines the 
    structure which the filenames are expect to use. The filename
    should map onto that structure with each component separated
    by an underscore ("_"). 
    
    Lists DRS contents of files which match the pattern, and
    then lists any others.

    Output is captured and returned.
    """

    try:
        drs = drs.split(',')
    except Exception:
        raise ValueError(f'DRS provided - {drs} - is not a comma seperated string!')

    contents = {k:[] for k in drs}
    content_length = {k:0 for k in drs}
    skipped = []
    
    collapsed = __get_collapsed(collapse, drs)

    for f in myfiles:
        try:
            parsed = parse_filename_to_drs_components(f,drs)
            for k,p in parsed.items():
                if p not in contents[k]:
                    contents[k].append(p)
                    content_length[k]+=1
        except ValueError:
            skipped.append(f)

    return drs_process(contents, collapsed, skipped)
   

def drs_metaview(metadata, selects={}, collapse='[]'):
    """
    Provide a drs-like view of the metadata associated with files.
    This version is assumes the input metadata is a set of
    (filename, metadata_dictionary) pairs and that the 
    metadtaa are key-value pairs.

    It extracts all the unique values for each key, and presents a 
    list of the values.
    """

    collapsed = __get_collapsed(collapse)

    contents = {}
    for f,m in metadata:
        for k,v in m.items():
            if k not in contents:
                contents[k]=[v]
            if v not in contents[k]:
                contents[k].append(v)

    return drs_process(contents, collapsed, skipped)


def drs_process(contents, collapsed, skipped):
    results = {}
    for k in contents:
        if k in collapsed and len(contents[k]) > 2:
            content = sorted(contents[k])
            results[k] = f'[{content[0]} ... {content[-1]}] (len={len(content)})'
        else: 
            results[k] = sorted(contents[k])
    return drs_pretty(results, skipped)
            

def drs_pretty(processed_drs, skipped):
    with Capturing() as output:
        for k,v in processed_drs.items():
            print(f'{_i(k)} : {_e(v)}')
        if skipped: 
            print(_e('Skipped the following files (no DRS match):'))
            for f in skipped:
                print(f)
    return output

def drs_select(files, selections, drs):
    """ Process files for DRS matches to selection """
    results, skipped = [], []
    drsc = drs.split(',')
    for f in files:
        try:
            parsed = parse_filename_to_drs_components(f['n'], drsc)
            if all(parsed.get(k) == v for k,v in selections.items()):
                results.append(f)
        except ValueError:
            skipped.append(f)
    return results, skipped
    


                
            
        

    
    


