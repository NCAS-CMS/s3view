from s3v.skin import _i, _e

def __get_collapsed(collapse, drs=None):
    """ Go from user string to usable list"""
    collapsed = []

    if collapse != '[]':
        try:
            collapsed = collapse[1:-1].split(',')
        except:
            raise ValueError(f'Collapse option - {collapsed} - is not a python list in a string!')
        
    if drs:
        for x in collapsed: 
            if x not in drs:
                raise ValueError(f'Cannot collapse non-existent{x} from {drs}')
        
    return collapsed

def drs_view(myfiles, drs, collapse='[]'):
    """ 
    Provide a lightweight view of the contents of a directory using a 
    provided DRS. Lists DRS contents of files which match the pattern, and
    then lists any others.

    Currently uses normal print, in case we want to introduce some sort of 
    ordered tree structure.
    """

    try:
        drs = drs[1:-1].split(',')
    except:
        raise ValueError(f'DRS provided - {drs} - is not a python list in a string!')
    contents = {k:[] for k in drs}
    content_length = {k:0 for k in drs}
    skipped = []
    
    collapsed = __get_collapsed(collapse, drs)

    for f in myfiles:
        parts = f.split('.')[0].split('_')  
        if len(parts) == len(drs):
            for k,p in zip(drs,parts):
                if p not in contents[k]:
                    contents[k].append(p)
                    content_length[k]+=1
        else:
            skipped.append(f)
   
    for k in contents:
        if k in collapsed and len(contents[k]) > 2:
            content = sorted(contents[k])
            print(_i(k),':',_e(f'[{content[0]} ... {content[-1]}] (len={len(content)})'))
        else: 
            print(_i(k),':',_e(sorted(contents[k])))
   

    if len(skipped) > 0:
        print('')
        print("The following files did not match the drs structure")
        for f in skipped:
            print(_e(f))

def drs_metaview(metadata, collapse='[]'):
    """Provide a drs-like view of the metadata associated with files"""

    collapsed = __get_collapsed(collapse)

    contents = {}
    for f,m in metadata:
        for k,v in m.items():
            if k not in contents:
                contents[k]=[v]
            if v not in contents[k]:
                contents[k].append(v)

    for k in contents:
        if k in collapsed and len(contents[k]) > 2:
            content = sorted(contents[k])
            print(_i(k),':',_e(f'[{content[0]} ... {content[-1]}] (len={len(content)})'))
        else: 
            print(_i(k),':',_e(sorted(contents[k])))
    
    

if __name__=="__main__":

    data = [
        'wa_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1995-12-01T0600_N120.nc',
        'zg500_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1995-09-01T0600_N120.nc',
        'zg500_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1996-01-01T0600_N120.nc'
    ]
    drs = '[Variable,Source,Experiment,Variant,Frequency,Period,nField]'

    drs_view(data, drs, collapse='[Period]')

