from cfs3.cftools import CFSplitter, MetaFix, FileNameFix
import logging
import os
import cf
from pathlib import Path
import json

def test_cfsplitter_simple(sample_netcdf, tmp_path, caplog):

    # for reasons none of me, Claude, or ChatGPT understand, no 
    # amount of mucking around in pytest.ini, or fixtuers, makes this 
    # logging work automagically, it needs to be explicit.

    caplog.set_level(logging.DEBUG)

    output_dir = tmp_path
    cfs = CFSplitter(output_folder=output_dir)

    filebases = cfs.split_one(sample_netcdf)

    assert len(filebases) == 2

    for f in filebases:
        ncf = f.with_suffix('.nc')
        jf = f.with_suffix('.json')
        flds = cf.read(ncf)
        print(flds)
        if f.stem.startswith('press'):
            assert flds[0].standard_name=='air_pressure'
        assert flds[0].nc_dataset_chunksizes()==(4,362,362)
        with open(jf,'r') as ojf:
            metadata = json.load(ojf)
        fset = set(flds[0].properties().keys())
        mset = set(metadata.keys())
        assert fset == mset
        for key in fset:
            assert flds[0].properties()[key] == metadata[key]


def test_cfsplitter_fix_metadata(sample_netcdf, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)

    # add something and fix something
    # external_metadata takes priority over internal metadata unless it is None, in which
    # case we expect to get it from the file 
    external_metadata = {'project':'pytest','experiment':'dummy2','standard_name':None}

    output_dir = tmp_path
    metafix = MetaFix(external_metadata)
    cfs = CFSplitter(meta_handler=metafix, output_folder=output_dir)

    filebases = cfs.split_one(sample_netcdf)

    assert len(filebases) == 2

    for f in filebases:
        ncf = f.with_suffix('.nc')
        jf = f.with_suffix('.json')
        flds = cf.read(ncf)
        print(flds)
        with open(jf,'r') as ojf:
            metadata = json.load(ojf)
        assert 'myattribute' not in metadata.keys()
        assert metadata['standard_name'] == flds[0].standard_name
        properties = flds[0].properties()
        assert metadata['experiment'] == 'dummy2'
        assert properties['experiment'] == 'dummy2'
        assert properties['project'] == 'pytest'
    

def test_cfsplitter_filenames(sample_netcdf, tmp_path, caplog):


    DRS = ['!ncname','experiment','institute','!freq']
    filename_map = ['ignore','experiment','institute','ignore']
    namefix = FileNameFix(DRS, filename_map)

    # now we can do the actual test
    output_dir = tmp_path
    cfs = CFSplitter(filename_handler=namefix, output_folder=output_dir)

    results = cfs.split_one(sample_netcdf)

    files = list(Path(output_dir).glob('*.nc'))
    assert len(files) == 3
    
    print(files)