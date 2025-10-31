from s3v.cftools import CFSplitter
import logging
import os
import cf
from pathlib import Path
import json

def test_cfsplitter_nc(sample_netcdf, tmp_path, caplog):

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

            
def NOtest_cfsplitter_use_b(sample_netcdf, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)

    output_dir = tmp_path
    cfs = CFSplitter(output_folder=output_dir, use_internal_bmetadata=True)

    cfs.split_one(sample_netcdf)

    files = list(Path(output_dir).glob('*.nc'))
    assert len(files) == 3
    json_files = list(Path(output_dir).glob('*.json'))
    assert len(json_files) == 2
    for ff in json_files:
        with open(ff,'r') as f:
            metadata = json.load(f)
        if str(ff.stem).startswith('temp'):
            assert metadata['standard_name'] == 'air_temperature'
        assert metadata['experiment'] == 'dummy'

def NOtest_cfsplitter_use_drs(sample_netcdf, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)

    DRS = ['experiment','institute','standard_name','freq']


    def drs_eg(f):
        return {'experiment':f[1],'institute':f[2]}
    
    def make_name_eg(f,fld):
        drs = drs_eg(f)
        ncv = fld.nc_get_variable()
        return f"{ncv}_{drs['experiment']}_{drs['institute']}"

    # make sure our test data name is what we think it is
    check_drs_first = drs_eg(sample_netcdf)
    assert check_drs_first['experiment'] == 'dummy'
    assert check_drs_first['institute'] == 'cslewis' 

    # now we can do the actual test
    output_dir = tmp_path
    cfs = CFSplitter(output_folder=output_dir, 
                    use_internal_bmetadata=False,
                    name_generator=make_name_eg,
                    external_metadata=DRS
                    )

    cfs.split_one(sample_netcdf)

    files = list(Path(output_dir).glob('*.nc'))
    assert len(files) == 3
    
    temp = 'temp_dummy_narnia.nc'
    press = 'press_dummy_narnia.nc'
    assert temp in [f.name for f in files]
    assert press in [f.name for f in files]

    check_file = output_dir/temp
    fld = cf.read(check_file)[0]
    properties = fld.properties()
    assert properties['experiment'] == 'cslewis' # not narnia, should have changed
    assert properties['myattribute'] == 'value' # still there

    json_files = list(Path(output_dir).glob('*.json'))
    assert len(json_files) == 2
    for ff in json_files:
        with open(ff,'r') as f:
            metadata = json.load(f)
        if str(ff.stem).startswith('temp'):
            assert metadata['standard_name'] == 'air_temperature'
        assert metadata['experiment'] == 'dummy'