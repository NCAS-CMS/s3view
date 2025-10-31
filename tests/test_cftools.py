from s3v.cftools import CFSplitter
import logging
import os
import cf
from pathlib import Path

def test_cfsplitter(sample_netcdf, tmp_path, caplog):

    # for reasons none of me, Claude, or ChatGPT understand, no 
    # amount of mucking around in pytest.ini, or fixtuers, makes this 
    # logging work automagically, it needs to be explicit.

    caplog.set_level(logging.DEBUG)

    output_dir = tmp_path
    cfs = CFSplitter(output_folder=output_dir)

    cfs.split_one(sample_netcdf)

    files = list(Path(output_dir).glob('*.nc'))
    assert len(files) == 3
    for f in files:
        flds = cf.read(f)
        print(flds)
        if not f.stem.startswith('test'):
            if f.stem.startswith('press'):
                assert flds[0].standard_name=='air_pressure'
            assert flds[0].nc_dataset_chunksizes()==(4,362,362)
            

