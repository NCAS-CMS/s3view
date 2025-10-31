from s3v.cftools import CFSplitter
import logging
import cf

def test_cfsplitter(sample_netcdf, tmp_path, caplog):

    # for reasons none of me, Claude, or ChatGPT understand, no 
    # amount of mucking around in pytest.ini, or fixtuers, makes this 
    # logging work automagically, it needs to be explicit.

    caplog.set_level(logging.DEBUG)

    cfs = CFSplitter(output_folder=tmp_path)

    cfs.split_one(sample_netcdf)

    files = os.listdir(tmp_path)
    print(files)

