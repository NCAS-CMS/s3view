from s3v.drs_view import drs_view, drs_pretty

def test_drsview():

    data = [
        'wa_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1995-12-01T0600_N120.nc',
        'zg500_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1995-09-01T0600_N120.nc',
        'zg500_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1996-01-01T0600_N120.nc'
    ]
    drs = 'Variable,Source,Experiment,Variant,Frequency,Period,nField'
    print()
    output = drs_view(data, drs, collapse='Period')
    
    for line in output:
        print(line)