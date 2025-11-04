from cfs3.drs_view import drs_view, drs_pretty, drs_select
import re

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

DATA = [
        {'n':'wa_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1995-12-01T0600_N120.nc'},
        {'n':'zg500_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1995-09-01T0600_N120.nc'},
        {'n':'zg500_HadGA7EA-N1280_highresSST-present_r1i1p1f1_6hrPt_1996-01-01T0600_N120.nc'}
    ]

DRS = 'Variable,Source,Experiment,Variant,Frequency,Period,nField'

def strip_ansi(s: str) -> str:
    """Remove ANSI escape codes from a string."""
    return ANSI_ESCAPE.sub('', s)

def helper(lines):
    result = {}
    for raw in lines:
        line = strip_ansi(raw)
        if ':' not in line:
            continue
        key, val = line.split(':', 1)
        key = key.strip()
        val = val.strip()

        # Try to safely evaluate Python-like lists
        try:
            parsed_val = ast.literal_eval(val)
        except Exception:
            parsed_val = val  # fallback: leave as string

        result[key] = parsed_val
    return result


def test_drsview():

    data = [f['n'] for f in DATA]
    output = drs_view(data, DRS, collapse='Period')
    
    print()
    for line in output:
        print(line)

    results = helper(output)
    assert list(results.keys()) == DRS.split(',')


def test_drs_select():

    selections = {'Variable':'wa'}
    output,skipped = drs_select(DATA, selections, DRS)
    assert len(output) == 1
    assert output[0] == DATA[0]