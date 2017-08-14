import json
from pw import worker
import glob
import numpy as np
import xarray as xr
from . import shared

from merlin.support import aardvark, chip_spec_queries, data_config
from merlin.timeseries import pyccd as pyccd_rods

# some constants
band = 'blue'
ubid = 'LANDSAT_7/ETM/sr_band1'
specs_url = 'http://localhost/landsat/tile-specs'
chips_json = 'data/tiles/band-json/{}_-1821585_2891595_{}.json'.format(band, ubid.replace('/', '-'))


def get_rods():
    dc       = data_config()
    point    = (dc['x'], dc['y'])
    specurl  = 'http://localhost'
    chipurl  = 'http://localhost'
    specs_fn = aardvark.chip_specs
    chips_fn = aardvark.chips
    acquired = dc['acquired']
    queries  = chip_spec_queries(chipurl)
    return pyccd_rods(point, specurl, specs_fn, chipurl, chips_fn, acquired, queries)


def test_dtstr_to_ordinal():
    # test function for converting datestring to ordinal
    assert worker.dtstr_to_ordinal('2011-04-27T12:31:16Z') == 734254


def test_detect(monkeypatch):
    # actually run ccd.detect
    rods = get_rods()
    resp = worker.detect(rods[0][1])

    assert isinstance(resp, dict)
    assert set(resp.keys()) == {'procedure', 'processing_mask', 'algorithm', 'change_models'}
    assert isinstance(resp['change_models'][0].start_day, np.int64)
    assert isinstance(resp['change_models'][0].curve_qa, int)

