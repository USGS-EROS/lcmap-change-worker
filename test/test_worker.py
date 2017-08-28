from pw import worker
from test import shared
import json

from merlin.support import aardvark, chip_spec_queries, data_config
from merlin import create as pyccd_create


def get_rods(chip, specs_url, specs_fn, chips_url, chips_fn, dates, queries):
    dc       = data_config()
    point    = (dc['x'], dc['y'])
    chipurl  = 'http://localhost'
    specs_fn = aardvark.chip_specs
    chips_fn = aardvark.chips
    acquired = dc['acquired']
    queries  = chip_spec_queries(chipurl)
    return pyccd_create(point=point,
                        chips_url=chipurl,
                        acquired=acquired,
                        queries=queries,
                        chips_fn=chips_fn,
                        specs_fn=specs_fn,
                        dates_fn=dates_fn)[:1]


def test_dtstr_to_ordinal():
    # test function for converting datestring to ordinal
    assert worker.dtstr_to_ordinal('2011-04-27T12:31:16Z') == 734254


def test_run(monkeypatch):
    monkeypatch.setattr('pw.worker.pyccd_rods', get_rods)
    msg = shared.good_input_data
    resp = list(worker.run(msg))[0]
    result_d = json.loads(resp['result'])
    assert isinstance(resp, dict)
    for k in {'procedure', 'processing_mask', 'algorithm', 'change_models'}:
        assert k in result_d
    assert isinstance(result_d['change_models'][0]['start_day'], int)
    assert isinstance(result_d['change_models'][0]['curve_qa'], int)
