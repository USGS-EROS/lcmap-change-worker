import json
import xarray
import cw
from cw import worker
import pytest
import glob
import numpy as np
import xarray as xr
from . import shared

# some constants
band = 'blue'
ubid = 'LANDSAT_7/ETM/sr_band1'
tile_specs_url = 'http://localhost/landsat/tile-specs'
tiles_json = 'test/resources/band_data/{}_-1821585_2891595_{}.json'.format(band, ubid.replace('/', '-'))


def fetch_json(jsonfile):
    # method for returning json stored in file
    with open(jsonfile) as file:
        return json.loads(file.read())


def mock_get_spectral_request(url):
    # method for mocking spectral requests
    if '?q=' in url:
        spectrum = url.split(':')[-1].replace(')', '')
        return fetch_json('test/resources/spec_data/spec_{}.json'.format(spectrum))
    else:
        return fetch_json('test/resources/spec_data/spec_all.json')


def mock_get_tiles_request(url, params):
    # method for mocking tiles data response
    ubid = params['ubid'].replace('/', '-')
    fname = "test/resources/band_data/*_{}_{}_{}.json".format(params['x'], params['y'], ubid)
    jfile = glob.glob(fname)[0]
    return fetch_json(jfile)


def mock_spectral_map(url):
    # method for mocking spectral map response
    _spec_map = dict()
    for bnd in ('blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'thermal', 'cfmask'):
        _json = fetch_json('test/resources/spec_data/spec_{}.json'.format(bnd))
        _spec_map[bnd] = list(set([i['ubid'] for i in _json]))
    _spec_whole = fetch_json('test/resources/spec_data/spec_all.json')
    return _spec_map, _spec_whole


def test_spectral_map(monkeypatch):
    # test creating spectral map
    monkeypatch.setattr('cw.worker.get_request', mock_get_spectral_request)
    spec_map, spec_whole = worker.spectral_map(tile_specs_url)
    assert set(spec_map.keys()) == shared.spect_map_keys
    assert len(spec_whole) == 75
    for i in spec_whole:
        assert set(i.keys()) == shared.tile_spec_keys


def test_dtstr_to_ordinal():
    # test function for converting datestring to ordinal
    assert worker.dtstr_to_ordinal('2011-04-27T12:31:16Z') == 734254


def test_as_numpy_array(monkeypatch):
    # test converting landsat dataset into numpy array
    monkeypatch.setattr('cw.worker.get_request', mock_get_spectral_request)
    _map, spec_whole = worker.spectral_map(tile_specs_url)
    tiles = fetch_json(tiles_json)

    uniq_specs = []
    for spec in spec_whole:
        if spec not in uniq_specs:
            uniq_specs.append(spec)

    specs_map = dict([[spec['ubid'], spec] for spec in uniq_specs if spec['ubid'] == ubid])
    resp = worker.as_numpy_array(tiles[0], specs_map)

    assert isinstance(resp, np.ndarray)
    assert len(resp) == 100


def test_landsat_dataset(monkeypatch):
    # test assembling landsat data
    monkeypatch.setattr('cw.worker.get_request', mock_get_spectral_request)
    _map, spec_whole = worker.spectral_map(tile_specs_url)
    tiles = fetch_json(tiles_json)

    resp = worker.landsat_dataset(band, ubid, spec_whole, tiles)
    assert isinstance(resp, xr.Dataset)
    assert len(resp[band]) == 631
    for k in ('source', 'acquired', 'ordinal', 't'):
        assert k in resp.coords.keys()


def test_rainbow(monkeypatch):
    # test assemble data for ccd
    monkeypatch.setattr('cw.worker.spectral_map', mock_spectral_map)
    monkeypatch.setattr('cw.worker.get_request', mock_get_tiles_request)

    msg = shared.good_input_data
    dates = [i.split('=')[1] for i in msg['inputs_url'].split('&') if 'acquired=' in i][0]
    tile_x = msg['tile_x']
    tile_y = msg['tile_y']
    tiles_url = msg['inputs_url'].split('?')[0]
    specs_url = tiles_url.replace('/tiles', '/tile-specs')

    querystr_list = msg['inputs_url'].split('?')[1].split('&')
    requested_ubids = [i.replace('ubid=', '') for i in querystr_list if 'ubid=' in i]

    resp = worker.rainbow(tile_x, tile_y, dates, specs_url, tiles_url, requested_ubids)
    assert isinstance(resp, xr.Dataset)
    assert len(resp[band]) == 1501
    for k in ('source', 'acquired', 'ordinal', 't'):
        assert k in resp.coords.keys()


def test_detect(monkeypatch):
    # actually run ccd.detect
    monkeypatch.setattr('cw.worker.spectral_map', mock_spectral_map)
    monkeypatch.setattr('cw.worker.get_request', mock_get_tiles_request)

    msg = shared.good_input_data
    dates = [i.split('=')[1] for i in msg['inputs_url'].split('&') if 'acquired=' in i][0]
    tile_x = msg['tile_x']
    tile_y = msg['tile_y']
    tiles_url = msg['inputs_url'].split('?')[0]
    specs_url = tiles_url.replace('/tiles', '/tile-specs')

    querystr_list = msg['inputs_url'].split('?')[1].split('&')
    requested_ubids = [i.replace('ubid=', '') for i in querystr_list if 'ubid=' in i]

    rainbow = worker.rainbow(tile_x, tile_y, dates, specs_url, tiles_url, requested_ubids)
    resp = worker.detect(rainbow, x=54, y=39)

    assert isinstance(resp, dict)
    assert set(resp.keys()) == {'procedure', 'processing_mask', 'algorithm', 'change_models'}
    assert resp['change_models'][0].start_day == 724134.0
    assert resp['change_models'][0].num_coefficients == 8


def test_simplify_detect_results(monkeypatch):
    # also tests the simplify_objects function
    monkeypatch.setattr('cw.worker.spectral_map', mock_spectral_map)
    monkeypatch.setattr('cw.worker.get_request', mock_get_tiles_request)

    msg = shared.good_input_data
    dates = [i.split('=')[1] for i in msg['inputs_url'].split('&') if 'acquired=' in i][0]
    tile_x = msg['tile_x']
    tile_y = msg['tile_y']
    tiles_url = msg['inputs_url'].split('?')[0]
    specs_url = tiles_url.replace('/tiles', '/tile-specs')

    querystr_list = msg['inputs_url'].split('?')[1].split('&')
    requested_ubids = [i.replace('ubid=', '') for i in querystr_list if 'ubid=' in i]

    rainbow = worker.rainbow(tile_x, tile_y, dates, specs_url, tiles_url, requested_ubids)
    dtect = worker.detect(rainbow, x=54, y=39)

    resp = worker.simplify_detect_results(dtect)
    assert set(resp.keys()) == set(shared.simplified_detect_results.keys())


def test_run(monkeypatch):
    monkeypatch.setattr('cw.worker.spectral_map', mock_spectral_map)
    monkeypatch.setattr('cw.worker.get_request', mock_get_tiles_request)
    resp = worker.run(shared.good_input_data, dimrng=3)
    for i in resp:
        assert set(i.keys()) == {'result_md5', 'algorithm', 'result_ok', 'result', 'result_produced', 'y', 'x', 'inputs_md5'}


def test_decode_body():
    assert 'True' == 'True'

