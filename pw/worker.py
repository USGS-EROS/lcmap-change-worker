#!/usr/bin/env python
import base64
import ccd
import hashlib
import numpy as np
import requests
import json
import xarray as xr
import pandas as pd
from datetime import datetime
import pw
from pyspark import SparkContext, SparkConf

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def get_request(url, params=None):
    return requests.get(url, params=params).json()


def spectral_map(specs_url):
    """ Return a dict of sensor bands keyed to their respective spectrum """
    _spec_map = dict()
    _map = {'thermal': 'toa -11', 'cfmask': '+cfmask -conf'}
    for bnd in ('blue', 'green', 'red', 'nir', 'swir1', 'swir2'):
        _map[bnd] = 'sr'

    try:
        for spectra in _map:
            url = "{specurl}?q=((tags:{band}) AND tags:{spec})".format(specurl=specs_url, spec=spectra, band=_map[spectra])
            pw.logger.debug("tile-specs url:{}".format(url))
            resp = get_request(url)
            # value needs to be a list, make it unique using set()
            _spec_map[spectra] = list(set([i['ubid'] for i in resp]))
        _spec_whole = get_request(specs_url)
    except Exception as e:
        raise Exception("Problem generating spectral map from api query, specs_url: {}\n message: {}".format(specs_url, e))

    return _spec_map, _spec_whole


def dtstr_to_ordinal(dtstr, iso=True):
    """ Return ordinal from string formatted date"""
    _fmt = '%Y-%m-%dT%H:%M:%SZ' if iso else '%Y-%m-%d %H:%M:%S'
    _dt = datetime.strptime(dtstr, _fmt)
    return _dt.toordinal()


def as_numpy_array(tile, specs_map):
    """ Return numpy array of tile data grouped by spectral map """
    NUMPY_TYPES = {
        'UINT8': np.uint8,
        'UINT16': np.uint16,
        'INT8': np.int8,
        'INT16': np.int16
    }
    try:
        spec    = specs_map[tile['ubid']]
        np_type = NUMPY_TYPES[spec['data_type']]
        shape   = specs_map[spec['ubid']]['data_shape']
        buffer  = base64.b64decode(tile['data'])
    except KeyError as e:
        raise Exception("as_numpy_array inputs missing expected keys: {}".format(e))

    return np.frombuffer(buffer, np_type).reshape(*shape)


def landsat_dataset(spectrum, ubid, specs, tiles):
    """ Return stack of landsat data for a given ubid, x, y, and time-span """
    # specs may not be unique, deal with it
    uniq_specs = []
    for spec in specs:
        if spec not in uniq_specs:
            uniq_specs.append(spec)

    specs_map = dict([[spec['ubid'], spec] for spec in uniq_specs if spec['ubid'] == ubid])
    rasters = xr.DataArray([as_numpy_array(tile, specs_map) for tile in tiles])

    ds = xr.Dataset()
    ds[spectrum] = (('t', 'x', 'y'), rasters)
    ds[spectrum].attrs = {'color': spectrum}
    ds.coords['t'] = (('t'), pd.to_datetime([t['acquired'] for t in tiles]))
    return ds


def rainbow(x, y, t, specs_url, tiles_url, requested_ubids):
    """ Return all the landsat data, organized by spectra for a given x, y, and time-span """
    spec_map, spec_whole = spectral_map(specs_url)
    ds = xr.Dataset()
    for (spectrum, ubids) in spec_map.items():
        for ubid in ubids:
            if ubid in requested_ubids:
                params = {'ubid': ubid, 'x': x, 'y': y, 'acquired': t}
                tiles_resp = get_request(tiles_url, params=params)
                if not tiles_resp:
                    raise Exception("No tiles returned for url: {} , params: {}".format(tiles_url, params))
                band = landsat_dataset(spectrum, ubid, spec_whole, tiles_resp)
                if band:
                    # combine_first instead of merge, for locations where data is missing for some bands
                    ds = ds.combine_first(band)
    return ds


def simplify_objects(obj):
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.int64):
        return int(obj)
    elif isinstance(obj, tuple) and ('_asdict' in dir(obj)):
        # looks like a namedtuple
        _out = {}
        objdict = obj._asdict()
        for key in objdict.keys():
            _out[key] = simplify_objects(objdict[key])
        return _out
    elif isinstance(obj, (list, np.ndarray, tuple)):
        return [simplify_objects(i) for i in obj]
    else:
        # should be a serializable type
        return obj


def simplify_detect_results(results):
    output = dict()
    for key in results.keys():
        output[key] = simplify_objects(results[key])
    return output


def assemble_data(inputs, dimrng=100):
    """ Assemble data for RDD generation """
    output = []
    dates  = [i.split('=')[1] for i in inputs['inputs_url'].split('&') if 'acquired=' in i][0]
    tx, ty = int(inputs['tile_x']), int(inputs['tile_y'])
    t_url  = inputs['inputs_url'].split('?')[0]
    s_url  = t_url.replace('/tiles', '/tile-specs')
    qlist  = inputs['inputs_url'].split('?')[1].split('&')
    ubids  = [i.replace('ubid=', '') for i in qlist if 'ubid=' in i]

    rbow = rainbow(tx, ty, dates, s_url, t_url, ubids)
    for x in range(0, dimrng):
        for y in range(0, dimrng):
            row, col = y, x
            _d = dict()
            for _bnd in ('blue', 'green', 'red', 'swir1', 'swir2', 'thermal', 'cfmask', 'nir'):
                _d[_bnd] = np.array(rbow[_bnd].values[:, row, col])
            _d['dates'] = np.array(rbow['t'].values)
            px = tx+(x * 30)
            py = ty+(y * -30)
            output.append(((px, py), _d))
    return {'tile_x': tx, 'tile_y': ty, 'data': output}


def save_detect(record):
    """ write result to cassandra """
    try:
        auth_provider = PlainTextAuthProvider(username=pw.DB_USERNAME, password=pw.DB_PASSWORD)
        cluster = Cluster(pw.DB_CONTACT_POINTS.split(','), auth_provider=auth_provider)
        session = cluster.connect()
        insrt_stmt = "INSERT INTO {}.results (y, tile_x, tile_y, algorithm, x, result_ok, inputs_md5, result, " \
                     "result_produced, result_md5) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(pw.DB_KEYSPACE)
        session.execute(insrt_stmt, (record['y'], record['tile_x'], record['tile_y'], record['algorithm'], record['x'],
                                     record['result_ok'], record['inputs_md5'], record['result'],
                                     record['result_produced'], record['result_md5']))
    except Exception as e:
        pw.logger.error("Exception saving ccd result to cassandra: {}".format(e))
        raise e
    return True


def detect(input, tile_x, tile_y):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    # input is a tuple: ((pixel x, pixel y), {bands dict}
    _px, _py = input[0][0], input[0][1]
    _bands   = input[1]
    output = dict()
    try:
        _results = ccd.detect(blues    = _bands['blue'],
                              greens   = _bands['green'],
                              reds     = _bands['red'],
                              nirs     = _bands['nir'],
                              swir1s   = _bands['swir1'],
                              swir2s   = _bands['swir2'],
                              thermals = _bands['thermal'],
                              quality  = _bands['cfmask'],
                              dates    = [dtstr_to_ordinal(str(pd.to_datetime(i)), False) for i in _bands['dates']])
        output['result'] = json.dumps(simplify_detect_results(_results))
        output['result_ok'] = True
        output['algorithm'] = _results['algorithm']
        output['tile_x'] = tile_x
        output['tile_y'] = tile_y
    except Exception as e:
        pw.logger.error("Exception running ccd.detect: {}".format(e))
        output['result'] = ''
        output['result_ok'] = False

    output['x'], output['y'] = _px, _py
    output['result_md5'] = hashlib.md5(output['result'].encode('UTF-8')).hexdigest()
    output['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    output['inputs_md5'] = 'not implemented'
    save_detect(output)
    return output


def spark_job(input_args):
    try:
        conf = (SparkConf().setAppName("lcmap-gen-{}".format(datetime.now().strftime('%Y-%m-%d-%I:%M'))))
        sc = SparkContext(conf=conf)

        inputs = dict()
        for arg in input_args:
            _al = arg.split("=", maxsplit=1)
            inputs[_al[0]] = _al[1]

        data = assemble_data(inputs)
        ccd_rdd = sc.parallelize(data['data'], 10000)
        ccd_rdd.foreach(lambda x: detect(x, data['tile_x'], data['tile_y']))
        return True
    except Exception as e:
        pw.logger.error('Unrecoverable error ({}) input args: {}'.format(e, input_args))
        # Throw an exception so when called from the console a non-zero return code is given
        # this may or may not be necessary
        raise e

