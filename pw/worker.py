#!/usr/bin/env python
import base64
import ccd
import hashlib
import msgpack
import numpy as np
import requests
import json
import xarray as xr
import pandas as pd
import sys
from . import messaging
from datetime import datetime
import pw


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


def detect(rainbow, x, y):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    try:
        # Beware: rainbow contains stacks of row-major two-dimensional arrays
        # for each band of data. These variables are used to make the order
        # of access clear.
        row, col = y, x
        rainbow_date_array = np.array(rainbow['t'].values)
        return ccd.detect(blues=np.array(rainbow['blue'].values[:, row, col]),
                          greens=np.array(rainbow['green'].values[:, row, col]),
                          reds=np.array(rainbow['red'].values[:, row, col]),
                          nirs=np.array(rainbow['nir'].values[:, row, col]),
                          swir1s=np.array(rainbow['swir1'].values[:, row, col]),
                          swir2s=np.array(rainbow['swir2'].values[:, row, col]),
                          thermals=np.array(rainbow['thermal'].values[:, row, col]),
                          quality=np.array(rainbow['cfmask'].values[:, row, col]),
                          dates=[dtstr_to_ordinal(str(pd.to_datetime(i)), False) for i in rainbow_date_array])
    except Exception as e:
        raise Exception(e)


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


def run(msg, dimrng=100):
    """
    Generator. Given parameters of 'inputs_url', 'tile_x', & 'tile_y',
    return results of ccd.detect along with other details necessary for
    returning change results
    """
    pw.logger.info("run() called with keys:{} values:{}".format(list(msg.keys()), list(msg.values())))
    try:
        dates     = [i.split('=')[1] for i in msg['inputs_url'].split('&') if 'acquired=' in i][0]
        tile_x    = msg['tile_x']
        tile_y    = msg['tile_y']
        tiles_url = msg['inputs_url'].split('?')[0]
        specs_url = tiles_url.replace('/tiles', '/tile-specs')

        querystr_list = msg['inputs_url'].split('?')[1].split('&')
        requested_ubids = [i.replace('ubid=', '') for i in querystr_list if 'ubid=' in i]
    except KeyError as e:
        raise Exception("input for worker.run missing expected key values: {}".format(e))

    rbow = rainbow(tile_x, tile_y, dates, specs_url, tiles_url, requested_ubids)

    # hard coding dimensions for the moment,
    # it should come from a tile-spec query
    # {'data_shape': [100, 100], 'pixel_x': 30, 'pixel_y': -30}
    # tile-spec query results should then be provided to detect()
    for x in range(0, dimrng):
        for y in range(0, dimrng):
            px, py = (30, -30)
            xx = tile_x + (x * px)
            yy = tile_y + (y * py)

            outgoing = dict()
            try:
                # results.keys(): algorithm, change_models, procedure, processing_mask,
                results = detect(rbow, x, y)
                outgoing['result'] = json.dumps(simplify_detect_results(results))
                outgoing['result_ok'] = True
                outgoing['algorithm'] = results['algorithm']
            except Exception as e:
                pw.logger.error("Exception running ccd.detect: {}".format(e))
                outgoing['result'] = ''
                outgoing['result_ok'] = False

            outgoing['x'], outgoing['y'] = xx, yy
            outgoing['result_md5'] = hashlib.md5(outgoing['result'].encode('UTF-8')).hexdigest()
            outgoing['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            outgoing['inputs_md5'] = 'not implemented'
            yield outgoing


def decode_body(body):
    """ Convert keys and values unpacked as bytes to strings """
    out = dict()
    for k, v in body.items():
        out_k, out_v = k, v
        if isinstance(k, bytes):
            out_k = k.decode('utf-8')
        if isinstance(v, bytes):
            out_v = v.decode('utf-8')
        out[out_k] = out_v
    return out


def callback(exchange, routing_key):
    def handler(channel, method_frame, properties, body):
        try:
            pw.logger.info("Received message with packed body: {}".format(body))
            unpacked_body = decode_body(msgpack.unpackb(body))
            results = run(unpacked_body)
            for result in results:
                pw.logger.debug("saving result: {} {}".format(result['x'], result['y']))
                packed_result = msgpack.packb(result)
                messaging.send(packed_result, channel, exchange, routing_key)
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as e:
            pw.logger.error('Unrecoverable error ({}) handling message: {}'.format(e, body))
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
            sys.exit(1)

    return handler
