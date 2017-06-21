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
    _map = {'blue': ('sr', 'blue'), 'green': ('sr', 'green'), 'red': ('sr', 'red'), 'nir': ('sr', 'nir'),
            'swir1': ('sr', 'swir1'), 'swir2': ('sr', 'swir2'), 'thermal': ('bt', 'thermal -BTB11'),
            'cfmask': 'pixelqa'}

    try:
        for spectra in _map:
            if isinstance(_map[spectra], str):
                _tags = ["tags:" + _map[spectra]]
            else:
                _tags = ["tags:"+i for i in _map[spectra]]
            _qs = " AND ".join(_tags)
            url = "{specurl}?q=({tags})".format(specurl=specs_url, tags=_qs)
            _start = datetime.now()
            resp = get_request(url)
            dur = datetime.now() - _start
            pw.logger.debug("request for {} took {} seconds to fulfill".format(url, dur.total_seconds()))
            # value needs to be a list, make it unique using set()
            _spec_map[spectra] = list(set([i['ubid'] for i in resp]))
        _start_whole = datetime.now()
        _spec_whole = get_request(specs_url)
        _dur_whole = datetime.now() - _start_whole
        pw.logger.debug("request for whole spec response to {}, took {} seconds".format(specs_url, _dur_whole.total_seconds()))
    except Exception as e:
        raise Exception("Problem generating spectral map from api query, specs_url: {}\n message: {}".format(specs_url, e))
    return _spec_map, _spec_whole


def dtstr_to_ordinal(dtstr, iso=True):
    """ Return ordinal from string formatted date"""
    _fmt = '%Y-%m-%dT%H:%M:%SZ' if iso else '%Y-%m-%d %H:%M:%S'
    _dt = datetime.strptime(dtstr, _fmt)
    return _dt.toordinal()


def as_numpy_array(chip, specs_map):
    """ Return numpy array of chip data grouped by spectral map """
    NUMPY_TYPES = {
        'UINT8': np.uint8,
        'UINT16': np.uint16,
        'INT8': np.int8,
        'INT16': np.int16
    }
    try:
        spec    = specs_map[chip['ubid']]
        np_type = NUMPY_TYPES[spec['data_type']]
        shape   = specs_map[spec['ubid']]['data_shape']
        buffer  = base64.b64decode(chip['data'])
    except KeyError as e:
        raise Exception("as_numpy_array inputs missing expected keys: {}".format(e))

    return np.frombuffer(buffer, np_type).reshape(*shape)


def landsat_dataset(spectrum, ubid, specs, chips):
    """ Return stack of landsat data for a given ubid, x, y, and time-span """
    # specs may not be unique, deal with it
    uniq_specs = []
    for spec in specs:
        if spec not in uniq_specs:
            uniq_specs.append(spec)

    specs_map = dict([[spec['ubid'], spec] for spec in uniq_specs if spec['ubid'] == ubid])
    rasters = xr.DataArray([as_numpy_array(chip, specs_map) for chip in chips])

    ds = xr.Dataset()
    ds[spectrum] = (('t', 'x', 'y'), rasters)
    ds[spectrum].attrs = {'color': spectrum}
    ds.coords['t'] = (('t'), pd.to_datetime([t['acquired'] for t in chips]))
    return ds


def rainbow(x, y, t, specs_url, chips_url, requested_ubids):
    """ Return all the landsat data, organized by spectra for a given x, y, and time-span """
    spec_map, spec_whole = spectral_map(specs_url)
    ds = xr.Dataset()
    for (spectrum, ubids) in spec_map.items():
        for ubid in ubids:
            if ubid in requested_ubids:
                params = {'ubid': ubid, 'x': x, 'y': y, 'acquired': t}
                _chip_start = datetime.now()
                chips_resp = get_request(chips_url, params=params)
                _chip_dur = datetime.now() - _chip_start
                pw.logger.debug("chip request for ubid, x, y, acquired: {}, {}, {}, {} "
                                "\ntook: {} seconds\nnumber of chips: {}".format(ubid, x, y, t, _chip_dur.total_seconds(), len(chips_resp)))
                if chips_resp:
                    band = landsat_dataset(spectrum, ubid, spec_whole, chips_resp)
                    if band:
                        # combine_first instead of merge, for locations where data is missing for some bands
                        ds = ds.combine_first(band)
                else:
                     pw.logger.warn("No chips returned for ubid, x, y, acquired: {}, {}, {}, {}".format(ubid, x, y, t))
    return ds.fillna(0)


def detect(rainbow, x, y):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    try:
        # Beware: rainbow contains stacks of row-major two-dimensional arrays
        # for each band of data. These variables are used to make the order
        # of access clear.
        row, col = y, x
        rainbow_date_array = np.array(rainbow['t'].values)
        # according to lcmap-pyccd README, values expected in the following order:
        # ccd.detect(dates, blues, greens, reds, nirs, swir1s, swir2s, thermals, qas)

        ccd_params = {}
        if pw.QA_BIT_PACKED is not 'True':
            ccd_params = {'QA_BITPACKED': False,
                          'QA_FILL': 255,
                          'QA_CLEAR': 0,
                          'QA_WATER': 1,
                          'QA_SHADOW': 2,
                          'QA_SNOW': 3,
                          'QA_CLOUD': 4}

        return ccd.detect([dtstr_to_ordinal(str(pd.to_datetime(i)), False) for i in rainbow_date_array],
                          np.array(rainbow['blue'].values[:, row, col]),
                          np.array(rainbow['green'].values[:, row, col]),
                          np.array(rainbow['red'].values[:, row, col]),
                          np.array(rainbow['nir'].values[:, row, col]),
                          np.array(rainbow['swir1'].values[:, row, col]),
                          np.array(rainbow['swir2'].values[:, row, col]),
                          np.array(rainbow['thermal'].values[:, row, col]),
                          np.array(rainbow['cfmask'].values[:, row, col], dtype=int),
                          params=ccd_params)
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
    Generator. Given parameters of 'inputs_url', 'chip_x', & 'chip_y',
    return results of ccd.detect along with other details necessary for
    returning change results
    """
    pw.logger.info("run() called with keys:{} values:{}".format(list(msg.keys()), list(msg.values())))
    try:
        dates     = [i.split('=')[1] for i in msg['inputs_url'].split('&') if 'acquired=' in i][0]
        chip_x    = msg['chip_x']
        chip_y    = msg['chip_y']
        chips_url = msg['inputs_url'].split('?')[0]
        specs_url = chips_url.replace('/chips', '/chip-specs')

        querystr_list = msg['inputs_url'].split('?')[1].split('&')
        requested_ubids = [i.replace('ubid=', '') for i in querystr_list if 'ubid=' in i]
    except KeyError as e:
        raise Exception("input for worker.run missing expected key values: {}".format(e))

    rbow = rainbow(chip_x, chip_y, dates, specs_url, chips_url, requested_ubids)
    alg  = ccd.version.__algorithm__

    # hard coding dimensions for the moment,
    # it should come from a chip-spec query
    # {'data_shape': [100, 100], 'pixel_x': 30, 'pixel_y': -30}
    # chip-spec query results should then be provided to detect()
    for x in range(0, dimrng):
        for y in range(0, dimrng):
            px, py = (30, -30)
            xx = chip_x + (x * px)
            yy = chip_y + (y * py)

            outgoing = dict()
            try:
                # results.keys(): algorithm, change_models, procedure, processing_mask,
                _detect_start = datetime.now()
                results = detect(rbow, x, y)
                _detect_dur = datetime.now() - _detect_start
                pw.logger.debug("detect results for x, y: {}, {} took {} seconds to generate".format(xx, yy, _detect_dur.total_seconds()))
                outgoing['result'] = json.dumps(simplify_detect_results(results))
                outgoing['result_ok'] = True
            except Exception as e:
                # using e.args since detect() is a wrapper for ccd.detect(), leaving the returned exception unclear
                detect_exception_msg = "Exception running ccd.detect. x: {}, y: {}, algorithm: {}, " \
                                       "message: {}, exception args: {}".format(xx, yy, alg, e, e.args)
                pw.logger.error(detect_exception_msg)
                outgoing['result'] = detect_exception_msg
                outgoing['result_ok'] = False

            outgoing['algorithm'] = alg
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
                _save_start = datetime.now()
                pw.logger.debug("saving result: {} {} at {}".format(result['x'], result['y'], _save_start.strftime("%H:%M:%S")))
                packed_result = msgpack.packb(result)
                messaging.send(packed_result, channel, exchange, routing_key)
                _dur = datetime.now() - _save_start
                pw.logger.debug("took {} seconds to deliver result message for x/y: {}/{}".format(_dur.total_seconds(), result['x'], result['y']))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as e:
            pw.logger.error('Unrecoverable error ({}) handling message: {}'.format(e, body))
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            sys.exit(1)

    return handler
