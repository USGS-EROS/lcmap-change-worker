#!/usr/bin/env python
import ccd
import hashlib
import msgpack
import numpy as np
import json
import sys
from . import messaging
from datetime import datetime
import pw

from merlin.support import aardvark, chip_spec_queries, data_config
from merlin.timeseries import pyccd as pyccd_rods

from merlin.chips import get as chips_fn
from merlin.chip_specs import get as specs_fn


def dtstr_to_ordinal(dtstr, iso=True):
    """ Return ordinal from string formatted date"""
    _fmt = '%Y-%m-%dT%H:%M:%SZ' if iso else '%Y-%m-%d %H:%M:%S'
    _dt = datetime.strptime(dtstr, _fmt)
    return _dt.toordinal()


def detect(rods):
    """ Return results of ccd.detect for a given stack of data at a particular x and y """
    try:
        # Beware: rainbow contains stacks of row-major two-dimensional arrays
        # for each band of data. These variables are used to make the order
        # of access clear.
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

        return ccd.detect(rods['dates'],
                          rods['blue'],
                          rods['green'],
                          rods['red'],
                          rods['nir'],
                          rods['swir1'],
                          rods['swir2'],
                          rods['thermal'],
                          rods['cfmask'],
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
    except KeyError as e:
        raise Exception("input for worker.run missing expected key values: {}".format(e))

    queries = chip_spec_queries(specs_url)
    rods = pyccd_rods((chip_x, chip_y), specs_url, specs_fn, chips_url, chips_fn, dates, queries)
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
