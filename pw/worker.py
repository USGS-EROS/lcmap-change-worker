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

from merlin.support import chip_spec_queries
from merlin.timeseries import pyccd as pyccd_rods

from merlin.chips import get as chips_fn
from merlin.chip_specs import get as specs_fn


def dtstr_to_ordinal(dtstr, iso=True):
    """ Return ordinal from string formatted date"""
    _fmt = '%Y-%m-%dT%H:%M:%SZ' if iso else '%Y-%m-%d %H:%M:%S'
    _dt = datetime.strptime(dtstr, _fmt)
    return _dt.toordinal()


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


def run(msg):
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

    json_dumps      = json.dumps
    pw_logger_debug = pw.logger.debug
    pw_logger_error = pw.logger.error
    datetime_now    = datetime.now
    ccd_detect      = ccd.detect
    #pw_qa_bitpacked = pw.QA_BIT_PACKED
    hashlib_md5     = hashlib.md5

    for rod in rods:
        outgoing = dict()
        # rod[0] = ((chip_x, chip_y), pixel_x, pixel_y)
        px = rod[0][1]
        py = rod[0][2]
        try:
            _detect_start = datetime_now()
            # lcmap-merlin intersects the rod contents so that they are all even size, report
            pw_logger_debug("rod length for x/y {}/{} {}".format(px, py, len(rod[1]['blues'])))
            results = ccd_detect(rod[1]['dates'],
                                 rod[1]['blues'],
                                 rod[1]['greens'],
                                 rod[1]['reds'],
                                 rod[1]['nirs'],
                                 rod[1]['swir1s'],
                                 rod[1]['swir2s'],
                                 rod[1]['thermals'],
                                 rod[1]['quality'],
                                 params={})
            _detect_dur = datetime_now() - _detect_start
            pw_logger_debug("detect results for x, y: {}, {} took {} seconds to generate".format(px, py, _detect_dur.total_seconds()))
            outgoing['result'] = json_dumps(simplify_detect_results(results))
            outgoing['result_ok'] = True
        except Exception as e:
            # using e.args since detect() is a wrapper for ccd.detect(), leaving the returned exception unclear
            detect_exception_msg = "Exception running ccd.detect. x: {}, y: {}, algorithm: {}, " \
                                   "message: {}, exception args: {}".format(px, py, alg, e, e.args)
            pw_logger_error(detect_exception_msg)
            outgoing['result'] = detect_exception_msg
            outgoing['result_ok'] = False

        outgoing['algorithm'] = alg
        outgoing['x'], outgoing['y'] = px, py
        outgoing['result_md5'] = hashlib_md5(outgoing['result'].encode('UTF-8')).hexdigest()
        outgoing['result_produced'] = datetime_now().strftime('%Y-%m-%dT%H:%M:%SZ')
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
            unpacked_body  = decode_body(msgpack.unpackb(body))
            results        = run(unpacked_body)
            datetime_now   = datetime.now
            msgpack_packb  = msgpack.packb
            messaging_send = messaging.send
            pw_logger_debug = pw.logger.debug

            for result in results:
                _save_start = datetime_now()
                pw_logger_debug("saving result: {} {} at {}".format(result['x'], result['y'], _save_start.strftime("%H:%M:%S")))
                packed_result = msgpack_packb(result)
                messaging_send(packed_result, channel, exchange, routing_key)
                _dur = datetime_now() - _save_start
                pw_logger_debug("took {} seconds to deliver result message for x/y: {}/{}".format(_dur.total_seconds(), result['x'], result['y']))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as e:
            pw.logger.error('Unrecoverable error ({}) handling message: {}'.format(e, body))
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            sys.exit(1)

    return handler
