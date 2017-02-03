#!/usr/bin/env python
import ast
import base64
import ccd
import hashlib
import os
import numpy as np
import sys
import requests
from glob import glob
from datetime import datetime
from util import ubid_band_dict
from cw.sending import Sending

class SparkException(Exception):
    pass

class Spark(object):
    def __init__(self, config):
        self.config = config
        ubids = 'LANDSAT_7/ETM/sr_band1&ubid=LANDSAT_7/ETM/sr_band2&ubid=LANDSAT_7/ETM/sr_band4&ubid=LANDSAT_7/ETM/sr_band5&ubid=LANDSAT_7/ETM/sr_band7&ubid=LANDSAT_7/ETM/cfmask&ubid=LANDSAT_7/ETM/sr_band3&ubid=LANDSAT_7/ETM/toa_band6'
        self.ubids_list = ubids.split('&ubid=')
        self.sender = Sending(config)

    def sort_band_data(self, band, field):
        return sorted(band, key=lambda x: x[field])

    def b64_to_bytearray(self, data):
        return np.frombuffer(base64.b64decode(data), np.int16)

    def dtstr_to_ordinal(self, dtstr):
        _dt = datetime.strptime(dtstr, '%Y-%m-%dT%H:%M:%SZ')
        return _dt.toordinal()

    def collect_data(self, band_group, json_data):
        _blist = "band2 band3 band4 band5 band6 band7 cfmask"
        band_list = "band1 " + _blist if band_group is 'tm' else "band10 " + _blist
        for b in band_list.split(" "):
            vars()[b] = []
        band_bucket = []
        for item in json_data:
            which_band = item['ubid'][-6:].replace("_", "")
            band_bucket.append(which_band)
            vars()[which_band].append(item)
        valid_sources = set([i['source'] for i in vars()['band2']]) & set([i['source'] for i in vars()['cfmask']])
        for bucket in band_list.split(" "):
            _orig = vars()[bucket]
            vars()[bucket+'_clean'] = [item for item in _orig if item['source'] in valid_sources]
        for bucket in band_list.split(" "):
            _sorted = vars()[bucket+'_sorted'] = self.sort_band_data(vars()[bucket+'_clean'], 'acquired')
            vars()[bucket+'_bytes'] = [self.b64_to_bytearray(item['data']) for item in _sorted]
        dates = [self.dtstr_to_ordinal(i['acquired']) for i in vars()['band2_sorted']]
        mapping = ubid_band_dict[band_group]
        for band in "red green blue nirs swirs1 swirs2 thermals qas".split(" "):
            vars()[band+'_array'] = np.array(vars()[mapping[band] + '_bytes'])
            print("{}: len {}".format(band, len(vars()[band+'_array'])))
        rows = len(dates)  #282
        cells = 10000      # per tile, 100x100
        output = []
        try:
            for pixel in range(0, cells):
                lower = pixel
                upper = pixel + 1
                _od = dict()
                _od[pixel] = {'dates': dates,
                              'red': vars()['red_array'][0:rows, lower:upper],
                              'green': vars()['green_array'][0:rows, lower:upper],
                              'blue': vars()['blue_array'][0:rows, lower:upper],
                              'nirs': vars()['nirs_array'][0:rows, lower:upper],
                              'swirs1': vars()['swirs1_array'][0:rows, lower:upper],
                              'swirs2': vars()['swirs2_array'][0:rows, lower:upper],
                              'thermals': vars()['thermals_array'][0:rows, lower:upper],
                              'qas': vars()['qas_array'][0:rows, lower:upper]}
                output.append(_od)
        except IndexError, e:
            output = "IndexError for returned data: {}".format(e.message)

        print "returning output from collect_output..."
        return output

    def run_pyccd(self, datad):
        def np_to_list(_d):
            _x = [i[0] for i in _d]
            return np.array(_x)

        data = datad.values()[0]
        print ("data is: {}".format(type(data)))
        results = ccd.detect(data['dates'], np_to_list(data['blue']), np_to_list(data['green']), np_to_list(data['red']), np_to_list(data['nirs']), np_to_list(data['swirs1']), np_to_list(data['swirs2']), np_to_list(data['thermals']), np_to_list(data['qas']))
        return results

    def pixel_xy(self, index, tilex, tiley, dim=100):
        # if index is 565, tilex is 123, tiley is 330, xdim is 100, ydim is 100
        row = index / dim
        col = index - row * dim
        return {'y': tiley+row, 'x': tilex+col}

    def run(self, indata):
        input_d = ast.literal_eval(indata)
        # url = "http://lcmap-test.cr.usgs.gov/landsat/tiles?x=-2013585&y=3095805&acquired=1982-01-01/2017-01-01&ubid="

        resp = requests.get(input_d['inputs_url'])
        if resp.status_code != 200:
            resp = requests.get(input_d['inputs_url'])

        # data = list()
        # for i in self.ubids_list:
        #     print "attempting query of: {}".format(url+i)
        #     resp = requests.get(url+i)
        #     if resp.status_code != 200:
        #         print "got a non-200: {}\ntrying again...".format(resp.status_code)
        #         resp = requests.get(url+i)
        #     print "len of resp.json() {}".format(len(resp.json()))
        #     for d in resp.json():
        #         data.append(d)

        # band_group = 'oli' if 'OLI_TIRS' in url else 'tm'
        band_group = 'oli' if 'OLI_TIRS' in input_d['inputs_url'] else 'tm'

        # output = collect_data(band_group, tile_resp.json())
        output = self.collect_data(band_group, resp.json())

        # HACK
        if isinstance(output, str):
            self.sender.send(output)
        else:
            for item in output:
                # item is a dict, keyed by pixel index {0: {dates: , green: , yada...}
                pixel_index = item.keys()[0]

                results = self.run_pyccd(item)
                outgoing = dict()
                outgoing['x'], outgoing['y'] = self.pixel_xy(pixel_index, input_d['tile_x'], input_d['tile_y'])
                outgoing['algorithm'] = input_d['algorithm']
                outgoing['result_md5'] = hashlib.md5("{}".format(results)).hexdigest()
                outgoing['result_ok'] = True
                outgoing['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                outgoing['inputs_md5'] = hashlib.md5("{}".format(resp.json())).hexdigest()

                self.sender.send("{}".format(outgoing))

        return True


def run(config, indata):
    sprk = Spark(config)
    return sprk.run(indata)
