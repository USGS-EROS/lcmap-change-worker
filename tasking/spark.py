#!/usr/bin/env python
import os
import sys
import requests
from glob import glob
from datetime import datetime
import base64
import numpy as np
from util import ubid_band_dict


class Spark(object):
    def __init__(self, config=None, params=None):
        self.config = config
        self.params = params
        #sys.path.insert(0, os.path.join(self.config['sparkhome'], 'python'))
        #sys.path.insert(0, os.path.join(self.config['sparkhome'], glob('python/lib/py4j*.zip')[0]))
        #sys.path.insert(0, glob(os.path.join(spark_home, 'python/lib/py4j*.zip'))[0]
        #from pyspark import SparkContext, SparkConf

    def run(self, url=None):
        #url = """http://lcmap-test.cr.usgs.gov/landsat/tiles?ubid=LANDSAT_7/ETM/sr_band1&ubid=LANDSAT_7/ETM/sr_band2&
        #ubid=LANDSAT_7/ETM/sr_band3&ubid=LANDSAT_7/ETM/sr_band4&ubid=LANDSAT_7/ETM/sr_band5&ubid=LANDSAT_7/ETM/toa_band6
        #&ubid=LANDSAT_7/ETM/sr_band7&ubid=LANDSAT_7/ETM/cfmask&x=-2013585&y=3095805&acquired=1982-01-01/2017-01-01"""
        url = """http://lcmap-test.cr.usgs.gov/landsat/tiles?ubid=LANDSAT_7/ETM/sr_band1&ubid=LANDSAT_7/ETM/sr_band2&ubid=LANDSAT_7/ETM/sr_band4&ubid=LANDSAT_7/ETM/sr_band5&ubid=LANDSAT_7/ETM/sr_band7&ubid=LANDSAT_7/ETM/cfmask&x=-2013585&y=3095805&acquired=1982-01-01/2017-01-01"""

        url36 = """http://lcmap-test.cr.usgs.gov/landsat/tiles?ubid=LANDSAT_7/ETM/sr_band3&ubid=LANDSAT_7/ETM/toa_band6&x=-2013585&y=3095805&acquired=1982-01-01/2017-01-01"""

        tile_resp = requests.get(url)
        resp36 = requests.get(url36)
        band_group = 'oli' if 'OLI_TIRS' in url else 'tm'

        print "response: {}".format(tile_resp.status_code)

        if tile_resp.status_code == 200:
            output = self.collect_data(band_group, (tile_resp.json(), resp36.json()))
            return output
        else:
            return tile_resp.status_code

    def sort_band_data(self, band, field):
        return sorted(band, key=lambda x: x[field])

    def b64_to_bytearray(self, data):
        return np.frombuffer(base64.b64decode(data), np.int16)

    def dtstr_to_ordinal(self, dtstr):
        _dt = datetime.strptime(dtstr, '%Y-%m-%dT%H:%M:%SZ')
        return _dt.toordinal()

    def collect_data(self, band_group, json_data):
        # sort out the initial band lists
        _blist = "band2 band3 band4 band5 band6 band7 cfmask"
        band_list = "band1 " + _blist if band_group is 'tm' else "band10 " + _blist
        for b in band_list.split(" "):
            vars()[b] = []

        # sort the data by band into their respective buckets
        ## this to be removed once all bands reporting
        band_bucket = []
        if isinstance(json_data, tuple):
            for jdata in json_data:
                for item in jdata:
                    which_band = item['ubid'][-6:].replace("_", "")
                    ## this to be removed once all bands reporting
                    band_bucket.append(which_band)
                    vars()[which_band].append(item)
        else:
            for item in json_data:
                which_band = item['ubid'][-6:].replace("_", "")
                ## this to be removed once all bands reporting
                band_bucket.append(which_band)
                vars()[which_band].append(item)

        ## this to be removed once all bands reporting
        print "set of which_band: {}".format(set(band_bucket))
        # we only want datasets with corresponding cfmask data
        valid_sources = set([i['source'] for i in vars()['band2']]) & set([i['source'] for i in vars()['cfmask']])
        for bucket in band_list.split(" "):
            _orig = vars()[bucket]
            vars()[bucket+'_clean'] = [item for item in _orig if item['source'] in valid_sources]

        # sort data by date acquired, and convert data to bytearray
        for bucket in band_list.split(" "):
            _sorted = vars()[bucket+'_sorted'] = self.sort_band_data(vars()[bucket+'_clean'], 'acquired')
            vars()[bucket+'_bytes'] = [self.b64_to_bytearray(item['data']) for item in _sorted]

        # create our date list
        dates = [self.dtstr_to_ordinal(i['acquired']) for i in vars()['band2_sorted']]

        # map bands to spectra
        mapping = ubid_band_dict[band_group]

        for band in "red green blue nirs swirs1 swirs2 thermals qas".split(" "):
            vars()[band] = vars()[mapping[band] + '_bytes']
            vars()[band+'_array'] = np.array(vars()[band])

        rows = len(dates)  #282
        cells = 10000      # per tile, 100x100

        output = {}
        for pixel in range(0, cells):
            lower = pixel
            upper = pixel + 1
            output[pixel] = {'dates': dates,
                             'red': vars()['red_array'][0:rows, lower:upper],
                             'green': vars()['green_array'][0:rows, lower:upper],
                             'blue': vars()['blue_array'][0:rows, lower:upper],
                             'nirs': vars()['nirs_array'][0:rows, lower:upper],
                             'swirs1': vars()['swirs1_array'][0:rows, lower:upper],
                             'swirs2': vars()['swirs2_array'][0:rows, lower:upper],
                             'thermals': vars()['thermals_array'][0:rows, lower:upper],
                             'qas': vars()['qas_array'][0:rows, lower:upper]}


        return output
        #reds_array = np.array(reds)
        # first day reds
        #reds_array[0:282,0:1]
        # last day reds
        #reds_array[0:282,9999:10000]


def run(config, params):
    sprk = Spark(config)
    return sprk.run(params)

