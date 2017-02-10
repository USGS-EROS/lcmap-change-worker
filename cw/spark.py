#!/usr/bin/env python
import base64
import ccd
import hashlib
import math
import numpy as np
import requests
import xarray as xr
import pandas as pd
import cw
from datetime import datetime


class SparkException(Exception):
    pass


class Spark(object):
    def __init__(self, config):
        self.config = config
        self.tiles_url = "{}:{}/{}".format(self.config['api-host'], self.config['api-port'], self.config['tiles-url'])
        self.specs_url = "{}:{}/{}".format(self.config['api-host'], self.config['api-port'], self.config['tiles-specs-url'])

    def spectral_map(self):
        """ Return a dict of sensor bands keyed to their respective spectrum """
        _spec_map = dict()
        _map = {'thermal': 'toa -11', 'cfmask': '+cfmask -conf'}
        for bnd in ('blue', 'green', 'red', 'nir', 'swir1', 'swir2'):
            _map[bnd] = 'sr'

        for spectra in _map:
            url = "{specurl}?q=((tags:{band}) AND tags:{spec})".format(specurl=self.specs_url, spec=spectra, band=_map[spectra])
            resp = requests.get(url).json()
            _spec_map[spectra] = [i['ubid'] for i in resp]
        return _spec_map

    def dtstr_to_ordinal(self, dtstr):
        """ Return ordinal from string formatted date"""
        _dt = datetime.strptime(dtstr, '%Y-%m-%dT%H:%M:%SZ')
        return _dt.toordinal()

    def as_numpy_array(self, tile, specs_map):
        """ Return numpy array of tile data grouped by spectral map """
        spec    = specs_map[tile['ubid']]
        np_type = self.config['numpy_type_map'][spec['data_type']]
        shape   = specs_map[spec['ubid']]['data_shape']
        buffer  = base64.b64decode(tile['data'])
        return np.frombuffer(buffer, np_type).reshape(*shape)

    def landsat_dataset(self, spectrum, x, y, t, ubid):
        """ Return stack of landsat data for a given ubid, x, y, and time-span """
        specs     = requests.get(self.specs_url).json()
        specs_map = dict([[spec['ubid'], spec] for spec in specs])
        query     = {'ubid': ubid, 'x': x, 'y': y, 'acquired': t}
        tiles     = requests.get(self.tiles_url, params=query).json()
        rasters   = xr.DataArray([self.as_numpy_array(tile, specs_map) for tile in tiles])

        ds = xr.Dataset()
        ds[spectrum]          = (('t', 'x', 'y'), rasters)
        ds[spectrum].attrs    = {'color': spectrum}
        ds.coords['t']        = (('t'), pd.to_datetime([t['acquired'] for t in tiles]))
        ds.coords['source']   = (('t'), [t['source'] for t in tiles])
        ds.coords['acquired'] = (('t'), [t['acquired'] for t in tiles])
        ds.coords['ordinal']  = (('t'), [self.dtstr_to_ordinal(t['acquired']) for t in tiles])
        return ds

    def rainbow(self, x, y, t):
        """ Return all the landsat data, organized by spectra for a given x, y, and time-span """
        ds = xr.Dataset()
        for (spectrum, ubids) in self.spectral_map().items():
            for ubid in ubids:
                band = self.landsat_dataset(spectrum, x, y, t, ubid)
                if band:
                    ds = ds.merge(band)
        return ds

    def detect(self, rainbow, x, y):
        """ Return results of ccd.detect for a given stack of data at a particular x and y """
        return ccd.detect(blues=np.array(rainbow['blue'].values[:, x, y]),
                          greens=np.array(rainbow['green'].values[:, x, y]),
                          reds=np.array(rainbow['red'].values[:, x, y]),
                          nirs=np.array(rainbow['nir'].values[:, x, y]),
                          swir1s=np.array(rainbow['swir1'].values[:, x, y]),
                          swir2s=np.array(rainbow['swir2'].values[:, x, y]),
                          thermals=np.array(rainbow['thermal'].values[:, x, y]),
                          quality=np.array(rainbow['cfmask'].values[:, x, y]),
                          dates=list(rainbow['ordinal']))

    def run(self, input_d):
        """
        Generator function. Given parameters of 'inputs_url', 'tile_x', & 'tile_y',
        return results of ccd.detect along with other details necessary for storing
        results in a data warehouse
        """
        print("run() called with keys:{} values:{}".format(list(input_d.keys()), list(input_d.values())))

        dates = [i.split('=')[1] for i in input_d['inputs_url'].split('&') if 'acquired=' in i][0]
        tile_x, tile_y = input_d['tile_x'], input_d['tile_y']
        rainbow = self.rainbow(tile_x, tile_y, dates)

        # hard coding dimensions for the moment,
        # it should come from a tile-spec query
        # {'data_shape': [100, 100], 'pixel_x': 30, 'pixel_y': -30}
        # tile-spec query results should then be provided to self.detect()
        dimrng = 100
        for x in range(0, dimrng):
            for y in range(0, dimrng):
                tx, ty = (100, 100)
                px, py = (30, -30)
                xx = tile_x + (x % tx) * px
                yy = tile_y + math.floor(y / ty) * py
                # results.keys(): algorithm, change_models, procedure, processing_mask,
                results = self.detect(rainbow, x, y)

                outgoing = dict()
                outgoing['result'] = str(results)
                outgoing['x'], outgoing['y'] = xx, yy
                outgoing['algorithm'] = results['algorithm']
                outgoing['result_md5'] = hashlib.md5("{}".format(results).encode('utf-8')).hexdigest()
                # somehow determine if the result is ok or not.
                # all True for the moment
                outgoing['result_ok'] = True
                outgoing['result_produced'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                outgoing['inputs_md5'] = 'not implemented'
                yield outgoing


def run(config, indata):
    sprk = Spark(config)
    return sprk.run(indata)
