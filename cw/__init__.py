import codecs
import logging
import json
import os
import sys
import traceback
import requests
import numpy as np
from . import messaging
from . import spark

#__format = '%(asctime)s %(module)s::%(funcName)-20s - %(message)s'
#logging.basicConfig(stream=sys.stdout,
#                    level=logging.DEBUG,
#                    format=__format,
#                    datefmt='%Y-%m-%d %H:%M:%S')
#logger = logging.getLogger('lcw')

config = {'rabbit-host': os.getenv('LCW_RABBIT_HOST', 'localhost'),
          'rabbit-port': int(os.getenv('LCW_RABBIT_PORT', 5672)),
          'rabbit-queue': os.getenv('LCW_RABBIT_QUEUE', 'local.lcmap.changes.worker'),
          'rabbit-exchange': os.getenv('LCW_RABBIT_EXCHANGE', 'local.lcmap.changes.worker'),
          'rabbit-result-routing-key': os.getenv('LCW_RABBIT_RESULT_ROUTING_KEY', 'change-detection-result'),
          'rabbit-ssl': os.getenv('LCW_RABBIT_SSL', False),
          'api-host': os.getenv('LCW_API_HOST', 'http://localhost'),
          'api-port': os.getenv('LCW_API_PORT', '5678'),
          'tile-specs-url': os.getenv('LCW_SPECS_URL', '/tile-specs'),
          'tiles-url': os.getenv('LCW_TILES_URL', '/tiles'),
          'ubid_band_dict': {
              'tm': {'red': 'band3',
                     'blue': 'band1',
                     'green': 'band2',
                     'nirs': 'band4',
                     'swir1s': 'band5',
                     'swir2s': 'band7',
                     'thermals': 'band6',
                     'qas': 'cfmask'},
              'oli': {'red': 'band4',
                      'blue': 'band2',
                      'green': 'band3',
                      'nirs': 'band5',
                      'swir1s': 'band6',
                      'swir2s': 'band7',
                      'thermals': 'band10',
                      'qas': 'cfmask'}},
           'numpy_type_map': {
               'UINT8': np.uint8,
               'UINT16': np.uint16,
               'INT8': np.int8,
               'INT16': np.int16
           }
          }




def send(cfg, message):
    return messaging.send(cfg, message)


def listen(cfg, callback):
    messaging.listen(cfg, callback)


def launch_task(cfg, msg_body):
    # msg_body needs to be a url
    return spark.run(cfg, msg_body)


def callback(cfg):
    def handler(ch, method, properties, body):
        try:
            print("Body type:{}".format(type(body.decode('utf-8'))))
            print("Launching task for {}".format(body))
            results = launch_task(cfg, json.loads(body.decode('utf-8')))
            print("Now returning results of type:{}".format(type(results)))
            for result in results:
                if type(result) is dict:
                    # right now, dict type indicates successful execution.
                    # results may not be valid though
                    print(send(cfg, json.dumps(result)))
                else:
                    # not successful, do something with this error like send it to
                    # an error queue or logfile.  print for the moment.
                    print("Execution error:{}".format(result))
                    traceback.print_exc(file=sys.stdout)
        except Exception as e:
            print("Exception message: {}".format(e))
            traceback.print_exc(file=sys.stdout)

    return handler
