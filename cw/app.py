import os
import sys
import logging
import numpy as np

config = {'rabbit-host': os.getenv('LCW_RABBIT_HOST', 'localhost'),
          'rabbit-port': int(os.getenv('LCW_RABBIT_PORT', 5672)),
          'rabbit-queue': os.getenv('LCW_RABBIT_QUEUE', 'local.lcmap.changes.worker'),
          'rabbit-exchange': os.getenv('LCW_RABBIT_EXCHANGE', 'local.lcmap.changes.worker'),
          'rabbit-result-routing-key': os.getenv('LCW_RABBIT_RESULT_ROUTING_KEY', 'change-detection-result'),
          'rabbit-ssl': os.getenv('LCW_RABBIT_SSL', False),
          'api-host': os.getenv('LCW_API_HOST', 'http://localhost'),
          'api-port': os.getenv('LCW_API_PORT', '5678'),
          'log-level': os.getenv('LCW_LOG_LEVEL', 10),
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

logging.basicConfig(stream=sys.stdout,
                    level=config['log-level'],
                    format='%(asctime)s %(module)s::%(funcName)-20s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
# turn Pika DOWN
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger('lcw')
