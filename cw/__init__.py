import codecs
import logging
import json
import os
import sys
import traceback
import numpy as np
from . import messaging
from . import spark
from .logger import get_logger

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

logger = get_logger(config['log-level'])


def send(cfg, message):
    conn = None
    try:
        conn = messaging.open_connection(cfg)
        return messaging.send(cfg, message, conn)
    except Exception as e:
        logger.error('Change-Worker message queue send error: {}'.format(e))
    finally:
        messaging.close_connection(conn)


def listen(cfg, callback):
    try:
        messaging.listen(cfg, callback)
    except Exception as e:
        logger.error('Change-Worker message queue listener error: {}'.format(e))


def launch_task(cfg, msg_body):
    # msg_body needs to be a url
    return spark.run(cfg, msg_body)


def callback(cfg):
    def handler(ch, method, properties, body):
        conn = None
        try:
            logger.info("Body type:{}".format(type(body.decode('utf-8'))))
            logger.info("Launching task for {}".format(body))
            results = launch_task(cfg, json.loads(body.decode('utf-8')))
            logger.info("Now returning results of type:{}".format(type(results)))
            for result in results:
                logger.info(send(cfg, json.dumps(result)))
        except Exception as e:
            logger.error('Change-Worker Execution error. body: {}\nexception: {}'.format(body.decode('utf-8')))

    return handler
