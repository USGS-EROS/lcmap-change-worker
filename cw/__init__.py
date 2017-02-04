import logging
import os
import sys
from . import messaging
from . import spark

#from messaging.sending import Sending

#__format = '%(asctime)s %(module)s::%(funcName)-20s - %(message)s'
#logging.basicConfig(stream=sys.stdout,
#                    level=logging.DEBUG,
#                    format=__format,
#                    datefmt='%Y-%m-%d %H:%M:%S')
#logger = logging.getLogger('lcw')

config = {'rabbit-host': os.getenv('lcw_rabbit_host', 'localhost'),
          'rabbit-port': os.getenv('lcw_rabbit_port', 5672),
          'rabbit-queue': os.getenv('lcw_rabbit_queue', 'local.lcmap.changes.worker'),
          'rabbit-exchange': os.getenv('lcw_rabbit_exchange', 'local.lcmap.changes.worker'),
          'rabbit-result-routing-key': os.getenv('lcw_rabbit_result_routing_key', 'change-detection-result'),
          'rabbit-ssl': os.getenv('lcw_rabbit_ssl', False),
          'ubid_band_dict' : {
              'tm': {'red': 'band3',
                     'blue': 'band1',
                     'green': 'band2',
                     'nirs': 'band4',
                     'swirs1': 'band5',
                     'swirs2': 'band7',
                     'thermals': 'band6',
                     'qas': 'cfmask'},
              'oli': {'red': 'band4',
                      'blue': 'band2',
                      'green': 'band3',
                      'nirs': 'band5',
                      'swirs1': 'band6',
                      'swirs2': 'band7',
                      'thermals': 'band10',
                      'qas': 'cfmask'}}}

def start_listener(cfg):
    receiver = messaging.Receiving(cfg)
    receiver.start_consuming()

def launch_task(cfg, msg_body):
    # config is a dictionary
    # msg_body needs to be a url
    return spark.run(cfg, msg_body)
    #sender = Sending(config)
    #sender.send(msg_body)
