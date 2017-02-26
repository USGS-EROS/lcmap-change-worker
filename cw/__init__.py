# these imports are to provide a clean set of imports for the entire package
# ex: import cw
#     cw.send(...)

from .messaging import send
from .messaging import listen
from .messaging import open_connection
from .messaging import close_connection
from .worker import callback
import sys
import logging
import os
import numpy as np

RABBIT_HOST = os.getenv('LCW_RABBIT_HOST', 'localhost')
RABBIT_PORT = os.getenv('LCW_RABBIT_PORT', 5672)
RABBIT_PORT = int(os.getenv('LCW_RABBIT_PORT', 5672))
RABBIT_QUEUE = os.getenv('LCW_RABBIT_QUEUE', 'local.lcmap.changes.worker')
RABBIT_EXCHANGE = os.getenv('LCW_RABBIT_EXCHANGE', 'local.lcmap.changes.worker')
RABBIT_SSL = os.getenv('LCW_RABBIT_SSL', False)
TILE_SPEC_HOST = os.getenv('LCW_TILE_SPEC_HOST', 'localhost')
TILE_SPEC_PORT = int(os.getenv('LCW_TILE_SPEC_PORT', 5678))
LOG_LEVEL = os.getenv('LCW_LOG_LEVEL', 'INFO')
RESULT_ROUTING_KEY = os.getenv('LCW_RESULT_ROUTING_KEY', 'change-detection-result')

logging.basicConfig(stream=sys.stdout,
                    level=LOG_LEVEL,
                    format='%(asctime)s %(module)s::%(funcName)-20s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# turn Pika DOWN
logging.getLogger("pika").setLevel(logging.WARNING)

#
logging.getLogger("").setLevel('WARN')

# let cw.* modules use configuration value
logger = logging.getLogger('cw')
logger.setLevel(LOG_LEVEL)
