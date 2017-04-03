# these imports are to provide a clean set of imports for the entire package
# ex: import pw
#     pw.send(...)
from .worker import spark_job
from ccd import algorithm as ccd_alg_version

import sys
import logging
import os

LOG_LEVEL         = os.getenv('LPW_LOG_LEVEL', 'INFO')
DB_CONTACT_POINTS = os.getenv('DB_CONTACT_POINTS', '0.0.0.0')
DB_KEYSPACE       = os.getenv('DB_KEYSPACE', 'lcmap_changes_local')
DB_PASSWORD       = os.getenv('DB_PASSWORD', '')
DB_USERNAME       = os.getenv('DB_USERNAME', '')

RESULT_ROUTING_KEY = ccd_alg_version

logging.basicConfig(stream=sys.stdout,
                    level=LOG_LEVEL,
                    format='%(asctime)s %(module)s::%(funcName)-20s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# default all loggers to WARNING then explictly override below
logging.getLogger("").setLevel(logging.WARNING)

# let pw.* modules use configuration value
logger = logging.getLogger('pw')
logger.setLevel(LOG_LEVEL)
