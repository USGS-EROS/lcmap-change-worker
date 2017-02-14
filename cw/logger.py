import logging
import sys

__format = '%(asctime)s %(module)s::%(funcName)-20s - %(message)s'
logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format=__format,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('lcw')

