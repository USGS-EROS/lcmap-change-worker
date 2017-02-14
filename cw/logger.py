import logging
import sys


def get_logger(log_level):
    __format = '%(asctime)s %(module)s::%(funcName)-20s - %(message)s'
    logging.basicConfig(stream=sys.stdout,
                        format=__format,
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('lcw')
    logger.setLevel(log_level)
    return logger
