import json
from . import messaging
from . import spark
from .app import logger


def send(cfg, message, connection):
    try:
        return messaging.send(cfg, message, connection)
    except Exception as e:
        logger.error('Change-Worker message queue send error: {}'.format(e))


def listen(cfg, callback, conn):
    try:
        messaging.listen(cfg, callback, conn)
    except Exception as e:
        logger.error('Change-Worker message queue listener error: {}'.format(e))


def launch_task(cfg, msg_body):
    # msg_body needs to be a url
    return spark.run(cfg, msg_body)


def callback(cfg, connection):
    def handler(ch, method, properties, body):
        try:
            #logger.info("Body type:{}".format(type(body.decode('utf-8'))))
            logger.info("Launching task for {}".format(body))
            results = launch_task(cfg, body)
            logger.info("Now returning results of type:{}".format(type(results)))
            for result in results:
                logger.info(send(cfg, result, connection))
        except Exception as e:
            logger.error('Change-Worker Execution error. body: {}\nexception: {}'.format(body, e))

    return handler
