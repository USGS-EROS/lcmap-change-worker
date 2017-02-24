import json
from . import messaging
from . import spark
from .app import logger
import msgpack

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
            logger.info("Received message with packed body: {}".format(body))
            unpacked_body = msgpack.unpackb(body)
            logger.info("Launching task for unpacked body {}".format(unpacked_body))
            results = launch_task(cfg, unpacked_body)
            logger.info("Now returning results of type:{}".format(type(results)))
            for result in results:
                packed_result = msgpack.packb(result)
                logger.info("Delivering packed result: {}".format(packed_result))
                logger.info(send(cfg, packed_result, connection))
        except Exception as e:
            logger.error('Change-Worker Execution error. body: {}\nexception: {}'.format(body, e))

    return handler
