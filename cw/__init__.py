import json
from . import messaging
from . import spark
from .app import logger


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
        try:
            logger.info("Body type:{}".format(type(body.decode('utf-8'))))
            logger.info("Launching task for {}".format(body))
            results = launch_task(cfg, json.loads(body.decode('utf-8')))
            logger.info("Now returning results of type:{}".format(type(results)))
            for result in results:
                logger.info(send(cfg, json.dumps(result)))
        except Exception as e:
            logger.error('Change-Worker Execution error. body: {}\nexception: {}'.format(body.decode('utf-8'), e))

    return handler
