import pika

from .logger import logger

class MessagingException(Exception):
    pass


def open_connection(cfg):
    try:
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=cfg['rabbit-host'],
                                      port=cfg['rabbit-port'],
                                      ssl=cfg['rabbit-ssl']))
    except Exception as e:
        raise MessagingException("problem establishing rabbitmq connection: {}".format(e))


def close_connection(conn):
    if conn is not None and conn.is_open:
        try:
            conn.close()
        except Exception as e:
            logger.error("Problem closing rabbitmq connection: {}".format(e))
    return True


def listen(cfg, callback_handler):
    conn = None
    try:
        conn = open_connection(cfg)
        channel = conn.channel()

        # This needs to be manual ack'ing, research and make sure
        # otherwise we'll get multiple deliveries.
        channel.basic_consume(callback_handler,
                              queue=cfg['rabbit-queue'],
                              no_ack=True)
        channel.start_consuming()
    except Exception as e:
        raise MessagingException("Exception in message listener:{}".format(e))
    finally:
        close_connection(conn)


def send(cfg, message, connection):
    try:
        channel = connection.channel()
        return channel.basic_publish(exchange=cfg['rabbit-exchange'],
                                     routing_key=cfg['rabbit-result-routing-key'],
                                     body=message,
                                     properties=pika.BasicProperties(
                                     delivery_mode=2, # make message persistent
                                     ))
    except Exception as e:
        raise MessagingException("Exception sending message:{}".format(e))
    finally:
        close_connection(connection)
