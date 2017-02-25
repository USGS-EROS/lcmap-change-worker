import pika
from cw import logger

class MessagingException(Exception):
    pass

def listen(cfg, callback_handler, conn):
    try:
        channel = conn.channel()
        # This needs to be manual ack'ing, research and make sure
        # otherwise we'll get multiple deliveries.
        channel.basic_consume(callback_handler,
                              queue=cfg['rabbit-queue'],
                              no_ack=True)
        channel.start_consuming()
    except Exception as e:
        raise MessagingException("Exception in message listener:{}".format(e))

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

def open_connection(config):
    try:
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=config['rabbit-host'],
                                      port=config['rabbit-port'],
                                      ssl=config['rabbit-ssl']))
    except Exception as e:
        raise MessagingException("problem establishing rabbitmq connection: {}".format(e))

def close_connection(conn):
    if conn is not None and conn.is_open:
        try:
            conn.close()
        except Exception as e:
            logger.error("Problem closing rabbitmq connection: {}".format(e))
    return True
