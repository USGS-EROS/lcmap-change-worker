import pika
import cw

class MessagingException(Exception):
    pass

def listen(callback_handler, conn, queue):
    channel = conn.channel()
    # This needs to be manual ack'ing, research and make sure
    # otherwise we'll get multiple deliveries.
    channel.basic_consume(callback_handler,
                          queue=queue,
                          no_ack=True)
    channel.start_consuming()

def send(message, connection, exchange, routing_key):
    channel = connection.channel()
    return channel.basic_publish(exchange=exchange,
                                 routing_key=routing_key,
                                 body=message,
                                 properties=pika.BasicProperties(
                                     delivery_mode=2, # make message persistent
                                 ))

def open_connection(host, port, ssl=False):
    try:
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=host,
                                      port=port,
                                      ssl=ssl))
    except Exception as e:
        raise MessagingException("problem establishing rabbitmq connection: {}".format(e))

def close_connection(conn):
    if conn is not None and conn.is_open:
        try:
            conn.close()
        except Exception as e:
            cw.logger.error("Problem closing rabbitmq connection: {}".format(e))
    return True
