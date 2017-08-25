import pika
import pw


class MessagingException(Exception):
    pass


def listen(callback_handler, conn, queue):
    channel = conn.channel()
    # This needs to be manual ack'ing, research and make sure
    # otherwise we'll get multiple deliveries.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback_handler, queue=queue, no_ack=False)
    channel.start_consuming()


def send(message, channel, exchange, routing_key):
    return channel.basic_publish(exchange=exchange,
                                 routing_key=routing_key,
                                 body=message,
                                 properties=pika.BasicProperties(
                                     delivery_mode=2, # make message persistent
                                 ))


def open_connection(host, port, ssl, connection_attempts, retry_delay, socket_timeout):
    try:
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=host,
                                      port=port,
                                      ssl=ssl,
                                      connection_attempts=connection_attempts,
                                      retry_delay=retry_delay,
                                      socket_timeout=socket_timeout)
        )
        # blocked_connection_timeout for ConnectionParameters only available in master, not officially released
    except Exception as e:
        raise MessagingException("problem establishing rabbitmq connection: {}".format(e))


def close_connection(conn):
    if conn is not None and conn.is_open:
        try:
            conn.close()
        except Exception as e:
            pw.logger.error("Problem closing rabbitmq connection: {}".format(e))
    return True
