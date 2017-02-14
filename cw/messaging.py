import pika


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
