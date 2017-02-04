import cw
import pika

def connection (config):
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=config['rabbit-host'],
                                  port=config['rabbit-port'],
                                  ssl=config['rabbit-ssl']))

def listen(cfg, callback_handler):
    conn = None
    try:
        conn = connection(cfg)
        channel = conn.channel()
        channel.basic_consume(callback_handler,
                              queue=cfg['rabbit-queue'],
                              no_ack=True) #This needs to be manual ack'ing, research and make sure otherwise we'll get multiiple deliveries.
        channel.start_consuming()
    except Exception as e:
        print ("Exception in message listener:{}".format(e))
        raise e
    finally:
        if conn is not None:
            conn.close()

def send(cfg, message):
    conn = connection(cfg)
    channel = conn.channel()
    try:
        return channel.basic_publish(exchange=cfg['rabbit-exchange'],
                                     routing_key=cfg['rabbit-result-routing-key'],
                                     body=message,
                                     properties=pika.BasicProperties(
                                     delivery_mode = 2, # make message persistent
                                     ))
    except Exception as e:
        print("Exception sending message:{}".format(e))
        raise e
    finally:
        conn.close()
