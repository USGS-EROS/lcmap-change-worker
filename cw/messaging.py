import cw
import pika

def open_connection (cfg):
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=cfg['rabbit-host'],
                                  port=cfg['rabbit-port'],
                                  ssl=cfg['rabbit-ssl']))

def close_connection(conn):
    if conn is not None and conn.is_open():
        try:
            conn.close()
        except Exception as e:
            pass
    return True
    
def listen(cfg, callback_handler):
    conn = None
    try:
        conn = open_connection(cfg)
        channel = conn.channel()

        #This needs to be manual ack'ing, research and make sure
        # otherwise we'll get multiiple deliveries.
        channel.basic_consume(callback_handler,
                              queue=cfg['rabbit-queue'],
                              no_ack=True)
        channel.start_consuming()

    except Exception as e:
        print ("Exception in message listener:{}".format(e))
        raise e
    finally:
        close_connection(conn)

def send(cfg, message):
    conn = None
    try:
        conn = open_connection(cfg)
        channel = conn.channel()
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
        close_connection(conn)
