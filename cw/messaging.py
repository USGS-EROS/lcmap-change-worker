import cw
import pika

# >>> params = pika.ConnectionParameters(host='lcsrlpnd01', port=5671, ssl=True)
# >>> connect = pika.BlockingConnection(params)

def connection (config):
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=config['rabbit-host'],
                                  port=config['rabbit-port'],
                                  ssl=config['rabbit-ssl']))

class Receiving(object):
    def __init__(self, cfg):
        self.cfg = cfg
        _connection = connection(cfg)
        self.channel = _connection.channel()

    def callback_handler(self, ch, method, properties, body):
        cw.launch_task(self.cfg, body)

    def start_consuming(self):
        #self.channel.queue_declare(queue=self.queue)
        self.channel.basic_consume(self.callback_handler,
                                   queue=self.cfg['rabbit-queue'],
                                   no_ack=True) #This needs to be manual ack'ing, research and make sure otherwise we'll get multiiple deliveries.
        self.channel.start_consuming()

class Sending(object):
    def __init__(self, cfg):
        print("config: {}".format(cfg))
        print("type config: {}".format(type(cfg)))
        self.cfg = cfg

    def send(self, message):
        connection = connection(self.cfg)
        channel = connection.channel()
        # channel.queue_declare(queue=self.queue)
        channel.basic_publish(exchange=self.cfg['rabbit-exchange'],
                              routing_key=self.cfg['rabbit-result-routing-key'],
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                              ))
        connection.close()
