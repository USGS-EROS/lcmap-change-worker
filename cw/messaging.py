import cw
import pika

# >>> params = pika.ConnectionParameters(host='lcsrlpnd01', port=5671, ssl=True)
# >>> connect = pika.BlockingConnection(params)

def connection (config):
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=config['rabbitmqhost'],
                                  port=config['rabbitmqport'],
                                  ssl=config['rabbitmqssl']))

class Receiving(object):
    def __init__(self, config):
        self.config = config
        _connection = connection(config)
        self.channel = _connection.channel()
        self.changequeue = config['rabbitmqlistenqueue']

    def callback_handler(self, ch, method, properties, body):
        cw.launch_task(self.config, body)

    def start_consuming(self):
        self.channel.queue_declare(queue=self.changequeue)
        self.channel.basic_consume(self.callback_handler,
                                   queue=self.changequeue,
                                   no_ack=True)
        self.channel.start_consuming()

class Sending(object):
    def __init__(self, config):
        print("config: {}".format(config))
        print("type config: {}".format(type(config)))
        self.config
        self.host = config['rabbitmqhost']
        self.port = config['rabbitmqport']
        self.ssl = config['rabbitmqssl']
        self.queue = config['rabbitmqchangequeue']
        self.routing_key = 'change-detection'
        self.exchange = config['rabbitmqexchange']

    def send(self, message):
        connection = connection(self.config)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        channel.basic_publish(exchange=self.exchange,
                              routing_key=self.routing_key,
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                              ))
        connection.close()
