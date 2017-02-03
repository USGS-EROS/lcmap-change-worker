import ast
import cw
import pika

# >>> params = pika.ConnectionParameters(host='lcsrlpnd01', port=5671, ssl=True)
# >>> connect = pika.BlockingConnection(params)

class Receiving(object):
    def __init__(self, sysargs):
        self._config = ast.literal_eval(sysargs)
        _connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._config['rabbitmqhost'],
                                                                        port=self._config['rabbitmqport'],
                                                                        ssl=self._config['rabbitmqssl']))
        self.channel = _connection.channel()
        self.changequeue = self._config['rabbitmqlistenqueue']

    def callback_handler(self, ch, method, properties, body):
        cw.launch_task(self._config, body)

    def start_consuming(self):
        self.channel.queue_declare(queue=self.changequeue)
        self.channel.basic_consume(self.callback_handler, queue=self.changequeue, no_ack=True)
        self.channel.start_consuming()


class Sending(object):
    def __init__(self, sysargs):
        print "sysargs: {}".format(sysargs)
        print "type sysargs: {}".format(type(sysargs))
        self.host = sysargs['rabbitmqhost']
        self.port = sysargs['rabbitmqport']
        self.ssl = sysargs['rabbitmqssl']
        self.queue = sysargs['rabbitmqchangequeue']

    def send(self, message):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                       port=self.port,
                                                                       ssl=self.ssl))
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        channel.basic_publish(exchange='',
                              routing_key=self.queue,
                              body=message)
        connection.close()
