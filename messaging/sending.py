#!/usr/bin/env python
import pika
from util import get_cfg


class Sending(object):
    def __init__(self, config=None):
        _config = config if config else get_cfg()['lcmap']
        _connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=_config['rabbitmqhost'],
                                      port=_config['rabbitmqport']))
        self.channel = _connection.channel()
        self.changequeue = _config['rabbitmqchangequeue']
        self.routingkey = 'change-worker'

    def send(self, message, queue=None):
        _queue = queue if queue else self.changequeue
        self.channel.queue_declare(queue=_queue)
        self.channel.basic_publish(exchange='',
                                   routing_key=self.routingkey,
                                   body=message)
        self.connection.close()




