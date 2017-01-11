#!/usr/bin/env python
import pika
import ast

from tasking import launch_task


class Receiving(object):
    def __init__(self, sysargs):
        self._config = ast.literal_eval(sysargs)
        _connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._config['rabbitmqhost'],
                                      port=self._config['rabbitmqport']))
        self.channel = _connection.channel()
        self.changequeue = self._config['rabbitmqchangequeue']

    def callback_handler(self, *args):
        launch_task(self._config, args)

    def start_consuming(self):
        self.channel.queue_declare(queue=self.changequeue)
        self.channel.basic_consume(self.callback_handler, queue=self.changequeue, no_ack=True)
        self.channel.start_consuming()
