#!/usr/bin/env python
import pika
import ast


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




