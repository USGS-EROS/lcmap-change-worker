#!/usr/bin/env python

import spark
#from messaging.sending import Sending

def launch_task(config, msg_body):
    # config is a dictionary
    # msg_body needs to be a url
    return spark.run(config, msg_body)
    #sender = Sending(config)
    #sender.send(msg_body)
