import json
import cw
import pytest
import pika

with open("test/resources/config.json", "r+") as h:
    config = json.loads(h.read())

def connection(config):
    pass

def setup_module(module):
    pass

def teardown_module(module):
    pass

def test_send_receive():
    pass
