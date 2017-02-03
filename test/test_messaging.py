import ast
import cw
import pytest
import pika

def config():
    with open("resources/config.txt", "rb+") as h:
        return ast.literal_eval(h.read())

def connection(config):
    pass

def setup_module(module):
    pass

def teardown_module(module):
    pass

def test_send_receive():
    pass
