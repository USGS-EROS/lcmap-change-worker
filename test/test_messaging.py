from cw import messaging, send, RABBIT_HOST, RABBIT_PORT, RABBIT_EXCHANGE, RESULT_ROUTING_KEY


def test_send():
    try:
        conn = messaging.open_connection(RABBIT_HOST, RABBIT_PORT)
        channel = conn.channel()
        channel.exchange_declare(exchange=RABBIT_EXCHANGE)
        send('heres a message', channel, RABBIT_EXCHANGE, RESULT_ROUTING_KEY)
        assert True
    except Exception as e:
        raise Exception('messaging test test_send has failed: {}'.format(e))