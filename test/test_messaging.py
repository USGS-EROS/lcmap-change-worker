from pw import messaging, send, RABBIT_HOST, RABBIT_PORT, RABBIT_EXCHANGE, RESULT_ROUTING_KEY, \
    RABBIT_SSL, RABBIT_CONN_ATT, RABBIT_RETRY_DELAY, RABBIT_SOCKET_TIMEOUT, RABBIT_HEARTBEAT


def test_send():
    try:
        conn = messaging.open_connection(RABBIT_HOST, RABBIT_PORT, RABBIT_SSL, RABBIT_CONN_ATT,
                                         RABBIT_RETRY_DELAY, RABBIT_SOCKET_TIMEOUT, RABBIT_HEARTBEAT)
        channel = conn.channel()
        channel.exchange_declare(exchange=RABBIT_EXCHANGE)
        send('heres a message', channel, RABBIT_EXCHANGE, RESULT_ROUTING_KEY)
        assert True
    except Exception as e:
        raise Exception('messaging test test_send has failed: {}'.format(e))