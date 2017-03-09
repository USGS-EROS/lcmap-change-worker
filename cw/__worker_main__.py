from cw import callback
from cw import logger
from cw import listen
from cw import open_connection
from cw import close_connection
from cw import run_http
from cw import RABBIT_HOST
from cw import RABBIT_PORT
from cw import RABBIT_QUEUE
from cw import RABBIT_EXCHANGE
from cw import RESULT_ROUTING_KEY
import traceback
import sys
import threading


def listen_wrapper():
    conn = None
    try:
        conn = open_connection(RABBIT_HOST, RABBIT_PORT)
        listen(callback(RABBIT_EXCHANGE, RESULT_ROUTING_KEY), conn, RABBIT_QUEUE)
    except Exception as e:
        logger.error('Worker exception: {}'.format(e))
        traceback.print_exc(file=sys.stderr)
    finally:
        close_connection(conn)


def main():
    try:
        listener_thread_name = 'listener_thread'
        listener_thread = threading.Thread(target=listen_wrapper, name=listener_thread_name)
        http_thread = threading.Thread(target=run_http, kwargs={'tname': listener_thread_name})
        listener_thread.start()
        http_thread.start()
    except Exception as e:
        logger.error("Worker exception: {}".format(e))
        traceback.print_exc(file=sys.stderr)

if __name__ == "__main__":
    main()
