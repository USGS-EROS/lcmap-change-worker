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


def main():
    conn = None
    try:
        conn = open_connection(RABBIT_HOST, RABBIT_PORT)
        listener_thread = threading.Thread(target=listen, args=(callback(RABBIT_EXCHANGE, RESULT_ROUTING_KEY), conn, RABBIT_QUEUE))
        http_thread = threading.Thread(target=run_http, args=(listener_thread))
        listener_thread.start()
        http_thread.start()

        #listen(callback(RABBIT_EXCHANGE, RESULT_ROUTING_KEY),
        #       conn,
        #       RABBIT_QUEUE)
        # run_http()
    except Exception as e:
        logger.error("Worker exception: {}".format(e))
        traceback.print_exc(file=sys.stderr)
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main()
