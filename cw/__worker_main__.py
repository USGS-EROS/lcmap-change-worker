from cw import callback
from cw import logger
from cw import listen
from cw import open_connection
from cw import close_connection
from cw import run_http
from cw import terminate_http
from cw import RABBIT_HOST
from cw import RABBIT_PORT
from cw import RABBIT_QUEUE
from cw import RABBIT_EXCHANGE
from cw import RESULT_ROUTING_KEY
import traceback
import sys


def main():
    conn = None
    http_process = None
    try:
        http_process = run_http()
        conn = open_connection(RABBIT_HOST, RABBIT_PORT)
        listen(callback(RABBIT_EXCHANGE, RESULT_ROUTING_KEY), conn, RABBIT_QUEUE)
    except Exception as e:
        logger.error('Worker exception: {}'.format(e))
        traceback.print_exc(file=sys.stderr)
    finally:
        close_connection(conn)
        terminate_http(http_process)


if __name__ == "__main__":
    main()
