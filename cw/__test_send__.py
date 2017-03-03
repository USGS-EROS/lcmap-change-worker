from cw import RABBIT_HOST
from cw import RABBIT_PORT
from cw import RABBIT_EXCHANGE
from cw import RESULT_ROUTING_KEY
from cw import logger
from cw import send
from cw import open_connection
from cw import close_connection
import sys

def main(message):
    conn = None
    try:
        conn = open_connection(RABBIT_HOST, RABBIT_PORT)
        channel = conn.channel()
        send(message, channel, RABBIT_EXCHANGE, RESULT_ROUTING_KEY)
    except Exception as e:
        logger.error("Exception sending message: {}".format(e))
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main(sys.argv[1])
