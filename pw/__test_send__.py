from pw import RABBIT_HOST
from pw import RABBIT_PORT
from pw import RABBIT_EXCHANGE
from pw import RESULT_ROUTING_KEY
from pw import logger
from pw import send
from pw import open_connection
from pw import close_connection
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
