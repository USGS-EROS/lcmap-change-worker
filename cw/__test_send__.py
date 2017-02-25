from cw import config
from cw import logger
from cw import send
from cw import open_connection
from cw import close_connection
import sys

def main(message):
    conn = None
    try:
        conn = open_connection(config)
        send(config, message, conn)
    except Exception as e:
        logger.error("Exception sending message: {}".format(e))
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main(sys.argv[1])
