import sys
from cw import app, send
from .messaging import open_connection, close_connection

def main(message):
    conn = None
    logger = app.logger
    try:
        conn = open_connection(app.config)
        send(app.config, message, conn)
    except Exception as e:
        logger.error("Problem sending message: {}".format(e))
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main(sys.argv[1])
