from cw import app, listen, callback
from .messaging import open_connection, close_connection

def main():
    conn = None
    logger = app.logger
    try:
        # the pika connection created here, ends up being the connection
        # used for sending messages
        conn = open_connection(app.config)
        listen(app.config, callback(app.config, conn), conn)
    except Exception as e:
        logger.error("Problem establishing message queue listener: {}".format(e))
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main()
