from cw import callback
from cw import config
from cw import logger
from cw import listen
from cw import open_connection
from cw import close_connection

def main():
    conn = None
    try:
        conn = open_connection(config)
        listen(config, callback(config, conn), conn)
    except Exception as e:
        logger.error("Worker exception: {}".format(e))
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main()
