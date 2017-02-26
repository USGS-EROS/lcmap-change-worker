from cw import callback
from cw import logger
from cw import listen
from cw import open_connection
from cw import close_connection
from cw import RABBIT_HOST
from cw import RABBIT_PORT
from cw import RABBIT_EXCHANGE
from cw import RESULT_ROUTING_KEY
from cw import RABBIT_QUEUE

def main():
    conn = None
    try:
        conn = open_connection(RABBIT_HOST, RABBIT_PORT)
        listen(callback(conn, RABBIT_EXCHANGE, RESULT_ROUTING_KEY),
               conn,
               RABBIT_QUEUE)
    except Exception as e:
        logger.error("Worker exception: {}".format(e))
    finally:
        close_connection(conn)

if __name__ == "__main__":
    main()
