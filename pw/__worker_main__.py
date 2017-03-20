from pw import logger
from pw import run_http
from pw import spark_job
from pw import terminate_http
from pw import HTTP_PORT
from pw import DB_CONTACT_POINTS
from pw import DB_KEYSPACE
from pw import DB_PASSWORD
from pw import DB_USERNAME


import traceback
import sys


def main():
    http_process = None
    try:
        http_process = run_http(HTTP_PORT)
        spark_job(sys.argv[1:])
    except Exception as e:
        logger.error('Worker exception: {}'.format(e))
        traceback.print_exc(file=sys.stderr)
    finally:
        terminate_http(http_process)

if __name__ == "__main__":
    main()
