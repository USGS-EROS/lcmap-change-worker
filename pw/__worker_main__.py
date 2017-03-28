from pw import logger
from pw import run_http
from pw import HTTP_PORT

import traceback
import sys


def main():
    try:
        run_http(HTTP_PORT)
    except Exception as e:
        logger.error('Worker exception running http: {}'.format(e))
        traceback.print_exc(file=sys.stderr)


if __name__ == "__main__":
    main()
