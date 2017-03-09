from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.view import view_config
from pyramid.response import Response
import multiprocessing as mp
import traceback


@view_config(route_name='health', renderer='json')
def health(request):
    return Response(status=200, body='healthy')


def run():
    try:
        config = Configurator()
        for func, route in (('health', '/health'),):
            config.add_route(func, route)
        config.scan()
        server = make_server('0.0.0.0', 8080, config.make_wsgi_app())
        server.serve_forever()
    except Exception as e:
        # Aid debugging when starting with multiprocess
        traceback.print_exc()
        raise e


def run_http():
    http_process = mp.Process(target=run)
    http_process.start()
    return http_process


def terminate_http(process):
    try:
        process.terminate()
    except AttributeError:
        pass
    return True
