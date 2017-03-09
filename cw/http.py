from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.view import view_config
from pyramid.response import Response
import threading


@view_config(route_name='health', renderer='json')
def health(request):
    status, msg = 500, 'listener is dead'
    for thread in threading.enumerate():
        if thread.name == request.registry.settings['listen_thread']:
            if thread.is_alive():
                status, msg = 200, 'health'
    return Response(status=status, body=msg)


def run_http(tname=None):
    config = Configurator(settings={'listen_thread': tname})
    for func, route in (('health', '/health'),):
        config.add_route(func, route)
    config.scan()
    server = make_server('0.0.0.0', 8080, config.make_wsgi_app())
    server.serve_forever()
