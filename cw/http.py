from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.view import view_config


@view_config(route_name='health', renderer='json')
def health(request):
    return {'response': request.registry.settings['thread']}


def run_http(t1):
    config = Configurator(settings={'thread': t1})
    for func, route in (('health', '/health'),):
        config.add_route(func, route)
    config.scan()
    server = make_server('0.0.0.0', 8080, config.make_wsgi_app())
    server.serve_forever()
