import time
from jadi import component
from aj.api.http import url, HttpPlugin

@component(HttpPlugin)
class Handler(HttpPlugin):
    """
    Plugin's HTTP handler class
    """

    @url(r'/api/test-download')
    def handle_execute(self, http_context):
        http_context.respond(200)
        if http_context.method == "GET":
            yield b'123'

        for l in http_context.body:
            print('---- 1 -----')
            time.sleep(0.1)
            yield ' -a ' + str(l)

    @url(r'/api/test-file/(?P<path>.+)')
    def handle_file(self, http_context, plugin=None, path=None):
        if '..' in path:
            return http_context.respond_not_found()
        return http_context.file(path, stream=True)
