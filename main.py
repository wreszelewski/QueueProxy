from queue_proxy.handlers import ClientHandler
from queue_proxy.common import PikaClient
import tornado.ioloop
import tornado.web


if __name__ == "__main__":

    io_loop = tornado.ioloop.IOLoop.instance()
    pc = PikaClient(io_loop)
    pc.connect()
    application = tornado.web.Application([(r"/xmlrpc", ClientHandler, dict(rabbit_client=pc))])
    application.listen(80)
    tornado.ioloop.IOLoop.current().start()
