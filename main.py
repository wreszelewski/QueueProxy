from queue_proxy.handlers import ClientHandler
import tornado.ioloop
import tornado.web
import pika


class PikaClient(object):

    def __init__(self, io_loop):
        self.io_loop = io_loop

        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.message_count = 0

    def connect(self):

        if self.connecting:
            return

        self.connecting = True

        param = pika.ConnectionParameters(host='localhost')
        self.connection = pika.TornadoConnection(param,
                                            on_open_callback=self.on_connected)
        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):

        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):

        self.channel = channel

    def on_closed(self, connection):

        self.io_loop.stop()

if __name__ == "__main__":

    io_loop = tornado.ioloop.IOLoop.instance()
    pc = PikaClient(io_loop)
    pc.connect()
    application = tornado.web.Application([(r"/xmlrpc", ClientHandler, dict(rabbit_client=pc))])
    application.listen(80)
    tornado.ioloop.IOLoop.current().start()
