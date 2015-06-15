import pika
from queue_proxy.dispatcher import Dispatcher

class PikaClient(object):

    def __init__(self, io_loop, dispatcher = False, backend=None):
        self.io_loop = io_loop

        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.message_count = 0
        self.dispatcher = dispatcher
        self.backend = backend

    def connect(self):

        if self.connecting:
            return

        self.connecting = True

        param = pika.ConnectionParameters(host='internal-Infrastructure-673672297.eu-west-1.elb.amazonaws.com', heartbeat_interval=30)
        self.connection = pika.TornadoConnection(param,
                                            on_open_callback=self.on_connected)
        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):

        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)
        if self.dispatcher:
            Dispatcher(self, self.backend)

    def on_channel_open(self, channel):

        self.channel = channel

    def on_closed(self, connection, arg1, arg2):

        self.io_loop.stop()
