from queue_proxy.handlers import ClientHandler
from queue_proxy.common import PikaClient
import tornado.ioloop
import tornado.web
from boto.s3.connection import S3Connection
from queue_proxy.config import Config

if __name__ == "__main__":

    config = Config()
    client = S3Connection(config['aws_access_key_id'],
                          config['aws_secret_access_key'])
    bucket = client.get_bucket(config["bucket"])
    io_loop = tornado.ioloop.IOLoop.instance()
    pc = PikaClient(io_loop)
    pc.connect()
    application = tornado.web.Application([(r"/xmlrpc", ClientHandler, dict(rabbit_client=pc, s3_bucket=bucket))])
    application.listen(80)
    tornado.ioloop.IOLoop.current().start()
