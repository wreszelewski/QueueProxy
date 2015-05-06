import tornado.web
import tornado.gen
import uuid
import os
import json
from boto.s3.key import Key
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps

EXECUTOR = ThreadPoolExecutor(max_workers=10)

def unblock(f):

    @tornado.web.asynchronous
    @wraps(f)
    def wrapper(*args, **kwargs):

        def callback(future):
            kwargs['callback'](future.result())

        EXECUTOR.submit(
            partial(f, *args, **kwargs)
        ).add_done_callback(
            lambda future: tornado.ioloop.IOLoop.instance().add_callback(
                partial(callback, future)))

    return wrapper

class ClientHandler(tornado.web.RequestHandler):

    @unblock
    def store_s3(self, path, content, callback):
        key = Key(self.s3_bucket)
        key.key = path
        key.set_contents_from_string(content)
         
    @unblock
    def get_s3(self, path, callback):
        key = Key(self.s3_bucket)
        key.key = path
        content = key.get_contents_as_string()
        key.delete()
        return content

    def initialize(self, rabbit_client, s3_bucket):
        self.rabbit_client = rabbit_client
        self.s3_bucket = s3_bucket

    def prepare_exchanges(self, channel):
        self.channel = channel
        self.channel.exchange_declare(exchange="request", type='direct')
        self.channel.exchange_declare(exchange="response", type='direct')
        self.channel.queue_declare(self.process_request, exclusive=True)

    @tornado.web.asynchronous
    def post(self):
        self.rabbit_client.connection.channel(self.prepare_exchanges)

    def process_request(self, queue):
        self.queue_name = queue.method.queue

        self.req_uuid = str(uuid.uuid4())
        storage_path = os.path.join("/storage", 'req', self.req_uuid)
        self.store_s3(storage_path, self.request.body, callback=self.data_saved)

    def data_saved(self, response):
 
        self.message = json.dumps({'id' : self.req_uuid})

        self.channel.queue_bind(self.consume, exchange='response',
                           queue=self.queue_name,
                           routing_key=self.req_uuid)

    def consume(self, arg):
        self.channel.basic_publish(exchange='request',
                              routing_key=self.request.host,
                              body=self.message)
        self.channel.basic_consume(self.resp,
                           queue=self.queue_name,
                           no_ack=True)




    def resp(self, ch, method, properties, body): 
        data = json.loads(body.decode('utf-8'))
        path = os.path.join('/storage', 'res', data['id'])
        ch.queue_delete(queue=self.queue_name)
        ch.close()
        self.get_s3(path, callback=self.complete_request)

    def complete_request(self, response):
        self.write(response)
        self.finish()
