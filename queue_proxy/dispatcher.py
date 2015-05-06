import os
import json
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
import tornado.web
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import time
from functools import partial
from queue_proxy.config import Config

class Dispatcher(object):

    def __init__(self, rabbit_client, backend):
        self.config = Config()
        client = S3Connection(self.config['aws_access_key_id'],
                              self.config['aws_secret_access_key'])
        self.bucket = client.get_bucket(self.config["bucket"])
        self.rabbit_client = rabbit_client
        self.backend = backend
        self.client = AsyncHTTPClient()
        self.rabbit_client.connection.channel(self.prepare_exchanges)

    def prepare_exchanges(self, channel):
        self.channel = channel
        self.channel.exchange_declare(exchange="request", type='direct')
        self.channel.exchange_declare(exchange="response", type='direct')
        self.channel.queue_declare(self.process_request, exclusive=True)

    def process_request(self, queue):
        self.queue_name = queue.method.queue
        self.channel.basic_qos(prefetch_count=self.config['prefetch'])
        self.channel.queue_bind(self.consume, exchange='request',
                           queue=self.queue_name,
                           routing_key=self.backend)

    def consume(self, arg):
        self.channel.basic_consume(self.resp,
                           queue=self.queue_name)



    
    def resp(self, ch, method, properties, body):
        req_id = json.loads(body.decode('utf-8'))['id']
        path = os.path.join('/storage', 'req', req_id)
        key = Key(self.bucket)
        key.key = path
        data = key.get_contents_as_string()
        headers = {"Content-type" : "text/xml"}
        request = HTTPRequest(url="http://localhost:8080/xmlrpc",
                              method='POST',
                              body=data,
                              headers=headers)
        key.delete()
        on_request = partial(self.on_request, req_id=req_id, ch=ch, method=method)
        self.client.fetch(request, callback=on_request)

    def on_request(self, response, req_id, ch, method):
        path = os.path.join("/storage", "res", req_id)
        key = Key(self.bucket)
        key.key = path
        data = key.set_contents_from_string(response.body)
        message = json.dumps({'id' : req_id})
        self.channel.basic_publish(exchange='response',
                              routing_key=req_id,
                              body=message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
