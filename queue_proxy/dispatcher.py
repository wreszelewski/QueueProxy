import os
import json
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
import tornado.web

class Dispatcher(object):

    def __init__(self, rabbit_client, backend):
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

        self.channel.queue_bind(self.consume, exchange='request',
                           queue=self.queue_name,
                           routing_key=self.backend)

    def consume(self, arg):
        self.channel.basic_consume(self.resp,
                           queue=self.queue_name,
                           no_ack=True)



    
    def resp(self, ch, method, properties, body):
        self.req_id = json.loads(body.decode('utf-8'))['id']
        path = os.path.join('/storage', 'req', self.req_id)
        with open(path, 'rb') as req:
            data = req.read()
            headers = {"Content-type" : "text/xml"}
            request = HTTPRequest(url="http://localhost:8080/xmlrpc",
                                  method='POST',
                                  body=data,
                                  headers=headers)
            self.client.fetch(request, callback=self.on_request)

    def on_request(self, response):
        path = os.path.join("/storage", "res", self.req_id)
        with open(path, "wb") as res:
            res.write(response.body)
        message = json.dumps({'id' : self.req_id})
        self.channel.basic_publish(exchange='response',
                              routing_key=self.req_id,
                              body=message)
