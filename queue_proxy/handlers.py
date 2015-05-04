import tornado.web
import tornado.gen
import uuid
import os
import json

class ClientHandler(tornado.web.RequestHandler):

    def initialize(self, rabbit_client):
        self.rabbit_client = rabbit_client

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

        req_uuid = str(uuid.uuid4())
        print(req_uuid)
        storage_path = os.path.join("/storage", 'req', req_uuid)
        with open(storage_path, "wb") as req_content:
            req_content.write(self.request.body)
 
        self.message = json.dumps({'id' : req_uuid})

        self.channel.queue_bind(self.consume, exchange='response',
                           queue=self.queue_name,
                           routing_key=req_uuid)

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
        with open(path, 'rb') as res:
            content = res.read()
            self.write(content)
        ch.queue_delete(queue=self.queue_name)
        ch.close()
        self.finish()
