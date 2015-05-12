#!/usr/bin/env python3

from queue_proxy.common import PikaClient
import tornado.ioloop
import tornado.web
import sys

if __name__ == "__main__":

    io_loop = tornado.ioloop.IOLoop.instance()
    pc = PikaClient(io_loop, True, sys.argv[1])
    pc.connect()
    tornado.ioloop.IOLoop.current().start()
