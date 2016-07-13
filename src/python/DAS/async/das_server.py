#!/usr/bin/env python3
"""
DAS web server based on asyncio/aiohttp.

# References:
- http://aiohttp.readthedocs.io/en/stable/web.html
- http://makina-corpus.com/blog/metier/2015/python-http-server-with-the-new-async-await-syntax
- http://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
- http://stackoverflow.com/questions/22190403/how-could-i-use-requests-in-asyncio
- http://stackoverflow.com/questions/24687061/can-i-somehow-share-an-asynchronous-queue-with-a-subprocess
"""

# system modules
import argparse

# async modules
import asyncio
from aiohttp import web

# DAS modules
from DAS.async.das_handlers import HANDLERS

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--host", action="store",
            dest="host", default='127.0.0.1', help="DAS server host IP address")
        self.parser.add_argument("--port", action="store",
            dest="port", default=8212, help="DAS server port number")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def dasserver(host, port, verbose=False):
    "DAS async web server"
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    for (method, path, handler) in HANDLERS:
        if  verbose:
            print('### Register: %s %s %s' % (method, path, handler))
        app.router.add_route(method, path, handler)

    server = loop.create_server(app.make_handler(), host, port)
    print('### Server started at http://%s:%s' % (host, port))
    loop.run_until_complete(server)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    port = int(opts.port)
    host = opts.host.replace('http://', '').replace('https://', '')
    dasserver(host, port, opts.verbose)

if __name__ == '__main__':
    main()
