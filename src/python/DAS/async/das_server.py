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
import json
import time
import urllib
import random
import datetime
import argparse
import traceback

# async modules
import asyncio
import aiohttp
from aiohttp import web

# DAS modules
from DAS.core.das_query import DASQuery, check_query
from DAS.core.das_core import DASCore

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--port", action="store",
            dest="port", default=8212, help="DAS server port number")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

async def workflow(dasquery, loop):
    print("### call workflow", dasquery, loop)
    dascore = DASCore()
    print("### dascore", dascore)
    res = None
    try:
        res = dascore.call(dasquery)
        print("### res", res)
    except:
        traceback.print_exc()
#     return res

async def process(request):
    """Process user request"""
    loop = asyncio.get_event_loop()
    params = dict(urllib.parse.parse_qsl(request.query_string))
    uinput = params.get('uinput', None)
    inst = params.get('instance', 'prod/global')
    if not uinput:
        raise Exception("No input query")
    try:
        dasquery = DASQuery(uinput, instance=inst)
    except Exception as exp:
        return {'status':'fail', 'query':uinput}
    pid = dasquery.qhash
    asyncio.ensure_future(workflow(dasquery, loop))
#     try:
#         task = loop.create_task(workflow(dasquery,loop))
#     except asyncio.CancelledError:
#         print('Tasks with query=%s has been canceled' % query)
#     finally:
#         task.cancel()
    # if we want to wait for task to complete before return we must call
    # res = await task
    # otherwise we let task to be executed in a future and return right away
    data = {'status': 'ok', 'pid':pid}
    return data

async def handle(request):
    # process request
    data = await process(request)
    return data

async def handleRequest(request):
    data = await handle(request)
    # will handle web request,
    page = json.dumps(data)
    return page

async def handleCache(request):
    data = await handle(request)
    # Build JSON response
    body = json.dumps(data).encode('utf-8')
    return web.Response(body=body, content_type="application/json")

async def handleRedirect(request):
    return
async def handleFAQ(request):
    return
async def handleCli(request):
    return
async def handleError(request):
    return
async def handleStatus(request):
    return
async def handleCheckPid(request):
    return

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    port = int(opts.port)

    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/redirect', handleRedirect)
    app.router.add_route('GET', '/request', handleRequest)
    app.router.add_route('GET', '/cache', handleCache)
    app.router.add_route('GET', '/faq', handleFAQ)
    app.router.add_route('GET', '/cli', handleCli)
    app.router.add_route('GET', '/error', handleError)
    app.router.add_route('GET', '/status', handleStatus)
    app.router.add_route('GET', '/check_pid', handleCheckPid)

    server = loop.create_server(app.make_handler(), '127.0.0.1', port)
    print("### Server started at http://127.0.0.1:%s" % port)
    loop.run_until_complete(server)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
