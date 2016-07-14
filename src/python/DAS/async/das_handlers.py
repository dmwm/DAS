#!/usr/bin/env python3
"""
DAS async handlers
"""

# system modules
import json
import time
import urllib
import random
import datetime
import traceback

# async modules
import asyncio
from aiohttp import web

# DAS modules
from DAS.core.das_query import DASQuery, check_query
from DAS.core.das_core import DASCore

async def workflow(dascore, dasquery, loop):
    print("### call workflow", dasquery, loop)
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
    uinput = params.get('input', None)
    inst = params.get('instance', 'prod/global')
    if not uinput:
        raise Exception("No input query")
    try:
        dasquery = DASQuery(uinput, instance=inst)
    except Exception as exp:
        traceback.print_exc()
        return {'status':'fail', 'query':uinput}
    pid = dasquery.qhash
    time0 = time.time()
    dascore = DASCore()
    print("### dascore", dascore, time.time()-time0)
    if  dascore.incache(dasquery):
        res = [r for r in dascore.get_from_cache(dasquery)]
        data = {'status':'ok', 'data': res, 'nresults':len(res), 'mongo_query':dasquery.mongo_query}
        return data
    asyncio.ensure_future(workflow(dascore, dasquery, loop))
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

HANDLERS = [
    ('GET', '/redirect', handleRedirect),
    ('GET', '/request', handleRequest),
    ('GET', '/cache', handleCache),
    ('GET', '/faq', handleFAQ),
    ('GET', '/cli', handleCli),
    ('GET', '/error', handleError),
    ('GET', '/status', handleStatus),
    ('GET', '/check_pid', handleCheckPid)
        ]
