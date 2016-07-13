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
