#!/usr/bin/env python3
# http://aiohttp.readthedocs.io/en/stable/web.html
# http://makina-corpus.com/blog/metier/2015/python-http-server-with-the-new-async-await-syntax
# http://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
# http://stackoverflow.com/questions/22190403/how-could-i-use-requests-in-asyncio
# http://stackoverflow.com/questions/24687061/can-i-somehow-share-an-asynchronous-queue-with-a-subprocess

# system modules
import json
import time
import urllib
import random
import datetime

# async modules
import asyncio
import aiohttp
from aiohttp import web

async def worker(query):
    sleep = random.randint(0, 10) % 2
    msg = "query=%s, sleep=%s, time=%s" % (query, sleep, datetime.datetime.now())
    print("MSG", msg)
    await asyncio.sleep(sleep)
    return msg

async def workflow(query, loop):
    ntasks = random.randint(1, 9)
    tasks = [loop.create_task(worker(query)) for _ in range(0, ntasks)]
    res = await asyncio.gather(*tasks, return_exceptions=True)
    return res

async def process(request):
    params = urllib.parse.parse_qsl(request.query_string)
    query = None
    for item in params:
        if  'query' == item[0]:
            query = item[1]
    if not query:
        raise Exception("No input query")
    pid = 123
    loop = asyncio.get_event_loop()
    try:
        task = loop.create_task(workflow(query, loop))
    except asyncio.CancelledError:
        print('Tasks with query=%s has been canceled' % query)
    finally:
        task.cancel()
    # if we want to wait for task to complete before return we must call
    # res = await task
    # otherwise we let task to be executed in a future and return right away
#    return pid

async def handle(request):
    # process request
#    pid = await process(request)
    asyncio.ensure_future(process(request))
    pid = 123

    # Analyze results
    response_data = {'status': 'ok', 'pid':pid}

    # Build JSON response
    body = json.dumps(response_data).encode('utf-8')
    return web.Response(body=body, content_type="application/json")

def main():
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', handle)

    server = loop.create_server(app.make_handler(), '127.0.0.1', 8000)
    print("Server started at http://127.0.0.1:8000")
    loop.run_until_complete(server)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
