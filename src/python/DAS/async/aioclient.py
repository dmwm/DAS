#!/usr/bin/env python3
# http://aiohttp.readthedocs.io/en/stable/web.html
# http://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

# system modules
import argparse

# modified fetch function with semaphore
import random
import asyncio
from aiohttp import ClientSession


class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--nreq", action="store",
            dest="nreq", default=10, help="Number of clients")
        self.parser.add_argument("--nsem", action="store",
            dest="nsem", default=1000, help="Number of sem")

async def fetch(url):
    async with ClientSession() as session:
        async with session.get(url) as response:
            delay = response.headers.get("DELAY")
            date = response.headers.get("DATE")
            print("{}:{} with delay {}".format(date, response.url, delay))
            return await response.read()


async def bound_fetch(sem, url):
    sleep = 0.5
    for idx in range(5): # make 5 attempts
        try:
            # getter function with semaphore
            async with sem:
                return await fetch(url)
        except:
            print("unable to fetch %s, sleep %s" % (url, idx))
            await asyncio.sleep(sleep)

async def run(loop, nreq, nsem):
    url = "http://localhost:8000/?query=test{}"
    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(nsem)
    for i in range(nreq):
        # pass Semaphore to every GET request
        task = asyncio.ensure_future(bound_fetch(sem, url.format(i)))
        tasks.append(task)

    responses = asyncio.gather(*tasks)
    await responses

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    number = int(opts.nreq)
    nsem = int(opts.nsem)
    loop = asyncio.get_event_loop()

    future = asyncio.ensure_future(run(loop, number, nsem))
    loop.run_until_complete(future)

if __name__ == '__main__':
    main()
