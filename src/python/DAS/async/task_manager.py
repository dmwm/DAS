#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : async/task_manager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: TaskManager class relies on asyncio to perform tasks asynchronously
"""

# system modules
import os
import sys
import time
import json
import hashlib

import asyncio

async def async_proc(func, args, kwds):
    func(*args, **kwds)

async def process(func, args, kwds):
    loop = asyncio.get_event_loop()
    task = loop.create_task(async_proc(func, args, kwds))
    res = await task
    return res

async def process_future(func, args, kwds):
    asyncio.ensure_future(async_proc(func, args, kwds))

def test_func(a):
    print("test_func", a, time.time())
#     time.sleep(1)

async def tasks(tid):
    jobs = []
    jobs.append(asyncio.Task(process_future(test_func, (tid,), {})))
    jobs.append(asyncio.Task(process_future(test_func, (tid,), {})))
    jobs.append(asyncio.Task(process_future(test_func, (tid,), {})))
    print("### jobs", jobs, time.time())
    asyncio.gather(*jobs)

async def server():
#     loop = asyncio.get_event_loop()
    for idx in range(3):
        asyncio.Task(tasks(idx))
        await asyncio.sleep(1)

#         jobs = []
#         jobs.append(asyncio.Task(process(test_func, (1,), {})))
#         jobs.append(asyncio.Task(process(test_func, (2,), {})))
#         jobs.append(asyncio.Task(process(test_func, (0,), {})))
#         print("### jobs", jobs, time.time())
#         asyncio.gather(*jobs)
#         await asyncio.sleep(2)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
