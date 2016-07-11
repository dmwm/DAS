#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: request_manager.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: request manager for DAS web server
"""

# system modules
import time

class Request(object):
    """Simple class to hold request object"""
    def __init__(self, pid, kwds):
        self.pid = pid
        self.kwds = kwds
        self.tstamp = time.time()
        self.tsrepr = time.strftime("%Y%m%d %H:%M:%S", time.localtime())

class RequestManager(object):
    """
    RequestManager holds information about DAS requests. It stores
    pid/kwds pairs into MongoDB, where we assign MongoDB _id to be
    equal to pid (to avoid creating a new index). Since MongoDB
    does not support storage of 'key.attr' as a dict key, we use
    json dumps/loads method to serialize kwds.
    """
    def __init__(self, lifetime=300):
        self.lifetime = lifetime # default 1 hour
        self.store = {}

    def clean(self):
        """Clean on hold collection"""
        for key, req in self.store.items():
            if  req.tstamp < (time.time()-self.lifetime):
                del self.store[key]

    def get(self, pid):
        """Get params for a given pid"""
        req = self.store.get(pid, None)
        if  req:
            return req.kwds

    def add(self, pid, kwds):
        """Add new pid/kwds"""
        self.clean()
        if  not kwds:
            return
        if  self.has_pid(pid):
            return
        self.store[pid] = Request(pid, kwds)

    def remove(self, pid):
        """Remove given pid"""
        self.clean()
        if  pid in self.store:
            del self.store[pid]

    def items(self):
        """Return list of current requests"""
        self.clean()
        for pid, req in self.store.items():
            row = dict(_id=pid, kwds=req.kwds, ts=req.tstamp,
                    timestamp=req.tsrepr)
            yield row

    def has_pid(self, pid):
        """Return true/false for requested pid"""
        return pid in self.store

    def size(self):
        """Return size of the request cache"""
        self.clean()
        return len(self.store.keys())

    def status(self):
        "Return status of RequestManager"
        self.clean()
        requests = [r.kwds['input'] for r in self.store.values()]
        info = {'nrequest': self.size(), 'requests': requests}
        return {'reqmgr': info}
