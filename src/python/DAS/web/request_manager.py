#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: request_manager.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: Persistent request manager for DAS web server
"""

class RequestManager(object):
    """RequestManager holds information about DAS requests"""
    def __init__(self):
        self._requests = {} # to be filled at run time

    def has_key(self, pid):
        """Check existence of pid"""
        if  self._requests.has_key(pid):
            return True
        
    def get(self, pid):
        """Get params for a given pid"""
        return self._requests[pid]
        
    def add(self, pid, kwds):
        """Add new pid/kwds"""
        self._requests[pid] = kwds
        
    def remove(self, pid):
        """Remove given pid"""
        try:
            del self._requests[pid]
        except:
            pass
        
    def lookup(self, pid):
        """Lookup given pid"""
        return self._requests[pid]
        
    def items(self):
        """Return list of current requests"""
        for pid, kwds in self._requests.items():
            yield pid, kwds

    def size(self):
        """Return size of the request cache"""
        return len([p for p, _ in self.items()])
