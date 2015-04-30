#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das_pymongo.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: helper module to deal with pymongo options
"""

from pymongo import version as pymongo_version

# pymongo driver changed usage of its options here we declare them
# based on its version
PYMVER = pymongo_version.split('.')[0]
if  PYMVER == '2':
    PYMONGO_OPTS = {'exhaust': True}
    PYMONGO_NOEXHAUST = {'exhaust': False}
    class MongoOpts():
        def __init__(self, **kwds):
            self.w = kwds.get('w', 1)
            self.psize = kwds.get('psize', 300)
        def opts(self):
            return dict(w=self.w, max_pool_size=self.psize)
elif  PYMVER == '3':
    from pymongo.cursor import CursorType
    PYMONGO_OPTS = {'cursor_type': CursorType.EXHAUST}
    PYMONGO_NOEXHAUST = {'cursor_type': CursorType.NON_TAILABLE}
    class MongoOpts():
        def __init__(self, **kwds):
            self.w = kwds.get('w', 1)
            self.psize = kwds.get('psize', 300)
        def opts(self):
            return dict(w=self.w, maxPoolSize=self.psize)
else:
    raise Exception('Unsupported pymongo version, %s' % pymongo_version)
