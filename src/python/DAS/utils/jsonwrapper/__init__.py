#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
JSON wrapper around different JSON python implementation.
So far we use simplejson (json) and cjson, other modules can be
added in addition.
"""

__revision__ = "$Id: __init__.py,v 1.3 2010/01/04 19:01:21 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"
__all__ = ['loads', 'dumps', 'JSONEncoder']

try:
    import cjson
except:
    pass
try:
    import json # python 2.6 and later
except:
    import simplejson as json # python 2.5 and earlier

_module = "cjson"

def loads(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON decoding API call
    """
    if  _module == 'json':
        return json.loads(idict, **kwargs)
    elif _module == 'cjson':
        return cjson.decode(idict)

def load(source):
    """
    Use json.load for back-ward compatibility, since cjson doesn't
    provide this method. The load method works on file-descriptor
    objects.
    """
    return json.load(source)

def JSONEncoder(*args, **kwargs):
    """
    This class is only available in non-cjson
    """
    return json.JSONEncoder(*args, **kwargs)

def dumps(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON encoding API call
    """
    if  _module == 'json':
        return json.dumps(idict, **kwargs)
    elif _module == 'cjson':
        try:
            data = cjson.encode(idict)
            return data
        except:
            try: 
                data = json.dumps(idict, **kwargs)
                return data
            except:
                msg = 'Unable to ecode %s' % idict
                raise Exception(msg)

