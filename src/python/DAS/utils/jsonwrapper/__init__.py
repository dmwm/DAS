#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
JSON wrapper around different JSON python implementations.
We use simplejson (json), cjson and yajl JSON implementation.
"""

__author__ = "Valentin Kuznetsov <vkuznet@gmail.com>"

_module = None
try:
    import yajl
    _module = "yajl"
except:
    pass

try:
    import cjson
    _module = "cjson"
except:
    pass

import json
if  not _module: # use default JSON module
    _module = "json"

# stick so far with cjson, until yajl will be fully tested
_module = "cjson"

def loads(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON decoding API call
    """
    if  _module == 'json':
        return json.loads(idict, **kwargs)
    elif _module == 'cjson':
        return cjson.decode(idict)
    elif _module == 'yajl':
        return yajl.loads(idict, **kwargs)
    else:
        raise Exception("Not support JSON module: %s" % _module)

def load(source):
    """
    Use json.load for back-ward compatibility, since cjson doesn't
    provide this method. The load method works on file-descriptor
    objects.
    """
    if  _module == 'json':
        return json.load(source)
    elif _module == 'cjson':
        data = source.read()
        return cjson.decode(data)
    elif _module == 'yajl':
        return yajl.load(source)
    else:
        raise Exception("Not support JSON module: %s" % _module)

def dumps(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON encoding API call
    """
    if  _module == 'json':
        return json.dumps(idict, **kwargs)
    elif _module == 'cjson':
        return cjson.encode(idict)
    elif _module == 'yajl':
        return yajl.dumps(idict, **kwargs)
    else:
        raise Exception("Not support JSON module: %s" % _module)

def dump(doc, source):
    """
    Use json.dump for back-ward compatibility, since cjson doesn't
    provide this method. The dump method works on file-descriptor
    objects.
    """
    if  _module == 'json':
        return json.dump(doc, source)
    elif _module == 'cjson':
        stj = cjson.encode(doc)
        return source.write(stj)
    elif _module == 'yajl':
        return yajl.dump(doc, source)
    else:
        raise Exception("Not support JSON module: %s" % _module)

class JSONEncoder(object):
    """
    JSONEncoder wrapper
    """
    def __init__(self, **kwargs):
        self.encoder = json.JSONEncoder(**kwargs)
        self.kwargs = kwargs

    def encode(self, idict):
        """Decode JSON method"""
        if  _module == 'cjson' and not self.kwargs:
            return cjson.encode(idict)
        elif _module == 'yajl':
            return yajl.Encoder(self.kwargs).encode(idict)
        return self.encoder.encode(idict)

    def iterencode(self, idict):
        return self.encoder.iterencode(idict)

class JSONDecoder(object):
    """
    JSONDecoder wrapper
    """
    def __init__(self, **kwargs):
        self.decoder = json.JSONDecoder(**kwargs)
        self.kwargs = kwargs

    def decode(self, istring):
        """Decode JSON method"""
        if  _module == 'cjson' and not self.kwargs:
            return cjson.decode(istring)
        elif _module == 'yajl':
            return yajl.Decoder(self.kwargs).decode(istring)
        return self.decoder.decode(istring)

    def raw_decode(self, istring):
        return self.decoder.raw_decode(istring)

