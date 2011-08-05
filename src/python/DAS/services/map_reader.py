#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Data-provider map reader. Service maps are represented in YAML format.
"""
__revision__ = "$Id: map_reader.py,v 1.9 2010/03/25 20:51:31 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

import yaml
import time

def read_service_map(filename, field="uri"):
    """
    Read service map file and construct DAS record for MappingDB.
    """
    record = {}
    system = ''
    url    = ''
    frmt   = ''
    notations = ''
    wild   = '*'
    ckey   = None
    cert   = None
    with open(filename, 'r') as apimap:
        for metric in yaml.load_all(apimap.read()):
            if  not ckey:
                ckey = metric.get('ckey')
            if  not cert:
                cert = metric.get('cert')
            if  metric.has_key('system'):
                system = metric['system']
            if  metric.has_key('url'):
                url = metric['url']
            if  metric.has_key('wild_card'):
                wild   = metric['wild_card']
            if  metric.has_key('format'):
                frmt = metric['format']
            if  field == 'uri' and metric.has_key('urn'):
                params = metric['params']
                urn    = metric['urn']
                expire = metric.get('expire', 600) # default 10 minutes
                apitag = metric.get('apitag', None)
                record = dict(url=url, system=system, expire=expire,
                                urn=urn, params=params, apitag=apitag,
                                format=frmt, wild_card=wild,
                                created=time.time())
                if  metric.has_key('das2api'):
                    record['das2api'] = metric['das2api']
                else:
                    record['das2api'] = []
                if  ckey:
                    record['ckey'] = ckey
                if  cert:
                    record['cert'] = cert
                if  metric.has_key('daskeys'):
                    record['daskeys'] = metric['daskeys']
                else:
                    msg = "map doesn't provide daskeys"
                    print metric
                    raise Exception(msg)
                if  validator(record):
                    yield record
            if  field == 'notations' and metric.has_key('notations'):
                notations = metric['notations']
                record = dict(notations=notations,
                                system=system, created=time.time())
                if  validator(record):
                    yield record
            if  field == 'presentation' and metric.has_key('presentation'):
                record = dict(presentation=metric['presentation'],
                                created=time.time())
                if  validator(record):
                    yield record
        if  field == 'notations' and not notations and system: # no notations
            record = dict(notations=[], system=system, created=time.time())
            if  validator(record):
                yield record

def validator(record):
    """
    DAS map validator
    """
    if  record.has_key('notations'):
        must_have_keys = ['system', 'notations', 'created']
    elif record.has_key('presentation'):
        must_have_keys = ['presentation', 'created']
    else:
        must_have_keys = ['system', 'format', 'urn', 'url', 'expire', 
                            'params', 'daskeys', 'created']
    if  set(record.keys()) & set(must_have_keys) != set(must_have_keys):
        return False
    return True
