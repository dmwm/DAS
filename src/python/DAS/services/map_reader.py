#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0912
"""
Data-provider map reader. Service maps are represented in YAML format.
Each map should provide the following fields:

    - system name, a common name of the data-provider
    - url, the URL of the API
    - expire, the expiration time in seconds
    - urn, alias to identify uniquely API/URL pair
    - lookup, the name of das key this entry covers
    - das_map, the DAS map list of dictionaries, where each entry consists of
      - das_key, the key name used in DAS QL
      - rec_key, the key name used in DAS data records
      - api_arg, the name of API argument
      - pattern, optional pattern
    - notations (the map of notations)
    - presentation (the map for presentation layer)
"""
__author__ = "Valentin Kuznetsov"

import yaml
import time

def read_service_map(filename, field="uri"):
    """
    Read service map file and construct DAS record for MappingDB.
    """
    tstamp = time.time()
    record = {}
    system = ''
    url    = ''
    frmt   = ''
    lookup = ''
    wild   = '*'
    notations = ''
    services = ''
    instances = []
    with open(filename, 'r') as apimap:
        for metric in yaml.load_all(apimap.read()):
            if  metric.has_key('system'):
                system = metric['system']
            if  metric.has_key('services'):
                services = metric['services']
            if  metric.has_key('instances'):
                instances = metric['instances']
            if  metric.has_key('url'):
                url = metric['url']
            if  metric.has_key('wild_card'):
                wild   = metric['wild_card']
            if  metric.has_key('format'):
                frmt = metric['format']
            if  metric.has_key('lookup'):
                lookup = metric['lookup']
            if  field == 'uri' and metric.has_key('urn'):
                params = metric['params']
                urn    = metric['urn']
                expire = metric.get('expire', 600) # default 10 minutes
                record = dict(url=url, system=system, expire=expire,
                                urn=urn, params=params,
                                format=frmt, wild_card=wild, lookup=lookup,
                                services=services,
                                ts=tstamp)
                if  instances:
                    record.update({'instances':instances})
                if  metric.has_key('das_map'):
                    record['das_map'] = metric['das_map']
                else:
                    msg = "map doesn't provide das_map"
                    print metric
                    raise Exception(msg)
                if  validator(record):
                    yield record
            if  field == 'notations' and metric.has_key('notations'):
                notations = metric['notations']
                record = dict(notations=notations,
                                system=system, ts=tstamp)
                if  validator(record):
                    yield record
            if  field == 'presentation' and metric.has_key('presentation'):
                record = dict(presentation=metric['presentation'],
                                ts=tstamp)
                if  validator(record):
                    yield record
        if  field == 'notations' and not notations and system: # no notations
            record = dict(notations=[], system=system, ts=tstamp)
            if  validator(record):
                yield record

def validator(record):
    """
    DAS map validator
    """
    if  record.has_key('notations'):
        must_have_keys = ['system', 'notations', 'ts']
    elif record.has_key('presentation'):
        must_have_keys = ['presentation', 'ts']
    else:
        must_have_keys = ['system', 'format', 'urn', 'url', 'expire',
                            'params', 'das_map', 'ts']
    if  set(record.keys()) & set(must_have_keys) != set(must_have_keys):
        return False
    return True
