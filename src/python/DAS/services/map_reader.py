#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0912
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
    - input_values (describe how to fetch list of possible input values,
        for inputs of low arity, e.g. in CMS that's release, primary_dataset)

"""
__author__ = "Valentin Kuznetsov"

import yaml
import time

# DAS modules
from DAS.utils.utils import add_hash

def read_service_map(filename, field="uri"):
    """
    Read service map file and construct DAS record for MappingDB.
    """
    # tstamp must be integer in order for json encoder/decoder to
    # work properly, see utils/jsonwrapper/__init__.py
    tstamp = round(time.time())
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
            if  'system' in metric:
                system = metric['system']
            if  'services' in metric:
                services = metric['services']
            if  'instances' in metric:
                instances = metric['instances']
            if  'url' in metric:
                url = metric['url']
            if  'wild_card' in metric:
                wild   = metric['wild_card']
            if  'format' in metric:
                frmt = metric['format']
            if  'lookup' in metric:
                lookup = metric['lookup']
            if  field == 'uri' and 'urn' in metric:
                params = metric['params']
                urn    = metric['urn']
                expire = metric.get('expire', 600) # default 10 minutes
                record = dict(url=url, system=system, expire=expire, \
                                urn=urn, params=params, \
                                format=frmt, wild_card=wild, lookup=lookup, \
                                services=services, ts=tstamp)
                if  instances:
                    record.update({'instances':instances})
                if  'das_map' in metric:
                    record['das_map'] = metric['das_map']
                else:
                    msg = "map doesn't provide das_map"
                    print metric
                    raise Exception(msg)
                if  validator(record):
                    yield record
            if  field == 'notations' and 'notations' in metric:
                notations = metric['notations']
                record = dict(notations=notations, system=system, ts=tstamp)
                if  validator(record):
                    yield record
            if  field == 'presentation' and 'presentation' in metric:
                record = dict(presentation=metric['presentation'], ts=tstamp)
                if  validator(record):
                    yield record
            if field == 'input_values' and 'input_values' in metric:
                record = dict(input_values=metric['input_values'], system=system)
                if validator(record):
                    yield record
        if  field == 'notations' and not notations and system: # no notations
            record = dict(notations=[], system=system, ts=tstamp)
            if  validator(record):
                yield record

def add_record_type(record):
    """Add record type to DAS record"""
    if  'presentation' in record:
        record.update({'type':'presentation'})
    elif 'notations' in record:
        record.update({'type':'notation'})
    elif 'input_values' in record:
        record.update({'type':'input_values'})
    else:
        record.update({'type':'service'})

def validator(record):
    """
    DAS map validator
    """
    add_record_type(record)
    add_hash(record)
    must_have_keys = ['type']
    if  'notations' in record:
        must_have_keys += ['system', 'notations', 'ts', 'hash']
    elif 'presentation' in record:
        must_have_keys += ['presentation', 'ts', 'hash']
    elif 'input_values' in record:
        must_have_keys += ['input_values']
    else:
        must_have_keys += ['system', 'format', 'urn', 'url', 'expire', \
                            'params', 'das_map', 'ts', 'hash']
    if  set(record.keys()) & set(must_have_keys) != set(must_have_keys):
        return False
    return True
