#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Data-provider map reader. Service maps are represented in YAML format.
"""
__revision__ = "$Id: map_reader.py,v 1.2 2010/02/02 19:55:20 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import yaml
import time

def read_service_map(filename, field="api"):
    """
    Read service map file and construct DAS record for MappingDB.
    """
    record = {}
    system = ''
    url    = ''
    format = ''
    with open(filename, 'r') as apimap:
        for metric in yaml.load_all(apimap.read()):
            if  metric.has_key('system'):
                system = metric['system']
            if  metric.has_key('url'):
                url = metric['url']
            if  metric.has_key('format'):
                format = metric['format']
            if  field == 'api' and metric.has_key('api'):
                params = metric['params']
                api    = metric['api']
                expire = metric.get('expire', 600) # default 10 minutes
                record = dict(url=url, system=system, expire=expire,
                                api=dict(name=api, params=params),
                                format=format, created=time.time())
                record.update(metric['record'])
                yield record
            if  field == 'notations' and metric.has_key('notations'):
                record = dict(notations=metric['notations'],
                                url=url, system=system, created=time.time())
                yield record
            if  field == 'presentation' and metric.has_key('presentation'):
                record = dict(presentation=metric['presentation'],
                                created=time.time())
                yield record

