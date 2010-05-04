#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Data-provider map reader. Service maps are represented in YAML format.
"""
__revision__ = "$Id: map_reader.py,v 1.7 2010/02/17 16:57:24 valya Exp $"
__version__ = "$Revision: 1.7 $"
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
    format = ''
    notations = ''
    wild   = '*'
    with open(filename, 'r') as apimap:
        for metric in yaml.load_all(apimap.read()):
            if  metric.has_key('system'):
                system = metric['system']
            if  metric.has_key('url'):
                url = metric['url']
            if  metric.has_key('wild_card'):
                wild   = metric['wild_card']
            if  metric.has_key('format'):
                format = metric['format']
            if  field == 'uri' and metric.has_key('urn'):
                params = metric['params']
                urn    = metric['urn']
                expire = metric.get('expire', 600) # default 10 minutes
                apitag = metric.get('apitag', None)
                record = dict(url=url, system=system, expire=expire,
                                urn=urn, params=params, apitag=apitag,
                                format=format, wild_card=wild,
                                created=time.time())
                if  metric.has_key('api2das'):
                    record['api2das'] = metric['api2das']
                else:
                    record['api2das'] = []
                if  metric.has_key('daskeys'):
                    record['daskeys'] = metric['daskeys']
                else:
                    msg = "map doesn't provide daskeys"
                    print metric
                    raise Exception(msg)
                yield record
            if  field == 'notations' and metric.has_key('notations'):
                notations = metric['notations']
                record = dict(notations=notations,
                                system=system, created=time.time())
                yield record
            if  field == 'presentation' and metric.has_key('presentation'):
                record = dict(presentation=metric['presentation'],
                                created=time.time())
                yield record
        if  field == 'notations' and not notations and system: # no notations
            record = dict(notations=[], system=system, created=time.time())
            yield record
