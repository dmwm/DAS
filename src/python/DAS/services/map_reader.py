#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Service map reader. Service maps represented in YAML format.
"""
__revision__ = "$Id: map_reader.py,v 1.1 2010/01/07 16:57:58 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import os
import yaml

def path(system=''):
    """
    Return path with map location for provided system.
    """
    if  os.environ.has_key('DAS_ROOT'):
        return os.path.join(os.environ['DAS_ROOT'], 
                'src/python/DAS/services/%s' % system)
    else:
        raise EnvironmentError('DAS_ROOT environment is not set up')

def readmap(system, mapfile):
    """
    Read system/api map file and construct DAS record for MappingDB.
    """
    record = {}
    with open(mapfile, 'r') as apimap:
        for metric in yaml.load_all(apimap.read()):
            params = metric['params']
            api    = metric['api']
            record = dict(system=system, api=dict(name=api, params=params))
            record.update(metric['record'])
            yield record

def read_api_map(system):
    """
    Read api map from provided system.
    """
    for record in readmap(system, os.path.join(path(system), 'apis.yml')):
        yield record

def read_notation_map(system):
    """
    Read notation map for provided service.
    """
    record = {}
    with open(os.path.join(path(system), 'notations.yml'), 'r') as notations:
        record = yaml.load(notations.read())
        record.update(dict(system=system))
    return record

def read_presentation_map():
    """
    Read UI presentation map.
    """
    record = {}
    with open(os.path.join(path(), 'presentation.yml'), 'r') as pmap:
        record = yaml.load(pmap.read())
    return record

if __name__ == '__main__':
    services = ['dbs', 'phedex', 'sitedb', 'dq', 'runsum', 'dashboard', 'lumidb', 'monitor']
    for srv in services:
        print "\n#### %s ####" % srv
        for rec in read_api_map(srv):
            print rec
        print read_notation_map(srv)
    print read_presentation_map()

