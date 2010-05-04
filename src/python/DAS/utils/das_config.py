#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Config utilities
"""

__revision__ = "$Id: das_config.py,v 1.33 2010/03/10 01:19:56 valya Exp $"
__version__ = "$Revision: 1.33 $"
__author__ = "Valentin Kuznetsov"

import os
import ConfigParser

def das_configfile():
    """
    Return DAS configuration file name $DAS_ROOT/etc/das.cfg
    """
    if  os.environ.has_key('DAS_ROOT'):
        dasconfig = os.path.join(os.environ['DAS_ROOT'], 'etc/das.cfg')
        if  not os.path.isfile(dasconfig):
            raise EnvironmentError('No DAS config file %s found' % dasconfig)
        return dasconfig
    else:
        raise EnvironmentError('DAS_ROOT environment is not set up')

def das_readconfig(dasconfig=None):
    """
    Read DAS configuration file and store DAS parameters into returning
    dictionary.
    """
    if  not dasconfig:
        dasconfig = das_configfile()
    config = ConfigParser.ConfigParser()
    config.read(dasconfig)
    configdict = {}

    configdict['mongocache_dbhost'] = config.get('mongocache', 'dbhost', 'localhost')
    configdict['mongocache_dbport'] = int(config.get('mongocache', 'dbport', '27017'))
    configdict['mongocache_dbname'] = config.get('mongocache', 'dbname', 'das')
    configdict['mongocache_bulkupdate_size'] = config.getint('mongocache', 'bulkupdate_size')
    configdict['mongocache_capped_size'] = config.getint('mongocache', 'capped_size')
    configdict['mongocache_lifetime'] = config.getint('mongocache', 'lifetime')

    configdict['mapping_dbhost'] = config.get('mapping_db', 'dbhost', 'localhost')
    configdict['mapping_dbport'] = int(config.get('mapping_db', 'dbport', '27017'))
    configdict['mapping_dbname'] = config.get('mapping_db', 'dbname', 'mapping')

    configdict['analytics_dbhost'] = config.get('analytics_db', 'dbhost', 'localhost')
    configdict['analytics_dbport'] = int(config.get('analytics_db', 'dbport', '27017'))
    configdict['analytics_dbname'] = config.get('analytics_db', 'dbname', 'analytics')

    configdict['rawcache'] = config.get('das', 'rawcache', None)
    configdict['logdir'] = config.get('das', 'logdir', '/tmp')
    configdict['web_server_port'] = config.get('das', 'web_server_port', 8212)
    configdict['cache_server_port'] = config.get('das', 'cache_server_port', 8211)

    verbose = config.getint('das', 'verbose')
    configdict['verbose'] = verbose
    return configdict

def das_writeconfig():
    """
    Write DAS configuration file
    """

    config = ConfigParser.ConfigParser()

    config.add_section('das')
    config.set('das', 'verbose', 0)
    config.set('das', 'rawcache', 'DASMongocache')
    config.set('das', 'logdir', '/tmp')
    config.set('das', 'web_server_port', 8212)
    config.set('das', 'cache_server_port', 8211)

    config.add_section('mongocache')
    config.set('mongocache', 'lifetime', 1*24*60*60) # in seconds
    config.set('mongocache', 'dbhost', 'localhost')
    config.set('mongocache', 'dbport', '27017')
    config.set('mongocache', 'dbname', 'das')
    config.set('mongocache', 'bulkupdate_size', 5000)
    config.set('mongocache', 'capped_size', 100*1024*1024) # 100MB

    config.add_section('mapping_db')
    config.set('mapping_db', 'dbhost', 'localhost')
    config.set('mapping_db', 'dbport', '27017')
    config.set('mapping_db', 'dbname', 'mapping')

    config.add_section('analytics_db')
    config.set('analytics_db', 'dbhost', 'localhost')
    config.set('analytics_db', 'dbport', '27017')
    config.set('analytics_db', 'dbname', 'analytics')

    dasconfig = das_configfile()
    config.write(open(dasconfig, 'wb'))
    return dasconfig
