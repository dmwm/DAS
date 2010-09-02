#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Config utilities
"""

__revision__ = "$Id: das_config.py,v 1.39 2010/04/15 18:01:27 valya Exp $"
__version__ = "$Revision: 1.39 $"
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

def das_read_cfg(dasconfig=None):
    """
    Read DAS configuration file and store DAS parameters into returning
    dictionary.
    """
    if  not dasconfig:
        dasconfig = das_configfile()
    print "Reading DAS configuration from %s file..." % dasconfig
    config = ConfigParser.ConfigParser()
    config.read(dasconfig)
    configdict = {}

    mongodb = {}
    mongodb['dbhost'] = config.get('mongodb', 'dbhost', 'localhost')
    mongodb['dbport'] = int(config.get('mongodb', 'dbport', 27017))
    mongodb['dbname'] = config.get('mongodb', 'dbname', 'das')
    mongodb['attempt'] = config.get('mongodb', 'attempt', 3)
    mongodb['bulkupdate_size'] = config.getint('mongodb', 'bulkupdate_size')
    mongodb['capped_size'] = config.getint('mongodb', 'capped_size')
    mongodb['lifetime'] = config.getint('mongodb', 'lifetime')
    configdict['mongodb'] = mongodb

    mapping = {}
    mapping['dbhost'] = config.get('mapping_db', 'dbhost', 'localhost')
    mapping['dbport'] = int(config.get('mapping_db', 'dbport', 27017))
    mapping['dbname'] = config.get('mapping_db', 'dbname', 'mapping')
    mapping['collname'] = config.get('mapping_db', 'collname', 'db')
    mapping['attempt']  = config.get('mapping_db', 'attempt', 3)
    configdict['mappingdb'] = mapping

    analytics = {}
    analytics['dbhost'] = config.get('analytics_db', 'dbhost', 'localhost')
    analytics['dbport'] = int(config.get('analytics_db', 'dbport', 27017))
    analytics['dbname'] = config.get('analytics_db', 'dbname', 'analytics')
    analytics['collname'] = config.get('analytics_db', 'collname', 'db')
    analytics['attempt']  = config.get('analytics_db', 'attempt', 3)
    configdict['analyticsdb'] = analytics

    cache_server = {}
    cache_server['port'] = config.getint('cache_server', 'port')
    cache_server['host'] = config.get('cache_server', 'host')
    cache_server['thread_pool'] = config.get('cache_server', 'thread_pool')
    cache_server['log_screen'] = config.get('cache_server', 'log_screen')
    cache_server['queue_limit'] = \
                config.get('cache_server', 'queue_limit', 100)
    cache_server['socket_queue_size'] = \
                config.get('cache_server', 'socket_queue_size')
    cache_server['n_worker_threads'] = \
                config.getint('cache_server', 'n_worker_threads')
    cache_server['logfile'] = config.get('cache_server', 'logfile')
    cache_server['loglevel'] = config.get('cache_server', 'loglevel')
    configdict['cache_server'] = cache_server

    web_server = {}
    web_server['port'] = config.getint('web_server', 'port')
    web_server['host'] = config.get('web_server', 'host')
    web_server['thread_pool'] = config.get('web_server', 'thread_pool')
    web_server['log_screen'] = config.get('web_server', 'log_screen')
    web_server['socket_queue_size'] = \
                config.get('web_server', 'socket_queue_size')
    web_server['url_base'] = config.get('web_server', 'url_base')
    web_server['cache_server_url'] = \
                config.get('web_server', 'cache_server_url')
    web_server['logfile'] = config.get('web_server', 'logfile')
    web_server['loglevel'] = config.get('web_server', 'loglevel')
    configdict['web_server'] = web_server

#    security = {}
#    security['role'] = config.get('security', 'role')
#    security['group'] = config.get('security', 'group')
#    security['site'] = config.get('security', 'site')
#    security['mount_point'] = config.get('security', 'mount_point')
#    security['enabled'] = config.get('security', 'enabled')
#    security['oid_server'] = config.get('security', 'oid_server')
#    security['session_name'] = config.get('security', 'session_name')
#    security['store_path'] = config.get('security', 'store_path')
#    configdict['security'] = security

    verbose   = config.getint('das', 'verbose')
    logfile   = config.get('das', 'logfile', None)
    logformat = config.get('das', 'logformat', '%(levelname)s %(message)s')
    services  = config.get('das', 'services', []).split(',')
    # store services, logformat, verbose separately, since used in different
    # tools without other config parameters
    configdict['services']  = services
    configdict['logformat'] = logformat
    if  logfile:
        configdict['logfile'] = logfile
    configdict['verbose'] = verbose
    configdict['das'] = dict(verbose=verbose, logfile=logfile, logformat=logformat, services=services)
    return configdict

def das_writeconfig():
    """
    Write DAS configuration file
    """

    config = ConfigParser.ConfigParser()

    config.add_section('das')
    config.set('das', 'verbose', 0)
    config.set('das', 'logfile', '/tmp/das.log')
    config.set('das', 'logformat', 
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # list of generic services, for CMS specific ones, please use 
    # DAS CMS python configuration
    services = ['ip_service', 'google_maps', 'postalcode']
    config.set('das', 'services', services)

    config.add_section('mongodb')
    config.set('mongodb', 'lifetime', 1*24*60*60) # in seconds
    config.set('mongodb', 'dbhost', 'localhost')
    config.set('mongodb', 'dbport', 27017)
    config.set('mongodb', 'dbname', 'das')
    config.set('mongodb', 'bulkupdate_size', 5000)
    config.set('mongodb', 'attempt', 3) # # of attempts to connect to db
    config.set('mongodb', 'capped_size', 100*1024*1024) # 100MB

    config.add_section('mapping_db')
    config.set('mapping_db', 'dbhost', 'localhost')
    config.set('mapping_db', 'dbport', 27017)
    config.set('mapping_db', 'dbname', 'mapping')
    config.set('mapping_db', 'collname', 'db')
    config.set('mapping_db', 'attempt', 3) # of attempts to connect to db

    config.add_section('analytics_db')
    config.set('analytics_db', 'dbhost', 'localhost')
    config.set('analytics_db', 'dbport', 27017)
    config.set('analytics_db', 'dbname', 'analytics')
    config.set('analytics_db', 'collname', 'db')
    config.set('analytics_db', 'attempt', 3) # of attempts to connect to db

    config.add_section('cache_server')
    config.set('cache_server', 'port', 8211)
    config.set('cache_server', 'host', '0.0.0.0')
    config.set('cache_server', 'thread_pool', 30)
    config.set('cache_server', 'log_screen', True)
    config.set('cache_server', 'socket_queue_size', 15)
    config.set('cache_server', 'n_worker_threads', 4)
    config.set('cache_server', 'queue_limit', 100)
    config.set('cache_server', 'logfile', '/tmp/das_cache.log')
    config.set('cache_server', 'loglevel', 0)

    config.add_section('web_server')
    config.set('web_server', 'port', 8212)
    config.set('web_server', 'host', '0.0.0.0')
    config.set('web_server', 'thread_pool', 30)
    config.set('web_server', 'log_screen', True)
    config.set('web_server', 'socket_queue_size', 15)
    config.set('web_server', 'cache_server_url', 'http://localhost:8211')
    config.set('web_server', 'url_base', '/das')
    config.set('web_server', 'logfile', '/tmp/das_web.log')
    config.set('web_server', 'loglevel', 0)

#    config.add_section('security')
#    config.set('security', 'role', '')
#    config.set('security', 'group', 'das')
#    config.set('security', 'site', '')
#    config.set('security', 'mount_point', '/das/auth')
#    config.set('security', 'enabled', True)
#    config.set('security', 'oid_server', 'http://localhost:8400')
#    config.set('security', 'session_name', 'DAS Security Module')
#    config.set('security', 'store_path', '/tmp')

    dasconfig = das_configfile()
    config.write(open(dasconfig, 'wb'))
    return dasconfig

def das_readconfig_helper(dasconfig=None):
    """
    Read DAS configuration file and store DAS parameters into returning
    dictionary.
    """
    configdict = {}
    # read first CMS python configuration file
    # if not fall back to standard python cfg file
    try:
        from DAS.utils.das_cms_config import das_read_cms_config
        print "Reading DAS CMS configuration..."
        cmsconfig = dasconfig
        if  not cmsconfig:
            cmsconfig = 'config/das_cms.py'
        configdict = das_read_cms_config(cmsconfig)
    except Exception, ex:
        print 'Unable to locate DAS CMS configuration,', str(ex)
        configdict = das_read_cfg(dasconfig)
    return configdict

class _DASConfigSingleton(object):
    """
    DAS configuration singleton class. It reads DAS configuration using
    das_readconfig_helper function which itself reads it from either
    CMS python configuration file or cfg.
    Code based on suggestion found here:
    http://code.activestate.com/recipes/52558-the-singleton-pattern-implemented-with-python/
    """
    def __init__(self):
        self.das_config  = das_readconfig_helper()
    def config(self):
        """Return DAS config"""
        return self.das_config

# ensure unique name for singleton object
DAS_CONFIG_SINGLETON = _DASConfigSingleton()

def das_readconfig():
    """
    Return DAS configuration
    """
    return DAS_CONFIG_SINGLETON.config()

