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
from DAS.utils.das_option import DASOption

DAS_OPTIONS = [
DASOption('mongodb', 'dburi', 'list', ['mongodb://localhost:27017']),
DASOption('mongodb', 'dbname', 'string', 'das'),
DASOption('mongodb', 'bulkupdate_size', 'int', 5000),

DASOption('mappingdb', 'dbname', 'string', 'mapping'),
DASOption('mappingdb', 'collname', 'string', 'db'),

DASOption('keylearningdb', 'dbname', 'string', 'keylearning'),
DASOption('keylearningdb', 'collname', 'string', 'db'),

DASOption('analyticsdb', 'dbname', 'string', 'analytics'),
DASOption('analyticsdb', 'collname', 'string', 'db'),
DASOption('analyticsdb', 'history', 'int', 60*24*60*60),
               
DASOption('parserdb', 'dbname', 'string', 'parser'),
DASOption('parserdb', 'collname', 'string', 'db'),
DASOption('parserdb', 'enable', 'bool', True),
DASOption('parserdb', 'sizecap', 'int', 1048576*5),

DASOption('loggingdb', 'dbname', 'string', 'parser'),
DASOption('loggingdb', 'collname', 'string', 'db'),
DASOption('loggingdb', 'capped_size', 'int', 100*1024*1024),

DASOption('dasdb', 'dbname', 'string', 'das'),
DASOption('dasdb', 'cachecollection', 'string', 'cache'),
DASOption('dasdb', 'mergecollection', 'string', 'merge'),
DASOption('dasdb', 'mrcollection', 'string', 'mapreduce'),

DASOption('cache_server', 'port', 'int', 8211),
DASOption('cache_server', 'host', 'string', '0.0.0.0'),
DASOption('cache_server', 'thread_pool', 'int', 30),
DASOption('cache_server', 'log_screen', 'bool', True),
DASOption('cache_server', 'queue_limit', 'int', 100),
DASOption('cache_server', 'socket_queue_size', 'int', 100),
DASOption('cache_server', 'n_worker_threads', 'int', 4),
DASOption('cache_server', 'logfile', 'string', '/tmp/das_cache.log'),
DASOption('cache_server', 'loglevel', 'int', 0),
DASOption('cache_server', 'clean_interval', 'int', 60),

DASOption('web_server', 'host', 'string', '0.0.0.0'),
DASOption('web_server', 'port', 'int', 8212),
DASOption('web_server', 'thread_pool', 'int', 30),
DASOption('web_server', 'log_screen', 'bool', True),
DASOption('web_server', 'socket_queue_size', 'int', 100),
DASOption('web_server', 'url_base', 'string', '/das'),
DASOption('web_server', 'cache_server_url', 'string', 'http://localhost:8211'),
DASOption('web_server', 'logfile', 'string', '/tmp/das_web.log'),
DASOption('web_server', 'loglevel', 'int', 0),
DASOption('web_server', 'status_update', 'int', 5000),

DASOption('test_server', 'host', 'string', '0.0.0.0'),
DASOption('test_server', 'port', 'int', 8214),
DASOption('test_server', 'thread_pool', 'int', 30),
DASOption('test_server', 'log_screen', 'bool', True),
DASOption('test_server', 'socket_queue_size', 'int', 100),
DASOption('test_server', 'logfile', 'string', '/tmp/das_test.log'),
DASOption('test_server', 'loglevel', 'int', 0),

DASOption('das', 'parserdir', 'string', '/tmp'),
DASOption('das', 'verbose', 'int', 0, destination='verbose'),
DASOption('das', 'logfile', 'string', '/tmp/das.log', destination='logfile'),
DASOption('das', 'gevent', 'bool', False),
DASOption('das', 'error_expire', 'int', 300),
DASOption('das', 'logformat', 'string', 
                '%(levelname)s %(message)s', destination='logformat'),
DASOption('das', 'services', 'list', 
                ['google_maps', 'ip', 'postalcode'], destination='services'),
]

def read_configparser(dasconfig):
    """Read DAS configuration"""
    config = ConfigParser.ConfigParser()
    config.read(dasconfig)
    
    configdict = {}
    
    for option in DAS_OPTIONS:
        value = option.get_from_configparser(config)
        if option.destination:
            configdict[option.destination] = value
        else:
            if option.section in configdict:
                configdict[option.section][option.name] = value
            else:
                configdict[option.section] = {}
                configdict[option.section][option.name] = value    
            
    return configdict

def write_configparser(dasconfig, use_default):
    """Write DAS configuration file""" 
    config = ConfigParser.ConfigParser()
    
    for option in DAS_OPTIONS:
        option.write_to_configparser(config, use_default)
        
    config.write(open(dasconfig, 'wb'))

def das_configfile():
    """
    Return DAS configuration file name $DAS_ROOT/etc/das.cfg
    """
    if  os.environ.has_key('DAS_CONFIG'):
        dasconfig = os.environ['DAS_CONFIG']
        if  not os.path.isfile(dasconfig):
            raise EnvironmentError('No DAS config file %s found' % dasconfig)
        return dasconfig
    else:
        raise EnvironmentError('DAS_CONFIG environment is not set up')

def read_wmcore(filename):
    """
    Read DAS python configuration file and store DAS parameters into
    returning dictionary.
    """
    from WMCore.Configuration import loadConfigurationFile
    configdict = {} # output dictionary
    config = loadConfigurationFile(filename)
    for option in DAS_OPTIONS:
        value = option.get_from_wmcore(config)
        if option.destination:
            configdict[option.destination] = value
        else:
            if option.section in configdict:
                configdict[option.section][option.name] = value
            else:
                configdict[option.section] = {}
                configdict[option.section][option.name] = value
    return configdict

def das_readconfig_helper():
    """
    Read DAS configuration file and store DAS parameters into returning
    dictionary.
    """
    configdict = {}
    dasconfig  = das_configfile()
    print "Reading DAS configuration from %s" % dasconfig
    # read first CMS python configuration file
    # if not fall back to standard python cfg file
    try:
        configdict = read_wmcore(dasconfig)
    except Exception, exp:
        print 'Unable to read DAS CMS configuration,', str(exp)
        try:
            configdict = read_configparser(dasconfig)
        except Exception, exp:
            print 'Unable to read DAS cfg configuration,', str(exp)
    if  not configdict:
        msg = 'Unable to read DAS configuration'
        raise Exception(msg)
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

