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
#
# MongoDB options
#
# URI, can be multiple
DASOption('mongodb', 'dburi', 'list', ['mongodb://localhost:27017']),
# default DB name
DASOption('mongodb', 'dbname', 'string', 'das'),
# default bulk size to be used by DAS during records insertion into MongoDB
DASOption('mongodb', 'bulkupdate_size', 'int', 5000),

#
# Mapping DB options
#
# default DB name
DASOption('mappingdb', 'dbname', 'string', 'mapping'),
# default collection name
DASOption('mappingdb', 'collname', 'string', 'db'),

#
# Keyleardnign DB options
#
# default DB name
DASOption('keylearningdb', 'dbname', 'string', 'keylearning'),
# default collection name
DASOption('keylearningdb', 'collname', 'string', 'db'),

#
# Analytics DB options
#
# default db name
DASOption('analyticsdb', 'dbname', 'string', 'analytics'),
# default collection name
DASOption('analyticsdb', 'collname', 'string', 'db'),
# controls how long records in analytics live, default 2 months
DASOption('analyticsdb', 'history', 'int', 60*24*60*60),
               
#
# ParserDB optins
#
# default db name
DASOption('parserdb', 'dbname', 'string', 'parser'),
# default colleciton name
DASOption('parserdb', 'collname', 'string', 'db'),
# enable/disable writing QL into parserdb
DASOption('parserdb', 'enable', 'bool', True),
# size of parser db (MongoDB capped collection)
DASOption('parserdb', 'sizecap', 'int', 1048576*5),

#
# Loggind DB options
#
# default db name
DASOption('loggingdb', 'dbname', 'string', 'parser'),
# default collection name
DASOption('loggingdb', 'collname', 'string', 'db'),
# size of logging db (MongoDB capped collection)
DASOption('loggingdb', 'capped_size', 'int', 100*1024*1024),

#
# DAS DBs options
#
# default db name
DASOption('dasdb', 'dbname', 'string', 'das'),
# default cache collection name
DASOption('dasdb', 'cachecollection', 'string', 'cache'),
# default merge collection name
DASOption('dasdb', 'mergecollection', 'string', 'merge'),
# default map reduce collecion name
DASOption('dasdb', 'mrcollection', 'string', 'mapreduce'),

#
# DAS web server options
#
# default host name, 0.0.0.0 to serve all incoming IPV4 requests
DASOption('web_server', 'host', 'string', '0.0.0.0'),
# default port number
DASOption('web_server', 'port', 'int', 8212),
# number of threads in cherrypy server
DASOption('web_server', 'thread_pool', 'int', 30),
# cherrypy log_screen option
DASOption('web_server', 'log_screen', 'bool', True),
# cherrypy server parameter, controls number of pending request in socket queue
DASOption('web_server', 'socket_queue_size', 'int', 100),
# default URL base mount point
DASOption('web_server', 'url_base', 'string', '/das'),
# default log file name
DASOption('web_server', 'logfile', 'string', '/tmp/das_web.log'),
# default log level
DASOption('web_server', 'loglevel', 'int', 0),
# status_update controls how often AJAX calls should be invoked (in milsec)
DASOption('web_server', 'status_update', 'int', 3000),
# number of workers to serve user queries in DAS server
DASOption('web_server', 'number_of_workers', 'int', 10),
# limit number of peding jobs in DAS server queue
DASOption('web_server', 'queue_limit', 'int', 50),
# The adjust_input function can be implemented for concrete use case
# In CMS we can adjust input values to regex certain things, like dataset/run/etc.
DASOption('web_server', 'adjust_input', 'bool', False),

#
# DAS test server options
#
# default host address, 0.0.0.0 to support all incoming IPV4
DASOption('test_server', 'host', 'string', '0.0.0.0'),
# default port number
DASOption('test_server', 'port', 'int', 8214),
# cherrypy thread pool parameter
DASOption('test_server', 'thread_pool', 'int', 30),
# cherrypy log_screen parameter
DASOption('test_server', 'log_screen', 'bool', True),
# cherrypy socket queue size
DASOption('test_server', 'socket_queue_size', 'int', 100),
# default log file name
DASOption('test_server', 'logfile', 'string', '/tmp/das_test.log'),
# default log level
DASOption('test_server', 'loglevel', 'int', 0),

#
# DAS core options
#
# location of parser.out file used by PLY
DASOption('das', 'parserdir', 'string', '/tmp'),
# verbosity level
DASOption('das', 'verbose', 'int', 0, destination='verbose'),
# log file for logger
DASOption('das', 'logfile', 'string', '/tmp/das.log', destination='logfile'),
# flag to turn on/off the multitasking (thread based) support in DAS
DASOption('das', 'multitask', 'bool', True),
# error_expire controls how long to keep DAS record for misbehaving data-srv
DASOption('das', 'error_expire', 'int', 300),
# logformat for DAS logger
DASOption('das', 'logformat', 'string', 
                '%(levelname)s %(message)s', destination='logformat'),
# list of data services participated in DAS
DASOption('das', 'services', 'list', 
                ['google_maps', 'ip', 'postalcode'], destination='services'),

# DAS dbs_phedex service options
DASOption('dbs_phedex', 'urls', 'list',
        ['http://vocms09.cern.ch:8989/dbs/DBSReader/datasets',
         'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockReplicas'])
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
        print 'Unable to read DAS CMS configuration, err="%s"' % str(exp)
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

