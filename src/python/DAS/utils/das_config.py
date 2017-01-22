#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Config utilities
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

import os
try:
    import ConfigParser
except ImportError: # python3
    import configparser as ConfigParser
from DAS.utils.das_option import DASOption

DAS_OPTIONS = [
#
# pycurl options
#
DASOption('pycurl', 'FOLLOWLOCATION', 'int', 1),
DASOption('pycurl', 'MAXREDIRS', 'int', 5),
DASOption('pycurl', 'CONNECTTIMEOUT', 'int', 270),
DASOption('pycurl', 'TIMEOUT', 'int', 270),
DASOption('pycurl', 'NOSIGNAL', 'int', 1),
DASOption('pycurl', 'SSL_VERIFYPEER', 'bool', False),
DASOption('pycurl', 'VERBOSE', 'int', 0),
#
# MongoDB options
#
# URI, can be multiple
DASOption('mongodb', 'dburi', 'list', ['mongodb://localhost:8230']),
# default DB name
DASOption('mongodb', 'dbname', 'string', 'das'),
# default DB collection names
DASOption('mongodb', 'collections', 'list', ['cache', 'merge']),
# default bulk size to be used by DAS during records insertion into MongoDB
DASOption('mongodb', 'bulkupdate_size', 'int', 5000),
# default lifetime of requests in MongoDB, default is 10min
DASOption('mongodb', 'lifetime', 'int', 600),

#
# Mapping DB options
#
# default DB name
DASOption('mappingdb', 'dbname', 'string', 'mapping'),
# default collection name
DASOption('mappingdb', 'collname', 'string', 'db'),
# default reload_time for MappingDB maps monitor daemon, default is 1h
DASOption('mappingdb', 'reload_time', 'int', 60*60),
# default reload_time when mappings are in inconsistent state
DASOption('mappingdb', 'reload_time_bad_maps', 'int', 2*60),

#
# Keylearning DB options
#
# default DB name
DASOption('keylearningdb', 'dbname', 'string', 'keylearning'),
# default collection name
DASOption('keylearningdb', 'collname', 'string', 'db'),

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
# enable logging of DAS db operation, put insert/delete into logging db
DASOption('dasdb', 'logging', 'bool', False),
# DAS cache record TTL (time-to-live) parameter, i.e. how long to keep
# records in DAS cache, default is 1 day.
DASOption('dasdb', 'record_ttl', 'int', 24*60*60),
# clean-up worker
DASOption('dasdb', 'cleanup_worker', 'bool', True),
# Inerval for clean-up worker
DASOption('dasdb', 'cleanup_interval', 'int', 600),
# delta TTL controls a buffer above expiration timestamp to ensure that
# DAS record will not disappear upon request, i.e. DAS server can be
# busy processing other requests and if upon request record was in cache
# and during processing chain was clean-up will make request disappear
DASOption('dasdb', 'delta_ttl', 'int', 60),
# cleanup delta TTL controls an offset for clean-up work daemon
# this daemon responsible to clean-up DAS cache periodically and it's
# better to have an offset to ensure that only old records will be wiped out
DASOption('dasdb', 'cleanup_delta_ttl', 'int', 3600),

#
# DAS web server options
#
# default host name, 0.0.0.0 to serve all incoming IPV4 requests
DASOption('web_server', 'host', 'string', '0.0.0.0'),
# default port number
DASOption('web_server', 'port', 'int', 8212),
# KWS port number
DASOption('web_server', 'kws_port', 'int', 8214),
# number of threads in cherrypy server
DASOption('web_server', 'thread_pool', 'int', 30),
# cherrypy log_screen option
DASOption('web_server', 'log_screen', 'bool', True),
# cherrypy server parameter, controls number of pending request in socket queue
DASOption('web_server', 'socket_queue_size', 'int', 100),
# default URL base mount point
DASOption('web_server', 'url_base', 'string', '/das'),
# status_update controls how often AJAX calls should be invoked (in milsec)
DASOption('web_server', 'status_update', 'int', 3000),
# number of web workers to serve user queries in DAS server
DASOption('web_server', 'web_workers', 'int', 50),
# limit number of peding jobs in DAS server queue
DASOption('web_server', 'queue_limit', 'int', 1000),
# type of Queue used by DAS server
DASOption('web_server', 'qtype', 'string', 'Queue'),
# frequency denominator for PriorityQueue, see code
DASOption('web_server', 'qfreq', 'int', 5),
# The adjust_input function can be implemented for concrete use case
# In CMS we can adjust input values to regex certain things, like dataset/run.
DASOption('web_server', 'adjust_input', 'bool', False),
# DBS dataset daemon fetch list of known DBS datasets and keep them
# in separate collection, to be used by DAS web server for autocompletion
DASOption('web_server', 'dbs_daemon', 'bool', False),
# Threshold for DAS normal clients
DASOption('web_server', 'hot_threshold', 'int', 100),
# DBS dataset daemon update interval in seconds
DASOption('web_server', 'dbs_daemon_interval', 'int', 3600),
# DBS dataset daemon expire timestamp (in seconds)
DASOption('web_server', 'dbs_daemon_expire', 'int', 3600),
# if true, will keep existing Datasets on server restart
# (i.e. it will not delete dbs collection; Record expiration still applies)
DASOption('web_server', 'preserve_on_restart', 'bool', False),
# local DAS services
DASOption('web_server', 'services', 'list', []),
# option to check DAS clients
DASOption('web_server', 'check_clients', 'bool', False),

# --- DAS web server plugins ---
# The hints plugin warns user about particular cases of queries that may
# be tricky: case-insensitive matches & datasets in other dbs instances
DASOption('web_plugins', 'show_hints', 'bool', False),
# TODO: dbs deamon to this section?
# TODO: adjust input to here?


# cacherequests section of DAS config
DASOption('cacherequests', 'Admin', 'int', 5000),
DASOption('cacherequests', 'ProductionAccess', 'int', 3000),
DASOption('cacherequests', 'Unlimited', 'int', 10000),

# DBS section of DAS config
# extended expire timestamp in seconds, it can be used to extend
# lifetime of DBS record in DAS cache based on their modification time stamp
DASOption('dbs', 'extended_expire', 'int', 0),
# extended threshold in seconds, it can be used to trigger
# extended timestamp usage, default is zero
DASOption('dbs', 'extended_threshold', 'int', 0),
# DBS url
DASOption('dbs', 'url', 'string', 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'),
# DBS url
DASOption('dbs', 'global', 'string', 'prod/global'),
# DBS instances
DASOption('dbs', 'instances', 'list', ['prod/global', 'prod/phys01', 'prod/phys02', 'prod/phys03', 'prod/caf']),

#
# DAS test server options
#
# default host address, 0.0.0.0 to support all incoming IPV4
DASOption('test_server', 'host', 'string', '0.0.0.0'),
# default port number
DASOption('test_server', 'port', 'int', 8214),
# cherrypy thread pool parameter
DASOption('test_server', 'thread_pool', 'int', 30),
# cherrypy socket queue size
DASOption('test_server', 'socket_queue_size', 'int', 100),

#
# DAS core options
#
# verbosity level
DASOption('das', 'verbose', 'int', 0, destination='verbose'),
# flag to turn on/off the multitasking (thread based) support in DAS
DASOption('das', 'multitask', 'bool', True),
# error_expire controls how long to keep DAS record for misbehaving data-srv
DASOption('das', 'error_expire', 'int', 300),
# emptyset_expire controls how long to keep DAS record for empty result set
DASOption('das', 'emptyset_expire', 'int', 5),
# list of data services participated in DAS
DASOption('das', 'services', 'list',
                ['dbs3', 'phedex'], destination='services'),
# Choice of main DBS
DASOption('das', 'main_dbs', 'string', 'dbs3'),
# Choice of DBS instances, use empty list to allow read them from DAS maps
DASOption('das', 'dbs_instances', 'list', []),
# number of DASCore workers
# defines how many data-service calls will run at onces
DASOption('das', 'core_workers', 'int', 50),
# number of API workers
# defines how many data-service API calls will run at onces
DASOption('das', 'api_workers', 'int', 2),
# weights for API worker threads (some srvs need more threads then others)
# list contains service_name:weight pairs as a single strings
# we use this format due to option parser constains
DASOption('das', 'thread_weights', 'list', []),

#
# Keyword Search options
#
DASOption('keyword_search', 'kws_on', 'bool', False),
DASOption('keyword_search', 'kws_service_on', 'bool', False),
# max time for exhaustive search ranker, default 5 seconds
DASOption('keyword_search', 'timeout', 'int', 5),
DASOption('keyword_search', 'show_scores', 'bool', False),
DASOption('keyword_search', 'colored_scorebar', 'bool', False),


#
# Load balancing
#
# host where keyword search is run, by default it's same as DAS
DASOption('load_balance', 'kws_host', 'string', ''),
# hosts from where keyword search (or autocompletion) could be initialized
# need to be set on KWS backend only and if no proxy is used
DASOption('load_balance', 'valid_origins', 'list', []),

# Query rewrite
DASOption('query_rewrite', 'pk_rewrite_on', 'bool', False),
DASOption('query_rewrite', 'dasclient_doc_url', 'string', ''),

# Security
DASOption('security', 'role', 'string', ''),
DASOption('security', 'group', 'string', ''),
DASOption('security', 'site', 'string', ''),
DASOption('security', 'module', 'string', ''),
DASOption('security', 'key_file', 'string', ''),
DASOption('security', 'store', 'string', '/tmp/security-store'),
DASOption('security', 'mount_point', 'string', 'auth'),
]  # end of DAS_OPTIONS list

def read_configparser(dasconfig):
    """Read DAS configuration"""
    config = ConfigParser.ConfigParser()
    if  dasconfig:
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

def das_configfile(fname=None):
    """
    Return DAS configuration file name $DAS_ROOT/etc/das.cfg
    """
    if  fname:
        if  not os.path.isfile(fname):
            raise EnvironmentError('No DAS config file %s found' % fname)
        return fname
    if  'DAS_CONFIG' in os.environ:
        dasconfig = os.environ['DAS_CONFIG']
        if  not os.path.isfile(dasconfig):
            raise EnvironmentError('No DAS config file %s found' % dasconfig)
        return dasconfig
    else:
        raise EnvironmentError('DAS_CONFIG environment is not set up')

def wmcore_config(filename):
    """Return WMCore config object for given file name"""
    from WMCore.Configuration import loadConfigurationFile
    config = loadConfigurationFile(filename)
    return config

def read_wmcore(filename):
    """
    Read DAS python configuration file and store DAS parameters into
    returning dictionary.
    """
    configdict = {} # output dictionary
    config = wmcore_config(filename)
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

def das_readconfig_helper(fname=None, debug=True):
    """
    Read DAS configuration file and store DAS parameters into returning
    dictionary.
    """
    configdict = {}
    dasconfig  = das_configfile(fname)
    # read first CMS python configuration file
    # if not fall back to standard python cfg file
    try:
        if  debug:
            print("### Reading DAS configuration from %s" % dasconfig)
        configdict = read_wmcore(dasconfig)
    except Exception as err:
        if  debug:
            print('Unable to read_wmcore DAS CMS configuration,', str(err))
        try:
            if  debug:
                print("### Reading DAS configuration from %s" % dasconfig)
            configdict = read_configparser(dasconfig)
        except Exception as exp:
            if  debug:
                print('Unable to read_configparser DAS cfg configuration,', str(exp))
            configdict = read_configparser(None)
    if  not configdict:
        msg = 'Unable to read DAS configuration'
        raise Exception(msg)
    configdict['das_config_file'] = dasconfig
    return configdict

class _DASConfigSingleton(object):
    """
    DAS configuration singleton class. It reads DAS configuration using
    das_readconfig_helper function which itself reads it from either
    CMS python configuration file or cfg.

    the configuration file is read once is lazy way, in config().
    """
    def __init__(self):
        self.das_config = None
        self.das_config_debug = None

    def config(self, fname=None, debug=False):
        """Return DAS config"""
        if  debug:
            if not self.das_config_debug:
                self.das_config_debug = das_readconfig_helper(fname, debug)
            return self.das_config_debug
        else:
            if not self.das_config:
                self.das_config = das_readconfig_helper(fname)
            return self.das_config

# ensure unique name for singleton object
DAS_CONFIG_SINGLETON = _DASConfigSingleton()

def das_readconfig(fname=None, debug=False):
    """
    Return DAS configuration
    """
    return DAS_CONFIG_SINGLETON.config(fname, debug)
