#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Config utilities
"""

__revision__ = "$Id: das_config.py,v 1.28 2009/10/14 15:19:05 valya Exp $"
__version__ = "$Revision: 1.28 $"
__author__ = "Valentin Kuznetsov"

import os
import ConfigParser

ACTIVE_SYSTEMS='dbs,sitedb,phedex,monitor,lumidb,runsum,dq,dashboard'

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

    configdict['cache_servers'] = config.get('cache', 'servers', '')
    configdict['cache_lifetime'] = config.getint('cache', 'lifetime')
    configdict['cache_chunk_size'] = config.getint('cache', 'chunk_size')

#    configdict['couch_servers'] = config.get('couch', 'servers', '')
#    configdict['couch_lifetime'] = config.getint('couch', 'lifetime')
#    configdict['couch_cleantime'] = config.getint('couch', 'cleantime')

    configdict['mongocache_dbhost'] = config.get('mongocache', 'dbhost', 'localhost')
    configdict['mongocache_dbport'] = int(config.get('mongocache', 'dbport', '27017'))
    configdict['mongocache_dbname'] = config.get('mongocache', 'dbname', 'das')
    configdict['mongocache_lifetime'] = config.getint('mongocache', 'lifetime')

    configdict['filecache_dir'] = config.get('filecache', 'dir', '')
    configdict['filecache_lifetime'] = config.getint('filecache', 'lifetime')
    configdict['filecache_base_dir'] = \
                config.get('filecache', 'base_dir', '00')
    configdict['filecache_files_dir'] = \
                int(config.get('filecache', 'files_dir', 100))
    configdict['filecache_db_engine'] = \
                config.get('filecache', 'db_engine', None)

#    configdict['views_engine'] = config.get('views', 'db_engine', None)
#    configdict['views_dir'] = config.get('views', 'dir', '')

    configdict['mapping_dbhost'] = config.get('mapping_db', 'dbhost', 'localhost')
    configdict['mapping_dbport'] = int(config.get('mapping_db', 'dbport', '27017'))
    configdict['mapping_dbname'] = config.get('mapping_db', 'dbname', 'mapping')

    configdict['analytics_dbhost'] = config.get('analytics_db', 'dbhost', 'localhost')
    configdict['analytics_dbport'] = int(config.get('analytics_db', 'dbport', '27017'))
    configdict['analytics_dbname'] = config.get('analytics_db', 'dbname', 'analytics')

    configdict['rawcache'] = config.get('das', 'rawcache', None)
    try:
        configdict['hotcache'] = config.get('das', 'hotcache', None)
    except:
        configdict['hotcache'] = None
    configdict['logdir'] = config.get('das', 'logdir', '/tmp')

    systems = config.get('das', 'systems', ACTIVE_SYSTEMS).split(',') 
    verbose = config.getint('das', 'verbose')
    configdict['systems'] = systems
    configdict['verbose'] = verbose
    for system in systems:
        verbose = config.getint(system, 'verbose')
        expire  = config.getint(system, 'expire')
        url     = config.get(system, 'url', '')
        configdict[system] = {'verbose': verbose, 'expire': expire,
                              'url'  : url}
#    sum_views = {}
#    for item in config.options('summary views'):
#        sum_views[item] = config.get('summary views', item)
#    configdict['sum_views'] = sum_views
    return configdict

def das_writeconfig():
    """
    Write DAS configuration file
    """

    config = ConfigParser.ConfigParser()

    systems = ACTIVE_SYSTEMS
    config.add_section('das')
    config.set('das', 'systems', '%s' % systems)
    config.set('das', 'verbose', 0)
    config.set('das', 'rawcache', 'DASMongocache')
#    config.set('das', 'hotcache', 'DASMemcache')
#    config.set('das', 'hotcache', 'DASFilecache')
    config.set('das', 'logdir', '/tmp')

    config.add_section('cache')
    config.set('cache', 'servers', '127.0.0.1:11211' )
    config.set('cache', 'lifetime', 5*60) # 5 minutes, in seconds
    config.set('cache', 'chunk_size', 100) # no more then 100 docs/commit

#    config.add_section('couch')
#    config.set('couch', 'servers', 'http://localhost:5984' )
#    config.set('couch', 'lifetime', 1*24*60*60) # in seconds
#    config.set('couch', 'cleantime', 2*60*60) # in seconds

    config.add_section('mongocache')
    config.set('mongocache', 'lifetime', 1*24*60*60) # in seconds
    config.set('mongocache', 'dbhost', 'localhost')
    config.set('mongocache', 'dbport', '27017')
    config.set('mongocache', 'dbname', 'das')

    config.add_section('filecache')
    dbdir  = os.path.join(os.environ['DAS_ROOT'], 'cache')
    dbfile = os.path.join(dbdir, 'das_filecache.db')
    config.set('filecache', 'dir', dbdir)
    config.set('filecache', 'lifetime', 1*24*60*60) # in seconds
    config.set('filecache', 'base_dir', '00')
    config.set('filecache', 'files_dir', 100)
    config.set('filecache', 'db_engine', 'sqlite:///%s' % dbfile)
#    config.set('filecache', 'db_engine', \
#                'mysql://%s:%s@localhost/DAS' % (login, pw))

#    config.add_section('views')
#    dbdir  = os.path.join(os.environ['DAS_ROOT'], 'db')
#    dbfile = os.path.join(dbdir, 'das_views.db')
#    config.set('views', 'dir', dbdir)
#    config.set('views', 'db_engine', 'sqlite:///%s' % dbfile)

    config.add_section('mapping_db')
    config.set('mapping_db', 'dbhost', 'localhost')
    config.set('mapping_db', 'dbport', '27017')
    config.set('mapping_db', 'dbname', 'mapping')

    config.add_section('analytics_db')
    config.set('analytics_db', 'dbhost', 'localhost')
    config.set('analytics_db', 'dbport', '27017')
    config.set('analytics_db', 'dbname', 'analytics')

#    config.add_section('summary views')
#    query  = 'find dataset, dataset.createdate, dataset.createby, '
#    query += 'sum(block.size), sum(file.numevents), count(file)'
#    config.set('summary views', 'dataset' , query)
#    query  = 'find block.name, block.size, block.numfiles, '
#    query += 'block.numevents, block.status, block.createby, '
#    query += 'block.createdate, block.modby, block.moddate'
#    config.set('summary views', 'block' , query)
#    query  = 'find site, sum(block.numevents), '
#    query += 'sum(block.numfiles), sum(block.size)'
#    config.set('summary views', 'site' , query)
#    query  = 'find datatype, dataset, run.number, run.numevents, '
#    query += 'run.numlss, run.totlumi, run.store, run.starttime, '
#    query += 'run.endtime, run.createby, run.createdate, run.modby, '
#    query += 'run.moddate, count(file), sum(file.size), '
#    query += 'sum(file.numevents)'
#    config.set('summary views', 'run' , query)

    config.add_section('dbs')
    config.set('dbs', 'expire', 1*60*60) # 1 hour, in seconds
    config.set('dbs', 'verbose', 0)
    config.set('dbs', 'url', 
    'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')

    config.add_section('sitedb')
    config.set('sitedb', 'expire', 12*60*60) # 12 hours
    config.set('sitedb', 'verbose', 0)
    config.set('sitedb', 'url', 
    'https://cmsweb.cern.ch/sitedb/json/index')

    config.add_section('phedex')
    config.set('phedex', 'expire', 30*60) # 30 minutes
    config.set('phedex', 'verbose', 0)
    config.set('phedex', 'url', 
    'https://cmsweb.cern.ch/phedex/datasvc/json/prod')

    config.add_section('monitor')
    config.set('monitor', 'expire', 1*60*60) # 1 hour
    config.set('monitor', 'verbose', 0)
    config.set('monitor', 'url', 
    'https://cmsweb.cern.ch/overview/')

    config.add_section('lumidb')
    config.set('lumidb', 'expire', 1*60*60) # 1 hour
    config.set('lumidb', 'verbose', 0)
    config.set('lumidb', 'url', 
    'http://cmslumi.cern.ch/lumi/servlet/LumiServlet')

    config.add_section('runsum')
    config.set('runsum', 'expire', 1*60*60) # 1 hour
    config.set('runsum', 'verbose', 0)
    config.set('runsum', 'url', 
    'https://cmswbm.web.cern.ch/cmswbm/cmsdb/servlet/RunSummary')

    config.add_section('dq')
    config.set('dq', 'expire', 1*60*60) # 1 hour
    config.set('dq', 'verbose', 0)
    config.set('dq', 'url', 
    'http://cmssrv49.fnal.gov:8989/DQSrvcTEST/DQSrvcServlet')

    config.add_section('dashboard')
    config.set('dashboard', 'expire', 1*60*60) # 1 hour
    config.set('dashboard', 'verbose', 0)
    config.set('dashboard', 'url', 
    'http://dashb-cms-job.cern.ch/dashboard/request.py')

    dasconfig = das_configfile()
    config.write(open(dasconfig, 'wb'))
    return dasconfig
