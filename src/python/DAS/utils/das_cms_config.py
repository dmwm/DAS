#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS CMS configuration utilities
"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"
__author__ = "Valentin Kuznetsov"

import os.path
from WMCore.Configuration import loadConfigurationFile

def das_read_cms_config(filename):
    """
    Read DAS python configuration file and store DAS parameters into
    returning dictionary.
    """
#    configdir = os.path.normcase(os.path.abspath(__file__)).rsplit('/', 1)[0]

    configdict = {} # output dictionary
    config = loadConfigurationFile(filename)
    configdict['mongodb'] = config.section_('mongodb').dictionary_()
    configdict['cache_server'] = config.section_('cache_server').dictionary_()
    configdict['web_server'] = config.section_('web_server').dictionary_()
    configdict['analyticsdb'] = config.section_('analyticsdb').dictionary_()
    configdict['mappingdb'] = config.section_('mappingdb').dictionary_()
    configdict['das'] = config.section_('das').dictionary_()

    configdict['rawcache'] = configdict['das'].get('rawcache', None)
    parserdir = configdict['das'].get('parserdir', '/tmp')
    verbose   = configdict['das'].get('verbose', 0)
    logfile   = configdict['das'].get('logfile', None)
    logformat = configdict['das'].get('logformat', '%(levelname)s %(message)s')
    services  = configdict['das'].get('services', [])
    configdict['services']  = services
    configdict['logformat'] = logformat
    if  logfile:
        configdict['logfile'] = logfile
    configdict['verbose'] = verbose
    configdict['das'] = dict(verbose=verbose, parserdir=parserdir,
                logfile=logfile, logformat=logformat, services=services)
    return configdict

if  __name__ == '__main__':
    config = das_read_cms_config('config/das_cms.py')
    print config
