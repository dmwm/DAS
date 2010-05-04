#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,C0301

"""
DAS cache server web configuration file
"""

__revision__ = "$Id: das_cacheconfig.py,v 1.5 2009/06/08 19:15:48 valya Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "Valentin Kuznetsov"

#
# This is an example configuration which loads the documentation classes for
# the webtools package. In general your application should have it's own 
# configuration and not use this, other than as a guideline.
#
from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')

# This is the application
config.Webtools.port = 8211
config.Webtools.application = 'DASCacheServer'
# This is the config for the application
config.component_('DASCacheServer')
# Define the default location for templates for the app
config.DASCacheServer.templates = environ['DAS_ROOT'] + '/src/templates'
config.DASCacheServer.title = 'DAS cache server'
config.DASCacheServer.description = 'CMS data-services cache'
config.DASCacheServer.admin = 'vkuznet@gmail.com'

# Views are all pages 
config.DASCacheServer.section_('views')

# These are all the active pages that Root.py should instantiate 
active = config.DASCacheServer.views.section_('active')
active.section_('rest')
active.rest.object = 'WMCore.WebTools.RESTApi'
active.rest.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
#active.rest.database = 'sqlite:////Users/metson/Documents/Workspace/GenDB/gendb.lite'
active.rest.database = 'sqlite:///:memory:'
active.rest.section_('model')
active.rest.model.object = 'DAS.web.DASCacheModel'
active.rest.model.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
active.rest.section_('formatter')
active.rest.formatter.object = 'DASRESTFormatter'
active.rest.formatter.templates = '/templates/WMCore/WebTools/'
#active.rest.logLevel = 'DEBUG'
# DASCacheMgr settings:
# sleep defines interval of checking cache queue
# verbose defines level of logger
active.rest.sleep = 2
active.rest.verbose = 0
