#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,C0301

"""
DAS web configuration file
"""

__revision__ = "$Id: das_webconfig.py,v 1.13 2009/07/02 20:19:16 valya Exp $"
__version__ = "$Revision: 1.13 $"
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
config.Webtools.port = 8212
# INADDR_ANY: listen on all interfaces (be visible outside of localhost)
#config.Webtools.host = '0.0.0.0' 
# listen only to localhost, do not allow connection outside of it, this can be
# used to hide service behind front-end
config.Webtools.host = '127.0.0.1' 
config.Webtools.application = 'DASWeb'

# This is the config for the application
config.component_('DASWeb')
# Define the default location for templates for the app
config.DASWeb.templates = environ['DAS_ROOT'] + '/src/templates'

# Define the class that is the applications index
#config.DASWeb.index = 'das'

# Views are all pages 
config.DASWeb.section_('views')

# These are all the active pages that Root.py should instantiate 
active = config.DASWeb.views.section_('active')
# The section name is also the location the class will be located
# e.g. http://localhost:8080/documentation
active.section_('documentation')
# The class to load for this view/page
active.documentation.object = 'WMCore.WebTools.Documentation'
# I could add a variable to the documenation object if I wanted to as follows:
# active.documentation.foo = 'bar'

active.section_('das')
active.das.object = 'DAS.web.DASSearch'
active.das.cache_server_url = 'http://localhost:8211'

# Controllers are standard way to return minified gzipped css and js
active.section_('dascontrollers')
# The class to load for this view/page
active.dascontrollers.object = 'WMCore.WebTools.Controllers'
# The configuration for this object - the location of css and js
active.dascontrollers.css = {
#    'reset.css': environ['YUI_ROOT'] + '/build/reset/reset.css', 
    'cms_reset.css': environ['WMCORE_ROOT'] + '/src/css/WMCore/WebTools/cms_reset.css', 
    'das.css': environ['DAS_ROOT'] + '/src/css/das.css'
}

active.dascontrollers.js = {
    'prototype.js' : environ['DAS_ROOT'] + '/src/js/prototype.js',
    'rico.js' : environ['DAS_ROOT'] + '/src/js/rico.js',
    'utils.js' : environ['DAS_ROOT'] + '/src/js/utils.js',
    'ajax_utils.js' : environ['DAS_ROOT'] + '/src/js/ajax_utils.js',
}
active.dascontrollers.images = {
    'loading.gif' : environ['DAS_ROOT'] + '/src/images/loading.gif',
    'cms_logo.jpg' : environ['DAS_ROOT'] + '/src/images/cms_logo.jpg',
    'cms_logo.png' : environ['DAS_ROOT'] + '/src/images/cms_logo.png',
}
# These are pages in "maintenance mode" - to be completed
maint = config.DASWeb.views.section_('maintenance')

active.section_('masthead')
active.masthead.object = 'WMCore.WebTools.Masthead'
active.masthead.templates = environ['WMCORE_ROOT'] + '/src/templates/WMCore/WebTools/Masthead'

