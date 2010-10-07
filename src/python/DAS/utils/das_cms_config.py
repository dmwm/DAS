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
from DAS.utils.das_config import DAS_OPTIONS

def read_wmcore(filename):
    """
    Read DAS python configuration file and store DAS parameters into
    returning dictionary.
    """
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

if  __name__ == '__main__':
    config = read_wmcore('config/das_cms.py')
    print config
