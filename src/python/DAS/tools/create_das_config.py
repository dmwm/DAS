#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS config generator
"""
__revision__ = "$Id: create_das_config.py,v 1.3 2009/04/29 19:52:12 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

from DAS.utils.das_config import das_writeconfig

#
# main
#
if __name__ == '__main__':
    dasconfig = das_writeconfig()
    print "DAS configuration file has been created"
    print dasconfig
