#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS config generator
"""
__revision__ = "$Id: create_das_config.py,v 1.2 2009/04/07 19:54:17 valya Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "Valentin Kuznetsov"

import os
from DAS.utils.das_config import das_writeconfig

#
# main
#
if __name__ == '__main__':
    file = das_writeconfig()
#    file = os.path.join(os.getcwd(), 'das.cfg')
    print "DAS configuration file has been created"
    print file
