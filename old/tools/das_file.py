#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface to read files in DAS cache
"""
from __future__ import print_function
__revision__ = "$Id: das_file.py,v 1.1 2010/01/19 19:02:58 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import os
import marshal
from optparse import OptionParser

import sys
if sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-f", "--file", action="store", type="string", 
                                          default=False, dest="file",
             help="specify query for your request.")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()
#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    filename = opts.file
    if  os.path.isfile(filename):
        fdr = open(filename, 'rb')
        while 1:
            try:
                res = marshal.load(fdr)
                print(res)
            except EOFError as err:
                break
        fdr.close()
    else:
        print("No such file %s" % filename)
