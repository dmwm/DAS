#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface to clean-up records in DAS cache
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
from optparse import OptionParser
from DAS.core.das_cache import DASCache
from DAS.utils.utils import dump

class DASOptionParser: 
    """
     DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                                          type="int", default=0, 
                                          dest="verbose",
             help="verbose output")
        self.parser.add_option("-c", "--cache", action="store", 
                                          type="string", default=None, 
                                          dest="cache",
             help="specify which cache to clean, e.g. memcache or couch")
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

    t0 = time.time()
    if  opts.verbose:
        debug = opts.verbose
    else:
        debug = 0
    DAS = DASCache(debug=debug)
    results = DAS.clean_cache(cache=opts.cache)
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    print "DAS execution time %s sec, %s" % ((time.time()-t0), timestamp)
