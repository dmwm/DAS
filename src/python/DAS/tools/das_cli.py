#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_cli.py,v 1.3 2009/04/29 16:09:51 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

import time
from optparse import OptionParser
#from DAS.core.das_core import DASCore
from DAS.core.das_cache import DASCache
from DAS.utils.utils import dump

import sys
if sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")

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
        self.parser.add_option("--profile", action="store_true", 
                                          dest="profile",
             help="profile output")
        self.parser.add_option("--input", action="store", type="string", 
                                          default=False, dest="input",
             help="specify input for your request.")
        self.parser.add_option("--services", action="store_true", 
                                          dest="services",
             help="return a list of supported data services")
        self.parser.add_option("--keys", action="store", 
                                          dest="service",
             help="return set of keys for given data service")
#        self.parser.add_option("--json", action="store_true", 
#                                          dest="json",
#             help="return JSON dict")
        self.parser.add_option("--limit", action="store", type="int", 
                                          default=0, dest="limit",
             help="limit number of returned results")
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
    query = opts.input
#    debug = 1
    if  opts.verbose:
        debug = opts.verbose
    else:
        debug = 0
    DAS = DASCache(debug=debug)
    sdict = DAS.keys()
    if  opts.services:
        msg = "DAS services:" 
        print msg
        print "-"*len(msg)
        keys = sdict.keys()
        keys.sort()
        for key in keys:
            print key
    elif  opts.service:
        msg = "DAS service %s:" % opts.service
        print msg
        print "-"*len(msg)
        keys = sdict[opts.service]
        keys.sort()
        for key in keys:
            print key
    elif query:

        if  opts.profile:
            import hotshot                   # Python profiler
            import hotshot.stats             # profiler statistics
            print "Start DAS in profile mode"
            profiler = hotshot.Profile("profile.dat")
            profiler.run("DAS.result(query)")
            profiler.close()
            stats = hotshot.stats.load("profile.dat")
            stats.sort_stats('time', 'calls')
            stats.print_stats()
        else:
            results = DAS.result(query)
            dump(results, opts.limit)
    else:
        print
        print "DAS CLI interface, no actions found,"
        print "please use --help for more options."
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    print "DAS execution time %s sec, %s" % ((time.time()-t0), timestamp)


