#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_cli.py,v 1.1 2009/03/09 19:43:34 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import time
from optparse import OptionParser
#from DAS.core.das_core import DASCore
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
        results = DAS.result(query)
#        print '\n+++++++++ DAS output ++++++++++\n'
        dump(results, opts.limit)
    else:
        print
        print "DAS CLI interface, no actions found,"
        print "please use --help for more options."
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    print "DAS execution time %s sec, %s" % ((time.time()-t0), timestamp)
#    DAS = DASCache(
#    if  opts.verbose:
#        DAS.verbose = 1
#    if  opts.json:
#        json = DAS.json(query)
#        print '\n+++++++++ DAS output ++++++++++\n'
#        print json
#    else:
#        results = DAS.call(query)
#        print '\n+++++++++ DAS output ++++++++++\n'
#        dump(results, opts.limit)
