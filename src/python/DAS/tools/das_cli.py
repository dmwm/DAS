#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_cli.py,v 1.28 2010/04/05 19:10:45 valya Exp $"
__version__ = "$Revision: 1.28 $"
__author__ = "Valentin Kuznetsov"

import time
from pprint import pformat
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.core.das_query import DASQuery
#from DAS.utils.query_utils import convert2pattern, encode_mongo_query
from DAS.utils.utils import dump, genkey
from DAS.utils.das_timer import get_das_timer

import sys
if sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

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
        self.parser.add_option("-q", "--query", action="store", type="string", 
                                          default="", dest="query",
             help="specify query for your request.")
        self.parser.add_option("--hash", action="store_true", dest="hash",
             help="look-up MongoDB-QL query and its hash")
        self.parser.add_option("--services", action="store_true", 
                                          dest="services",
             help="return a list of supported data services")
        self.parser.add_option("--keys", action="store", 
                                          dest="service",
             help="return set of keys for given data service")
        self.parser.add_option("--print-config", action="store_true", 
                                          dest="dasconfig",
             help="print current DAS configuration")
        self.parser.add_option("--no-format", action="store_true", 
                                          dest="plain",
             help="return unformatted output, useful for scripting")
        self.parser.add_option("--idx", action="store", type="int", 
                                          default=0, dest="idx",
             help="start index for returned result set, aka pagination, use w/ limit")
        self.parser.add_option("--limit", action="store", type="int", 
                                          default=0, dest="limit",
             help="limit number of returned results")
        self.parser.add_option("--sort-key", action="store", type="string", 
                                          default=None, dest="skey",
             help="specify sorting key, please use dot notation for compound keys")
        self.parser.add_option("--sort-order", action="store", type="string", 
                                          default='asc', dest="sorder",
             help="specify sorting order, e.g. asc/desc")
        self.parser.add_option("--no-output", action="store_true", 
                                          dest="nooutput",
             help="run DAS workflow but don't print results")
        self.parser.add_option("--no-results", action="store_true", 
                                          dest="noresults",
             help="run DAS workflow but don't write results into the cache")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def iterate(input_results):
    """Just iterate over generator, but don't print it out"""
    for elem in input_results:
        pass

def run(DAS, query, idx, limit, skey, sorder, nooutput, plain, debug):
    """
    Execute DAS workflow for given set of parameters.
    We use this function in main and in profiler.
    """
    if  not nooutput:
        results = DAS.result(query, idx, limit, skey, sorder)
        if  plain:
            for item in results:
                print item
        else:
            dump(results, idx)
    else:
        results = DAS.call(query)
        print "\n### DAS.call returns", results

#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    t0 = time.time()
    query = opts.query
    debug = opts.verbose
    DAS = DASCore(debug=debug, nores=opts.noresults)
    if  opts.hash:
        dasquery = DASQuery(query)
        mongo_query = dasquery.mongo_query
        service_map = dasquery.service_apis_map()
        enc_query   = dasquery.storage_query
        loose_query = dasquery.loose_query
        print "---------------"
        print "DAS-QL query  :", query
        print "Mongo query   :", mongo_query
        print "Loose query   :", loose_query
        print "Encoded query :", enc_query
        print "enc_query hash:", genkey(enc_query)
        print "Services      :\n%s" % pformat(service_map) 
        sys.exit(0)
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

        idx    = opts.idx
        limit  = opts.limit
        output = opts.nooutput
        plain  = opts.plain
        skey   = opts.skey
        sorder = opts.sorder

        if  opts.profile:
            import cProfile # python profiler
            import pstats   # profiler statistics
            cmd  = 'run(DAS,query,idx,limit,skey,sorder,output,plain,debug)'
            cProfile.run(cmd, 'profile.dat')
            info = pstats.Stats('profile.dat')
            info.sort_stats('cumulative')
            info.print_stats()
        else:
            run(DAS, query, idx, limit, skey, sorder, output, plain, debug)
    elif opts.dasconfig:
        print pformat(DAS.dasconfig)
    else:
        print
        print "DAS CLI interface, no actions found,"
        print "please use --help for more options."
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    timer = get_das_timer()
    print "\nDAS execution time:\n"
    if  debug:
        timelist = []
        for hash, timerdict in timer.items():
            counter = timerdict['counter']
            tag = timerdict['tag']
            exetime = timerdict['time']
            timelist.append((counter, tag, exetime))
        timelist.sort()
        for _, tag, exetime in timelist:
            print "%s %s sec" % (tag, round(exetime, 2))
    print "Total %s sec, %s" % (round(time.time()-t0, 2), timestamp)
