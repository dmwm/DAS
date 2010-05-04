#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_cli.py,v 1.20 2009/11/16 20:53:10 valya Exp $"
__version__ = "$Revision: 1.20 $"
__author__ = "Valentin Kuznetsov"

import time
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.utils.utils import dump, genkey

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
                                          type="int", default=None, 
                                          dest="verbose",
             help="verbose output")
        self.parser.add_option("--profile", action="store_true", 
                                          dest="profile",
             help="profile output")
        self.parser.add_option("-q", "--query", action="store", type="string", 
                                          default=False, dest="query",
             help="specify query for your request.")
        self.parser.add_option("--hash", action="store_true", dest="hash",
             help="look-up MongoDB-QL query and its hash")
        self.parser.add_option("--services", action="store_true", 
                                          dest="services",
             help="return a list of supported data services")
        self.parser.add_option("--keys", action="store", 
                                          dest="service",
             help="return set of keys for given data service")
#        self.parser.add_option("--view", action="store", 
#                                          dest="view",
#             help="return view definition in DAS, use --view=all to list all views")
#        self.parser.add_option("--create-view", action="store", 
#                                          dest="createview",
#             help="create a new view in DAS, e.g. --create-view=name,query")
#        self.parser.add_option("--update-view", action="store", 
#                                          dest="updateview",
#             help="update a view in DAS, e.g. --update-view=name,query")
#        self.parser.add_option("--delete-view", action="store", 
#                                          dest="deleteview",
#             help="delete a view in DAS, e.g. --delete-view=name")
        self.parser.add_option("--no-format", action="store_true", 
                                          dest="plain",
             help="return unformatted output, useful for scripting")
        self.parser.add_option("--idx", action="store", type="int", 
                                          default=0, dest="idx",
             help="start index for returned result set, aka pagination, use w/ limit")
        self.parser.add_option("--limit", action="store", type="int", 
                                          default=0, dest="limit",
             help="limit number of returned results")
        self.parser.add_option("--no-output", action="store_true", 
                                          dest="nooutput",
             help="run DAS but don't print results")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def iterate(input_results):
    """Just iterate over generator, but don't print it out"""
    for elem in input_results:
        pass

def run(DAS, query, idx, limit, nooutput, plain, debug):
    """
    Execute DAS workflow for given set of parameters.
    We use this function in main and in profiler.
    """
    results = DAS.result(query, idx, limit)
    if  not nooutput:
        if  plain:
            for item in results:
                print item
        else:
            dump(results, idx)
    else:
        iterate(results)
#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    t0 = time.time()
    query = opts.query
    debug = opts.verbose
    DAS = DASCore(debug=debug)
    if  opts.hash:
        mongo_query = DAS.mongoparser.requestquery(query)
        print "DAS-QL query:", query
        print "Mongo query :", mongo_query
        print "query hash  :", genkey(str(mongo_query))
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

        if  opts.profile:
            import cProfile # python profiler
            import pstats   # profiler statistics
            cProfile.run('run(DAS,query,idx,limit,output,plain,debug)', 'profile.dat')
            info = pstats.Stats('profile.dat')
            info.sort_stats('cumulative')
            info.print_stats()
        else:
            run(DAS, query, idx, limit, output, plain, debug)
    else:
        print
        print "DAS CLI interface, no actions found,"
        print "please use --help for more options."
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    if  debug:
        for key, val in DAS.timer.timer.items():
            if  len(val) > 1:
                print "DAS execution time (%s) %s sec" % (key, val[-1] - val[0])
    print "DAS execution time %s sec, %s" % ((time.time()-t0), timestamp)


