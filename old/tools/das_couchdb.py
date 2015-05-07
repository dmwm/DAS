#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface to get results from CouchDB
"""
from __future__ import print_function
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import sys
import time
import types
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.core.das_couchdb import DASCouchDB
from DAS.utils.utils import dump, timestamp, genkey

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
        self.parser.add_option("--delete", action="store", type="string", 
                                          default=None, dest="delete",
             help="delete couch DB")
        self.parser.add_option("--listviews", action="store_true", 
                                          dest="listviews",
             help="return a list of supported couch db views")
        self.parser.add_option("--listqueries", action="store_true", 
                                          dest="queries",
             help="return a list of all queries found in CouchDB")
        self.parser.add_option("--query", action="store", type="string", 
                                          default=None, dest="query",
             help="look-up results for given query")
        self.parser.add_option("--system", action="store", type="string", 
                                          default=None, dest="system",
             help="look-up results for given system, e.g. dbs, phedex, sitedb")
        self.parser.add_option("--fromtime", action="store", type="int", 
                                          default=0, dest="fromtime",
             help="specify time int(sec. since epoch), to be used in timer view")
        self.parser.add_option("--totime", action="store", type="int", 
                                          default=0, dest="totime",
             help="specify time int(sec. since epoch), to be in timer view")
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

    if  not len([val for val in opts.__dict__.values() if val]):
        print("Run with --help for more options")
        sys.exit(0)

    t0 = time.time()
    if  opts.verbose:
        debug = opts.verbose
    else:
        debug = 0
    MGR = DASCore(debug=debug)
    DAS = DASCouchDB(MGR)
    if  opts.listviews:
        for viewname, viewmap in DAS.views.items():
            print()
            print("DAS view:", viewname)
            print(viewmap['map'])
        sys.exit(0)
        
    if  opts.delete:
        if  opts.system:
            msg = "Delete '%s' docs in '%s' couch DB" % \
                (opts.system, opts.delete)
            DAS.delete(opts.delete, opts.system)
        else:
            msg = "Delete '%s' couch DB" % opts.delete
            DAS.delete(opts.delete)
        print(msg, end=' ')
        sys.exit(0)
        
    t1 = 0
    t2 = timestamp()
    if  opts.fromtime:
        t1 = opts.fromtime
    if  opts.totime:
        t2 = opts.totime

    options = {'group':'true'}
    view = 'timer'
    design = 'dasviews'
    if  opts.query:
        key  = genkey(opts.query)
#        skey = '["%s", %s ]' % (key, t1)
#        ekey = '["%s", %s ]' % (key, t2)
        skey = '["%s", %s ]' % (key, timestamp())
        ekey = '["%s", %s ]' % (key, 9999999999)
        options = {'startkey': skey, 'endkey': ekey}
        view = 'query'
        design = 'dasviews'
    elif opts.system:
        key = '"%s"' % opts.system
        options = {'key' : key}
        view = 'system'
        design = 'dasadmin'
    elif opts.queries:
        view = 'queries'
        design = 'dasadmin'
    else:
        skey = '%s' % t1
        ekey = '%s' % t2
        options = {'startkey': skey, 'endkey': ekey}
        view = 'timer'
        design = 'dasadmin'
    results = DAS.get_view(design, view, options)
    if type(results) is list:
        for row in results:
            print(row)
        print("Found %s documents" % len(results))
    else:
        print(results)
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    print("DAS execution time %s sec, %s" % ((time.time()-t0), timestamp))
