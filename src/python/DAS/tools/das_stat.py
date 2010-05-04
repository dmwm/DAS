#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface to get results from CouchDB
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import sys
import time
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.core.das_couchdb import DASCouchDB
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
        self.parser.add_option("--listviews", action="store_true", 
                                          dest="listviews",
             help="return a list of supported couch db views")
        self.parser.add_option("--view", action="store", type="string", 
                                          dest="view", default="query",
             help="specify which view to look at")
        self.parser.add_option("--limit", action="store", type="int", 
                                          default=10, dest="limit",
             help="limit number of returned results")
        self.parser.add_option("--order", action="store", type="string", 
                                          default="desc", dest="order",
             help="specify which order to use, e.g. desc or asc")
        self.parser.add_option("--system", action="store", type="string", 
                                          default=None, dest="system",
             help="specify which order to use, e.g. desc or asc")
        self.parser.add_option("--timestamp", action="store", type="int", 
                                          default=0, dest="time",
             help="specify time int(sec. since epoch)")
        self.parser.add_option("--group", action="store_true", 
                                          dest="group",
             help="group results")
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
    MGR = DASCore(debug=debug)
    DAS = DASCouchDB(MGR)
    if  opts.listviews:
        views = DAS.list_views()
        for view in views:
            print view
        sys.exit(0)
        
    if  opts.view == 'query':
        skey = '["%s", %s ]' % (key, timestamp())
        ekey = '["%s", %s ]' % (key, 9999999999)
        options = {'startkey': skey, 'endkey': ekey}
    elif opts.view == 'system':
        key = '"%s"' % opts.system
        options = {'key' : key}
    elif opts.view == 'timer':
        key = '%s' % opts.time
        options = {'key' : key}
#    if  opts.order:
#        if  opts.order == 'desc':
#            options['descending'] = True
#        else:
#            options['descending'] = False
    if  opts.group:
        options['group'] = opts.group
#    if  opts.limit:
#        options['limit'] = opts.limit
    print options
    results = DAS.get_view(opts.view, options)
    for row in results:
        print row
#    dump(results, opts.limit)
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    print "DAS execution time %s sec, %s" % ((time.time()-t0), timestamp)
