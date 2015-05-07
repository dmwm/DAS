#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface to clean-up records in DAS cache
"""
from __future__ import print_function
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
from optparse import OptionParser
from DAS.utils.das_config import das_readconfig
from DAS.utils.logger import DASLogger
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
        self.parser.add_option("-l", "--list-caches", action="store_true", 
                                          dest="listcaches",
             help="specify which cache to clean, e.g. memcache")
        self.parser.add_option("-c", "--cache", action="store", 
                                          type="string", default=None, 
                                          dest="cache",
             help="specify which cache to clean, e.g. memcache")
        self.parser.add_option("-d", "--delete", action="store", 
                                          type="string", default="das", 
                                          dest="delete",
             help="clean cache (delete/invalidate) all records in cache")
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
    config = das_readconfig()
    logger = DASLogger(verbose=debug, stdout=debug)
    config['logger'] = logger
    config['verbose'] = debug
    DAS = DASCache(config)
    if  opts.listcaches:
        for name, obj in DAS.servers.items():
            print(name, obj)
            if  name == 'memcache':
                print("cache lifetime: %s" % obj.limit)
                print("cache servers : %s" % obj.servers)
            elif  name == 'couchcache':
                print("cache lifetime: %s" % obj.limit)
                print("cache servers : %s" % obj.uri)
            elif  name == 'filecache':
                print("cache lifetime: %s" % obj.limit)
                print("cache dir     : %s" % obj.dir)
    elif  opts.delete:
        DAS.delete_cache(dbname=opts.delete, cache=opts.cache)
    else:
        DAS.clean_cache(cache=opts.cache)
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    print("DAS execution time %s sec, %s" % ((time.time()-t0), timestamp))
