#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS admin tool to handle DAS records in DAS cache server
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import sys
import time
import traceback
from optparse import OptionParser
from DAS.utils.das_config import das_readconfig

# monogo db modules
from pymongo.connection import Connection

class DASOptionParser: 
    """
     DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                              type="int", default=0, dest="verbose",
             help="verbose output")
        self.parser.add_option("--stats", action="store_true", dest="stats",
             help="provide information about DAS records in MongoDB")
        self.parser.add_option("--system", action="store", dest="system",
             default="", type="string",
             help="provide information about specific DAS system, e.g. dbs")
        self.parser.add_option("--delete-expired", action="store_true", dest="delete",
             help="delete expired records")
        self.parser.add_option("--renew-expired", action="store_true", dest="renew",
             help="renew expired records")
        self.parser.add_option("--interval", action="store", dest="interval",
             default="0", type="int",
             help="specify interval in seconds for renewal process")
        self.parser.add_option("--host", action="store", type="string", 
             default="localhost", dest="host",
             help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="int", 
             default="27017", dest="port",
             help="specify MongoDB port")
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

class DASMongoDB(object):
    """
    General class to work with DAS mongo dbs.
    """
    def __init__(self, host, port):
        self.host   = host
        self.port   = port
        self.conn   = Connection(host, port)
        self.cache  = self.conn['das']['cache']
        self.line   = "-"*80
        self.config = das_readconfig()

    def delete(self, system=None):
        """
        Delete expired documents in das.cache.
        """
        spec = {'das.expire':{'$lt':time.time()}}
        if  system:
            spec['das.system'] = system
        msg = "Found %s expired documents" % self.cache.find(spec).count()
        try:
            self.cache.remove(spec)
            msg += ", delete operation [OK]"
            print msg
        except:
            msg += ", delete operation [FAIL]"
            print msg
            traceback.print_exc()

    def renew(self, interval, system=None):
        """
        Renew expired documents in das.cache for provided interval of time
        in seconds and system.
        """
        spec = {'das.expire':{'$lt':time.time()}}
        if  system:
            spec['das.system'] = system
        msg = "Found %s expired document(s)\n" % self.cache.find(spec).count()
        idx = 0
        for row in self.cache.find(spec):
            spec = {'_id':row['_id']}
            row['das']['expire'] += interval
            self.cache.update(spec, row)
            idx += 1
        msg += "Updated %s document(s)" % idx
        print msg

    def stats(self, isystem=None):
        """
        Retrieve some statistics about DAS DBs.
        """
        nrecords    = self.cache.find({}).count()
        nexpire     = self.cache.find({'das.expire' : {'$lt':time.time()}}).count()
        timestamp   = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        systems     = self.config['systems']

        if  isystem:
            systems = [isystem]

        print self.line
        print "DB report, cache,", timestamp 
        print "total number of records:", nrecords
        print "total number of expired records:", nexpire
        for system in systems:
            spec = {'das.system' : system}
            nrec = self.cache.find(spec).count()
            print "number of records for %s: %s" % (system, nrec)
        # get info about indexes
        print "Existing indexes:"
        for row in self.conn.das.system.indexes.find({}):
            print row

# see this thread
# http://groups.google.com/group/mongodb-user/browse_thread/thread/2c62409009b5a3e4
# how to get info about DB/index statistics
# should be available in future Mongo releases.
#        func = "function(){return this._db.runCommand({collstats: this._shortName});"
#        res = self.cache.database().eval(func)
#        print "eval res", res

        if  not isystem:
            nrecords   = self.conn.analytics.db.find({}).count()
            print self.line
            print "DB report, analytics,", timestamp 
            print "total number of records:", nrecords
            # get info about indexes
            print "Existing indexes:"
            for row in self.conn.analytics.system.indexes.find({}):
                print row

            nrecords   = self.conn.mapping.db.find({}).count()
            print self.line
            print "DB report, mapping,", timestamp 
            print "total number of records:", nrecords
            # get info about indexes
            print "Existing indexes:"
            for row in self.conn.mapping.system.indexes.find({}):
                print row

#
# main
#
if __name__ == '__main__':
    OMGR = DASOptionParser()
    (opts, args) = OMGR.get_opt()

    DASMONGO = DASMongoDB(opts.host, opts.port)
   
    if  opts.stats:
        DASMONGO.stats(opts.system)
        sys.exit(0)

    if  opts.delete:
        DASMONGO.delete(opts.system)
        sys.exit(0)

    if  opts.renew:
        if  not opts.interval:
            print "Specify non-zero interval"
            sys.exit(0)
        DASMONGO.renew(opts.interval, opts.system)
        sys.exit(0)

