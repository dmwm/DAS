#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=C0301,C0103,E1101

"""
DAS admin tool to handle DAS records in DAS cache server
"""
from __future__ import print_function
__author__ = "Valentin Kuznetsov"

# system modules
import sys
import json
import time

# DAS modules
from optparse import OptionParser
from DAS.utils.das_config import das_readconfig
from DAS.utils.utils import print_exc

# monogo db modules
from pymongo import MongoClient

class PrintManager:
    """
    Print manager class defines color-full output of the messages.
    """
    def __init__(self):
        try:
            from IPython import ColorANSI
            self.term = ColorANSI.TermColors
        except:
            self.term = None

    def red(self, msg):
        """yield message using red color"""
        if  not msg:
            msg = ''
        if  self.term:
            return self.term.Red + msg + self.term.Black
        else:
            return msg

    def purple(self, msg):
        """yield message using purple color"""
        if  not msg:
            msg = ''
        if  self.term:
            return self.term.Purple + msg + self.term.Black
        else:
            return msg

    def cyan(self, msg):
        """yield message using cyan color"""
        if  not msg:
            msg = ''
        if  self.term:
            return self.term.LightCyan + msg + self.term.Black
        else:
            return msg

    def green(self, msg):
        """yield message using green color"""
        if  not msg:
            msg = ''
        if  self.term:
            return self.term.Green + msg + self.term.Black
        else:
            return msg

    def blue(self, msg):
        """yield message using blue color"""
        if  not msg:
            msg = ''
        if  self.term:
            return self.term.Blue + msg + self.term.Black
        else:
            return msg

# Globals, to be use in magic functions
PM = PrintManager()

def print_dict(idict):
    """
    Pretty print of input dictionary
    """
    sys.stdout.write(PM.red("{"))
    for key, val in idict.items():
        if  isinstance(val, list):
            sys.stdout.write( "'%s':" % PM.blue(key) )
            sys.stdout.write( PM.purple('[') )
            for item in val:
                if  isinstance(item, dict):
                    print_dict(item)
                else:
                    sys.stdout.write( "'%s'" % item )
                if  item != val[-1]:
                    sys.stdout.write(", ")
            sys.stdout.write(PM.purple(']'))
        elif isinstance(val, dict):
            print_dict(val)
        else:
            sys.stdout.write( "'%s':'%s'" % (PM.blue(key), val) )
        if  key != list(idict.keys())[-1]:
            sys.stdout.write(", ")
        else:
            sys.stdout.write("")
    sys.stdout.write(PM.red("}"))

class DASOptionParser:
    """
     DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("--host", action="store", type="string",\
             default="localhost", dest="host",\
             help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="int",\
             default="27017", dest="port",\
             help="specify MongoDB port")
        self.parser.add_option("--db", action="store", dest="db",\
             default="das.cache", type="string",\
             help="specify db to use, supported: das.cache, mapping.db")
        self.parser.add_option("--system", action="store", dest="system",\
             default="", type="string",\
             help="provide information about specific DAS system, e.g. dbs")
        self.parser.add_option("--stats", action="store_true", dest="stats",\
             help="provide information about DAS records in MongoDB")
        self.parser.add_option("--records", action="store_true", dest="records",\
             help="fetch and print DAS records, can be applied together with --system")
        self.parser.add_option("--spec", action="store", dest="spec",\
             default="{}", type="string",\
             help="specify conditions, using MongoDB syntax")
        self.parser.add_option("--fields", action="store", dest="fields",\
             default="[]", type="string",\
             help="specify selection fields, using MongoDB syntax")
        self.parser.add_option("--delete-expired", action="store_true", dest="delete",\
             help="delete expired records")
        self.parser.add_option("--renew-expired", action="store_true", dest="renew",\
             help="renew expired records")
        self.parser.add_option("--clear", action="store_true", dest="clear", \
             help="clean up DAS, remove entries from parser.db, das.cache, das.merge")
        self.parser.add_option("--clean", action="store", dest="clean",\
             default=None, type="string",\
             help="clean up DAS db.collection")
        self.parser.add_option("--interval", action="store", dest="interval",\
             default="0", type="int",\
             help="specify interval in seconds for renewal process")
        self.parser.add_option("--pretty-print", action="store_true", dest="pretty",\
             help="invoke pretty print function to colorize record output")

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
        self.conn   = MongoClient(host, port)
        self.cache  = self.conn['das']['cache']
        self.line   = "-"*80
        self.config = das_readconfig()

    def clear(self):
        """
        Clear DAS, remove entries from all db.collections
        """
        dbname = self.config['dasdb']['dbname']
        dbcoll = self.config['dasdb']['cachecollection']
        self.clean(dbname, dbcoll)
        dbcoll = self.config['dasdb']['mergecollection']
        self.clean(dbname, dbcoll)
        dbcoll = self.config['dasdb']['mrcollection']
        self.clean(dbname, dbcoll)
        # parser
        dbname = 'parser'
        dbcoll = 'db'
        self.clean(dbname, dbcoll)

    def clean(self, dbname, dbcoll):
        """
        Perform DAS clean-up on given db.collection
        """
        coll = self.conn[dbname][dbcoll]
        if  dbname == 'parser':
            coll.drop() # capped collection use drop
        else:
            coll.remove({}) # normal collections use remove
            coll.drop_indexes()

    def fetch(self, spec, fields, db=None, system=None, pretty=False):
        """
        Retrieve DAS records
        """
        # provided spec is a str representation of a dict
        spec   = json.loads(spec)
        # provided fields are in a form of "[bla, bla]"
        fields = fields.replace('[', '').replace(']', '').split(',')
        if  not fields:
            fields = None
        if  not db:
            db = 'das.cache'
        if  system:
            spec['das.system'] = system
        dbname, collection = db.split('.')
        print(self.line)
        print("MongoDB query:", dict(spec=spec, fields=fields))
        idx = 0
        for row in self.conn[dbname][collection].find(spec, fields):
            print("\nrow: %s" % idx)
            if  pretty:
                print_dict(row)
                print()
            else:
                print(row)
            idx += 1

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
            print(msg)
        except Exception as exc:
            msg += ", delete operation [FAIL]"
            print(msg)
            print_exc(exc)

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
        print(msg)

    def stats(self, isystem=None):
        """
        Retrieve some statistics about DAS DBs.
        """
        nrecords    = self.cache.count()
        nexpire     = self.cache.find({'das.expire' : {'$lt':time.time()}}).count()
        timestamp   = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        systems     = self.config['systems']

        if  isystem:
            systems = [isystem]

        print(self.line)
        print("DB report, cache,", timestamp)
        print("total number of records:", nrecords)
        print("total number of expired records:", nexpire)
        for system in systems:
            spec = {'das.system' : system}
            nrec = self.cache.find(spec).count()
            print("number of records for %s: %s" % (system, nrec))
        # get info about indexes
        print("Existing indexes:")
        for row in self.conn.das.system.indexes.find({}):
            print(row)

# see this thread
# http://groups.google.com/group/mongodb-user/browse_thread/thread/2c62409009b5a3e4
# how to get info about DB/index statistics
# should be available in future Mongo releases.
#        func = "function(){return this._db.runCommand({collstats: this._shortName});"
#        res = self.cache.database().eval(func)
#        print "eval res", res

        if  not isystem:
            nrecords   = self.conn.mapping.db.count()
            print(self.line)
            print("DB report, mapping,", timestamp)
            print("total number of records:", nrecords)
            # get info about indexes
            print("Existing indexes:")
            for row in self.conn.mapping.system.indexes.find({}):
                print(row)

#
# main
#
if __name__ == '__main__':
    OMGR = DASOptionParser()
    (opts, args) = OMGR.get_opt()

    DASMONGO = DASMongoDB(opts.host, opts.port)

    if  opts.stats:
        DASMONGO.stats(opts.system)

    if  opts.records:
        DASMONGO.fetch(opts.spec, opts.fields, opts.db, opts.system, opts.pretty)

    if  opts.delete:
        DASMONGO.delete(opts.system)

    if  opts.renew:
        if  not opts.interval:
            print("Specify non-zero interval")
            sys.exit(0)
        DASMONGO.renew(opts.interval, opts.system)

    if  opts.clear:
        DASMONGO.clear()

    if  opts.clean:
        try:
            DBNAME, DBCOLL = opts.clean.split('.')
            DASMONGO.clean(DBNAME, DBCOLL)
        except:
            print("Please provide <dbname>.<dbcollection>")

