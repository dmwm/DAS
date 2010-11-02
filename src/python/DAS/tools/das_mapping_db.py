#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_mapping_db.py,v 1.28 2010/04/14 20:39:13 valya Exp $"
__version__ = "$Revision: 1.28 $"
__author__ = "Valentin Kuznetsov"

import sys
from optparse import OptionParser
from DAS.core.das_mapping_db import DASMapping
from DAS.utils.logger import DASLogger
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection
from DAS.services.map_reader import read_service_map

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                                          type="int", default=0, 
                                          dest="debug",
             help="verbose output")
        self.parser.add_option("--host", action="store", type="string",
             default="localhost", dest="host", help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="int",
             default=27017 , dest="port", help="specify MongoDB port number")
        self.parser.add_option("--db", action="store", type="string",
             default="mapping.db" , dest="db", help="specify MongoDB db name")
        self.parser.add_option("--system", action="store", type="string",
             default=None , dest="system", help="specify DAS sub-system")
        self.parser.add_option("--uri-map", action="store", type="string",
             default=None , dest="umap", help="specify uri map file")
        self.parser.add_option("--notation-map", action="store", type="string",
             default=None , dest="nmap", help="specify notation map file")
        self.parser.add_option("--presentation-map", action="store", type="string",
             default=None , dest="pmap", help="specify presentation map file")
        self.parser.add_option("--list-apis", action="store_true", 
             dest="listapis", help="return a list of APIs")
        self.parser.add_option("--list-daskeys", action="store_true", 
             dest="listkeys", help="return a list of DAS keys")
        self.parser.add_option("--remove", action="store", type="string",
             default=None , dest="remove", 
             help="remove DAS Mapping DB record, provide the spec")
        self.parser.add_option("--clean", action="store_true", 
             dest="clean", help="clean DAS mapping DB")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def nooutput(results):
    """Just iterate over generator, but don't print it out"""
    for row in results:
        pass
#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    dburi     = 'mongodb://%s:%s' % (opts.host, opts.port)
    dasconfig = das_readconfig()
    logfile   = dasconfig['das'].get('logfile', None)
    logger    = DASLogger(logfile=logfile, verbose=opts.debug)
    dbname, colname = opts.db.split('.')
    mongodb   = dict(dburi=dburi)
    mappingdb = dict(dbname=dbname, collname=colname)
    config    = dict(logger=logger, verbose=opts.debug, mappingdb=mappingdb,
                mongodb=mongodb, services=dasconfig['das'].get('services', []))

    mgr = DASMapping(config)

    if  opts.listapis:
        apis = mgr.list_apis(opts.system)
        print apis
        sys.exit(0)

    if  opts.listkeys:
        keys = mgr.daskeys(opts.system)
        print keys
        sys.exit(0)

    if  opts.umap:
        for rec in read_service_map(opts.umap, field='uri'):
            if  opts.debug:
                print rec
            spec = {'url':rec['url'], 'urn':rec['urn']}
            mgr.remove(spec) # remove previous record
            mgr.add(rec)

    if  opts.nmap:
        for rec in read_service_map(opts.nmap, field='notations'):
            if  opts.debug:
                print rec
            system = rec['system']
            spec = {'notations':{'$exists':True}, 'system':system}
            mgr.remove(spec) # remove previous record
            mgr.add(rec)

    if  opts.pmap:
        for rec in read_service_map(opts.pmap, field='presentation'):
            if  opts.debug:
                print rec
            spec = {'presentation':{'$exists':True}}
            mgr.remove(spec) # remove previous record
            mgr.add(rec)

    if  opts.clean:
        mgr.delete_db()
        mgr.create_db()
        # I need to clear DAS cache/merge since I don't know
        # a-priory what kind of changes new maps will bring
        conn   = db_connection(dburi)
        dbname = dasconfig['dasdb']['dbname']
        cache  = conn[dbname][dasconfig['dasdb']['cachecollection']]
        cache.remove({})
        merge  = conn[dbname][dasconfig['dasdb']['mergecollection']]
        merge.remove({})

    if  opts.remove:
        mgr.remove(opts.remove)
