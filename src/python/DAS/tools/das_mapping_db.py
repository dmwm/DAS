#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_mapping_db.py,v 1.25 2010/02/16 18:37:01 valya Exp $"
__version__ = "$Revision: 1.25 $"
__author__ = "Valentin Kuznetsov"

import sys
from optparse import OptionParser
from DAS.core.das_mapping_db import DASMapping
from DAS.utils.logger import DASLogger
from DAS.services.map_reader import read_service_map

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                                          type="int", default=None, 
                                          dest="debug",
             help="verbose output")
        self.parser.add_option("--host", action="store", type="string",
             default="localhost", dest="host", help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="int",
             default=27017 , dest="port", help="specify MongoDB port number")
        self.parser.add_option("--db", action="store", type="string",
             default="mapping" , dest="db", help="specify MongoDB db name")
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

    logger = DASLogger(verbose=opts.debug, stdout=opts.debug)
    config = dict(logger=logger, verbose=opts.debug,
        mapping_dbhost=opts.host, mapping_dbport=opts.port, 
        mapping_dbname=opts.db)

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

    if  opts.remove:
        mgr.remove(opts.remove)
