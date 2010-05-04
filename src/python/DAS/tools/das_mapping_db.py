#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_mapping_db.py,v 1.20 2010/01/27 16:53:57 valya Exp $"
__version__ = "$Revision: 1.20 $"
__author__ = "Valentin Kuznetsov"

import os
import sys
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.utils.utils import dump
from DAS.core.das_mapping_db import DASMapping
from DAS.utils.logger import DASLogger
from DAS.services.map_reader import read_api_map
from DAS.services.map_reader import read_notation_map
from DAS.services.map_reader import read_presentation_map
from DAS.utils.das_config import ACTIVE_SYSTEMS

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
                                          dest="debug",
             help="verbose output")
#        self.parser.add_option("--db", action="store", type="string", 
#             default='das_mapping.db', dest="dbfile",
#             help="specify DB file to use.")
        self.parser.add_option("--host", action="store", type="string",
             default="localhost", dest="host", help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="int",
             default=27017 , dest="port", help="specify MongoDB port number")
        self.parser.add_option("--db", action="store", type="string",
             default="mapping" , dest="db", help="specify MongoDB db name")
        self.parser.add_option("--system", action="store", type="string",
             default=None , dest="system", help="specify DAS sub-system")
        self.parser.add_option("--list-apis", action="store_true", 
             dest="listapis", help="return a list of APIs")
        self.parser.add_option("--list-daskeys", action="store_true", 
             dest="listkeys", help="return a list of DAS keys")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def nooutput(results):
    """Just iterate over generator, but don't print it out"""
    for i in results:
        a = 1
#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    logger = DASLogger(verbose=opts.debug, stdout=opts.debug)
    config = dict(logger=logger, verbose=opts.debug,
        mapping_dbhost=opts.host, mapping_dbport=opts.port, mapping_dbname=opts.db)

    mgr = DASMapping(config)

    if  opts.listapis:
        apis = mgr.list_apis(opts.system)
        print apis
        sys.exit(0)

    if  opts.listkeys:
        keys = mgr.list_daskeys(opts.system)
        print keys
        sys.exit(0)
    # daskeys defines a mapping between daskey used in DAS-QL and DAS record key
    # e.g. block means block.name, the pattern defines regular expression to which
    # daskey should satisfy
    #
    # api2das defines mapping between API input parameter and DAS-QL key,
    # dict(api_param='storage_element_name', das_key='site', 
    #      pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
    # here pattern is applied to passed api_param

    # clean-up existing Mapping DB
    mgr.delete_db()
    mgr.create_db()

    # read API, notation maps for all known data-services
    services = ACTIVE_SYSTEMS.split(',')
    for srv in services:
        print "\nAdding API map for %s system" % srv
        for rec in read_api_map(srv):
            if  opts.debug:
                print rec
            mgr.add(rec)
        print "Adding notation map for %s system" % srv
        rec = read_notation_map(srv)
        if  opts.debug:
            print rec
        mgr.add(rec)
    print "\nAdding presentation map for all systems"
    rec = read_presentation_map()
    if  opts.debug:
        print rec
    mgr.add(rec)
 
    print "New DAS Mapping DB has been created"
