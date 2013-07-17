#!/usr/bin/env python

"""
DAS map reader which transform DAS YML files into JSON docs and
dump them on stdout
"""
__author__ = "Valentin Kuznetsov"

import os
import json
from optparse import OptionParser
from DAS.services.map_reader import read_service_map

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("--uri-map", action="store", type="string",
             default=None , dest="umap", help="specify uri map file")
        self.parser.add_option("--notation-map", action="store", type="string",
             default=None , dest="nmap", help="specify notation map file")
        self.parser.add_option("--presentation-map", action="store",
             type="string",
             default=None , dest="pmap", help="specify presentation map file")
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def nooutput(results):
    """Just iterate over generator, but don't print it out"""
    for _row in results:
        pass

def main():
    """Main function"""
    opt_mgr = DASOptionParser()
    (opts, _args) = opt_mgr.get_opt()

    if  opts.umap:
        count = 0
        system = ''
        for rec in read_service_map(opts.umap, field='uri'):
            print json.dumps(rec)
            count += 1
            system = rec['system']
        if  system: # notations map must have system
            arecord = {'type': 'uri', 'count': count, 'system': system}
            print json.dumps(dict(arecord=arecord))

    if  opts.nmap:
        count = 0
        system = ''
        for rec in read_service_map(opts.nmap, field='notations'):
            print json.dumps(rec)
            count += 1
            system = rec['system']
        if  system: # notations map must have system
            arecord = {'type': 'notations', 'count': count, 'system': system}
            print json.dumps(dict(arecord=arecord))

    if  opts.pmap:
        count = 0
        system = 'presentation'
        for rec in read_service_map(opts.pmap, field='presentation'):
            print json.dumps(rec)
            count += 1
        if  count == 1: # we should have one presentation map
            arecord = {'type': 'presentation', 'count': count, 'system': system}
            print json.dumps(dict(arecord=arecord))

#
# main
#
if __name__ == '__main__':
    main()
