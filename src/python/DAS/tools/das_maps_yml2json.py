#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
DAS map reader which transform DAS YML files into JSON docs and
dump them on stdout
"""
__author__ = "Valentin Kuznetsov"

import json
from optparse import OptionParser
from DAS.services.map_reader import read_service_map
from DAS.core.das_mapping_db import verification_token

class DASOptionParser(object):
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("--uri-map", action="store", type="string",\
             default=None , dest="umap", help="specify uri map file")
        self.parser.add_option("--notation-map", action="store", type="string",\
             default=None , dest="nmap", help="specify notation map file")
        self.parser.add_option("--presentation-map", action="store",\
             type="string",\
             default=None , dest="pmap", help="specify presentation map file")
        self.parser.add_option("--get-verification-token-for", action="store",\
             type="string", default=None , dest="get_verification_token_for",\
             help="generate a verification token for given dasmaps file")
    def get_opt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def nooutput(results):
    """Just iterate over generator, but don't print it out"""
    for _ in results:
        pass

def main():
    """Main function"""
    opt_mgr = DASOptionParser()
    opts, _ = opt_mgr.get_opt()

    if  opts.umap:
        count = 0
        system = ''
        for rec in read_service_map(opts.umap, field='uri'):
            print json.dumps(rec)
            count += 1
            system = rec['system']
        if  system: # notations map must have system
            arecord = {'type': 'service', 'count': count, 'system': system}
            print json.dumps(dict(arecord=arecord))
        # output record(s) containing uris for listing allowed input_values
        for rec in read_service_map(opts.umap, field='input_values'):
            print json.dumps(rec)

    if  opts.nmap:
        count = 0
        system = ''
        for rec in read_service_map(opts.nmap, field='notations'):
            print json.dumps(rec)
            count += 1
            system = rec['system']
        if  system: # notations map must have system
            arecord = {'type': 'notation', 'count': count, 'system': system}
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

    # generate a verification token composed of all record hashes
    if opts.get_verification_token_for:
        with open(opts.get_verification_token_for) as file_:
            token = verification_token(json.loads(line)
                                       for line in file_ if line)
            print json.dumps({'verification_token': token,
                              'type': 'verification_token'})


#
# main
#
if __name__ == '__main__':
    main()
