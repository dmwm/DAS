#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Helper script to read sections from DAS config
"""
from __future__ import print_function

import getopt
import sys
from pymongo.uri_parser import parse_uri
from DAS.utils.das_config import das_readconfig


def get_mongo_uri():
    """ read dasconfig and return mongodb host and port (as dict) """
    uri = das_readconfig()['mongodb']['dburi'][0]
    parsed_uri = parse_uri(uri)
    host, port = parsed_uri['nodelist'][0]
    return dict(mongo_host=host, mongo_port=port)

def main():
    """Read mongo options from DAS config"""
    mongo_opts = ['mongo_host', 'mongo_port']
    try:
        optlist, _ = getopt.getopt(sys.argv[1:], '', mongo_opts)
    except getopt.GetoptError as err:
        print(str(err))
        exit(1)
    else:
        opts = [opt_name for opt_name, _ in optlist]
        if '--mongo_host' in opts:
            print(get_mongo_uri()['mongo_host'])
        elif '--mongo_port' in opts:
            print(get_mongo_uri()['mongo_port'])

if __name__ == '__main__':
    main()
