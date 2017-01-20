#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongoimport.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Replacement of mongoimport command which is not require Go language
"""

# system modules
import json
import optparse

# MongoDB modules
from pymongo import MongoClient

class OptionParser():
    """Parser class"""
    def __init__(self):
        "User based option parser"
        usage = "Usage: %prog [options]"
        usage += "Description: "
        self.parser = optparse.OptionParser(usage=usage)
        self.parser.add_option("--host", action="store", type="string",\
            dest="host", default="http://localhost", help="MongoDB host name")
        self.parser.add_option("--port", action="store", type="int",\
            dest="port", default=8230, help="MongoDB port")
        self.parser.add_option("--db", action="store", type="string",\
            dest="dbname", default="", help="MongoDB database name")
        self.parser.add_option("--collection", action="store", type="string",\
            dest="cname", default="", help="MongoDB collection name")
        self.parser.add_option("--file", action="store", type="string",\
            dest="fjs", default="", help="Input JS file")
    def get_opt(self):
        "Return list of options"
        return self.parser.parse_args()

def mongoimport(host, port, dbname, cname, fjs):
    "Mongoimport function to import given JS file into mongodb database"
    if  host.startswith('http://'):
        host = host.replace('http://', '')
    client = MongoClient(host, port)
    col = client[dbname][cname]
    docs = []
    for _, line in enumerate(open(fjs, 'r')):
        line = line.replace('\n', '')
        docs.append(json.loads(line))
    if  pymongo.version.startswith('3.'): # pymongo 3.X
        coll.insert_many(spec)
    else:
        col.insert(docs)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts, _ = optmgr.get_opt()
    mongoimport(opts.host, opts.port, opts.dbname, opts.cname, opts.fjs)

if __name__ == '__main__':
    main()
