#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Set of tools to create CMSSW configuration DB using MongoDB back-end.
"""

# python modules
import os
import sys
import fnmatch
from optparse import OptionParser
try:
    # with python 2.5
    import hashlib
except:
    # prior python 2.5
    import md5

# pymongo modules
from pymongo.connection import Connection
from pymongo import DESCENDING
from pymongo.errors import InvalidStringData

class RelOptionParser: 
    """
    Option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                    type="int", default=None, dest="verbose",
             help="verbose output")
        self.parser.add_option("--release", action="store", type="string", 
                                          default=False, dest="release",
             help="specify CMSSW release name")
        self.parser.add_option("--host", action="store", type="string", 
             default="localhost", dest="host",
             help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="string", 
             default=27017, dest="port",
             help="specify MongoDB port number")
        self.parser.add_option("--path", action="store", type="string", 
             default="", dest="path",
             help="specify path to CMS software area")
    def getopt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()


def genkey(query):
    """
    Generate a new key-hash for a given query. We use md5 hash for the
    query and key is just hex representation of this hash.
    """
    try:
        keyhash = hashlib.md5()
    except:
        # prior python 2.5
        keyhash = md5.new()

    keyhash.update(query)
    return keyhash.hexdigest()

def gen_find(filepat, top):
    """
    Find files which belong to provided pattern starting from top dir.
    Equvalent of find UNIX command.
    """
    for path, dirlist, filelist in os.walk(top, followlinks=True):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)

def connect(host, port):
    """
    Connect to MongoDB database.
    """
    connection = Connection(host, port)
    db = connection.configdb
    return db

def inject(host, port, path, release, debug=0):
    """
    Function to inject CMSSW configuration files into MongoDB located
    at provided host/port.
    """
    db = connect(host, port)
    collection = db[release]
    if  not os.environ.has_key('SCRAM_ARCH'):
        msg = 'SCRAM_ARCH environment is not set'
        raise Exception(msg)

    cdir = os.path.join(path, os.environ['SCRAM_ARCH'])
    if  not os.path.isdir(cdir):
        msg = "Path %s not found" % cdir
        raise Exception(msg)
    cdir = os.path.join(cdir, 'cms')
    cdir = os.path.join(cdir, 'cmssw')
    cdir = os.path.join(cdir, release)
    cdir = os.path.join(cdir, 'python')
    os.chdir(cdir)

    for name in gen_find("*.py", "./"):
        if  name.find('__init__.py') != -1:
            continue
        if  debug:
            print "%s/%s" % (cdir, name)
        try:
            dot, system, subsystem, config = name.split('/')
        except:
            continue
        fdsc = open(name, 'r')
        content = fdsc.read()
        fdsc.close()
        record  = dict(system=system, subsystem=subsystem, 
                        config=config, content=content, hash=genkey(content))
        try:
            collection.insert(record)
        except InvalidStringData:
            content = content.replace('\0', '')
            record  = dict(system=system, subsystem=subsystem, 
                        config=config, content=content, hash=genkey(content))
            collection.insert(record)
        except:
            print "Fail to insert the following record:\n"
            print "system", system
            print "subsystem", subsystem
            print "config", config
            print "hash", hash
            print "content", content
            raise
        lkeys = ['system', 'subsystem', 'config', 'hash']
        index_list = [(key, DESCENDING) for key in lkeys]
        collection.ensure_index(index_list)
#
# MAIN
#
if __name__ == '__main__':

    if sys.version_info < (2, 6):
        raise Exception("This script requires python 2.6 or greater")

    optManager  = RelOptionParser()
    (opts, args) = optManager.getopt()
    if  not opts.release or not opts.path:
        msg = "Please provide: --release=<CMSSW_X_Y_Z> --path=/afs/cern.ch/cms/sw"
        print msg
        sys.exit(1)
    inject(opts.host, opts.port, opts.path, opts.release, opts.verbose)
