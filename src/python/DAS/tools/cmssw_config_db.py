#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103,E1101

"""
Set of tools to create CMSSW configuration DB using MongoDB back-end.
"""

# system modules
import os
import sys
import time
import fnmatch
import hashlib

from optparse import OptionParser
#from operator import itemgetter
#from heapq import nlargest

# pymongo modules
from pymongo.connection import Connection
#from pymongo import DESCENDING
#from pymongo.errors import InvalidStringData

from DAS.services.cmsswconfigs.base import CMSSWConfig

# mongo additions
from DAS.utils import mongosearch
import mongoengine

class RelOptionParser: 
    """
    Option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                    type="int", default=0, dest="verbose",
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
             default=os.environ.get('VO_CMS_SW_DIR',None), dest="path",
             help="specify path to CMS software area")
        self.parser.add_option("--delete", action="store_true",  
             default=False, dest="delete",
             help="delete entry about release in MongoDB")
        self.parser.add_option("--check", action="store_true",
             default=False, dest="check", 
             help="check if release exists in DB")
        self.parser.add_option("--list", action="store_true",
             default=False, dest="list",
             help="list releases in DB.")
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
    keyhash = hashlib.md5()
    keyhash.update(query)
    return keyhash.hexdigest()

def gen_find(filepat, top):
    """
    Find files which belong to provided pattern starting from top dir.
    Equvalent of find UNIX command.
    """
    for path, _, filelist in os.walk(top, followlinks=True):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)

def connect(host, port):
    """
    Connect to MongoDB database.
    """
    connection = Connection(host, port)
    db = connection.configdb
    return db

def delete(host, port, release):
    """
    Delete given relase from configdb
    """
    db = connect(host, port)
    db.drop_collection(release)
    db.drop_collection('%sindex' % release)

def check(host, port, release):
    """
    Check if given release exists in MongoDB
    """
    db = connect(host, port)
    for coll in db.collection_names():
        if  coll == release:
            return True
    return False

def releases(host, port):
    db = connect(host, port)
    names = db.collection_names() 
    return filter(lambda x: x.startswith('CMSSW') and not x.endswith('index'),
                  names)

#def inject(host, port, path, release, debug=0):
def inject(path, release, debug=0):
    """
    Function to inject CMSSW configuration files into MongoDB located
    at provided host/port.
    """
    
    content_translate = '?????????\t\n??\r??'+\
                        ''.join(['?']*16+\
                                [chr(i) for i in range(32,128)]+\
                                ['?']*128)
    
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

    # Use Mongosearch/index
    mongoengine.connect('configdb')
    config_obj = CMSSWConfig
    config_obj._meta['collection'] = release
    # Create an index for the blog post and add the fields to be indexed
    index = mongosearch.SearchIndex(config_obj, use_term_index=False)
    index.add_field('content', html=False)

    print "Searching %s for configurations." % (cdir)
    time0 = time.time()
    filecount = 0

    for name in gen_find("*.py", "./"):
        if  name.find('__init__.py') != -1:
            continue
        if  debug:
            print "%s/%s" % (cdir, name)
        try:
            _, system, subsystem, config = name.split('/')
        except:
            continue
        
        filecount += 1
        
        fdsc = open(name, 'r')
        content = fdsc.read()
        fdsc.close()
        
        #content = content.encode("utf_8") #ensure the encoding is utf8
        content = content.translate(content_translate)

        content = content.replace('\0', '')
        obj = CMSSWConfig(name=config, content=content, 
                system=system, subsystem=subsystem, hash=genkey(content))
        obj.save()

    # Index the collection
    print 'Search and insert %s files took %s seconds' % (filecount, 
                                                          time.time() - time0)
    print "Generating index ..."
    time0 = time.time()
    index.generate_index()
    print 'Indexing took %s seconds' % (time.time() - time0)

#        record  = dict(system=system, subsystem=subsystem, 
#                        config=config, content=content, hash=genkey(content))
#        try:
#            res = collection.insert(record)
#            print "res record", res
#        except InvalidStringData:
#            content = content.replace('\0', '')
#            record  = dict(system=system, subsystem=subsystem, 
#                        config=config, content=content, hash=genkey(content))
#            collection.insert(record)
#        except:
#            print "Fail to insert the following record:\n"
#            print "system", system
#            print "subsystem", subsystem
#            print "config", config
#            print "hash", hash
#            print "content", content
#            raise
#    lkeys = ['system', 'subsystem', 'config', 'hash']
#    index_list = [(key, DESCENDING) for key in lkeys]
#    collection.ensure_index(index_list)

#
# MAIN
#
if __name__ == '__main__':

    if sys.version_info < (2, 6):
        raise Exception("This script requires python 2.6 or greater")

    optManager  = RelOptionParser()
    (opts, args) = optManager.getopt()
    if opts.list:
        print "Listing releases in DB."
        for release in releases(opts.host, opts.port):
            print " - %s" % release
    elif opts.delete and opts.release:
        print "Attempting to delete %s" % opts.release
        if check(opts.host, opts.port, opts.release):
            delete(opts.host, opts.port, opts.release)
            print "Done."
        else:
            print "Release does not exist, exiting."
            sys.exit(1)
    elif opts.check and opts.release:
        print "Checking if release %s exists in DB" % opts.release
        if check(opts.host, opts.port, opts.release):
            print "Release exists."
        else:
            print "Release does not exist."
            sys.exit(1)
    elif opts.release and opts.path:
        print "Attempting to insert %s from %s" % (opts.release, opts.path)
        if check(opts.host, opts.port, opts.release):
            print "Release already exists, delete before attempting to reinsert."
            sys.exit(1)
        else:
            inject(opts.path, opts.release, opts.verbose)
            print "Done."
    else:
        print "Please specify either:\n\t--path and --release\n\t--release and --delete\n\t--release and --check\n\t--list."
        sys.exit(1)

