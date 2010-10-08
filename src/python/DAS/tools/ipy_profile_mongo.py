#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902

"""
Shell mode for MongoDB based on IPython and pymongo
Start ipython in shell mode by invoking "ipython -p mongo"
"""

__revision__   = "$Id: ipy_profile_mongo.py,v 1.4 2009/06/24 14:05:14 valya Exp $"
__version__    = "$Revision: 1.4 $"
__author__     = "Valentin Kuznetsov"
__license__    = "GPL"
__version__    = "1.0.1"
__maintainer__ = "Valentin Kuznetsov"
__email__      = "vkuznet@gmail.com"
__status__     = "Alpha"

# python modules
import os
import sys
import textwrap
import inspect
import traceback

# ipython modules
from   IPython import Release
import IPython.ipapi

# pymongo modules
from pymongo.connection import Connection

class PrintManager:
    def __init__(self):
        from IPython import ColorANSI
        self.term = ColorANSI.TermColors

    def red(self, msg):
        """yield message using red color"""
        if  not msg:
            msg = ''
        return self.term.Red + msg + self.term.Black

    def purple(self, msg):
        """yield message using purple color"""
        if  not msg:
            msg = ''
        return self.term.Purple + msg + self.term.Black

    def cyan(self, msg):
        """yield message using cyan color"""
        if  not msg:
            msg = ''
        return self.term.LightCyan + msg + self.term.Black

    def green(self, msg):
        """yield message using green color"""
        if  not msg:
            msg = ''
        return self.term.Green + msg + self.term.Black

    def blue(self, msg):
        """yield message using blue color"""
        if  not msg:
            msg = ''
        return self.term.Blue + msg + self.term.Black

# Globals, to be use in magic functions
PM = PrintManager()
IP = IPython.ipapi.get()

def set_prompt(in1):
    """Define shell prompt"""
    ip = IP.user_ns['__IP']
    prompt = getattr(ip.outputcache, 'prompt1') 
    prompt.p_template = '\C_LightBlue[\C_LightCyan%s\C_LightBlue]|\#> ' % in1
    prompt.set_p_str() 

def das_clean():
    # clean analytics.db
    dbname = 'analytics'
    col = 'db'
    IP.user_ns['db'] = IP.user_ns['connection'][dbname]
    IP.user_ns['collection'] = IP.user_ns['connection'][dbname][col]
    collection.remove({})
    collection.drop_indexes()
    # clean das.merge
    dbname = 'das'
    col = 'merge'
    IP.user_ns['db'] = IP.user_ns['connection'][dbname]
    IP.user_ns['collection'] = IP.user_ns['connection'][dbname][col]
    collection.remove({})
    collection.drop_indexes()
    # clean das.cache
    dbname = 'das'
    col = 'cache'
    IP.user_ns['db'] = IP.user_ns['connection'][dbname]
    IP.user_ns['collection'] = IP.user_ns['connection'][dbname][col]
    collection.remove({})
    collection.drop_indexes()
    set_prompt('das.cache')

def clean(dict={}):
    """
    Clean collection with provided spec.
    """
    if  not collection:
        msg  = '\nCollection name is not set, to set it up please invoke: '
        msg += PM.blue('use <db_name.collection_name>')
        print PM.red(msg)
        return
    collection.remove(dict)

def print_res(dict={}, fields=None):
    """
    Dump documents found in MongoDB for provided set of conditions.
    Accept input dict of conditions and list of output fields.
    """
    if  not collection:
        msg  = '\nCollection name is not set, to set it up please invoke: '
        msg += PM.blue('use <db_name.collection_name>')
        print PM.red(msg)
        return
    try:
        if  fields:
            res = collection.find(dict, fields=fields)
        else:
            res = collection.find(dict)
        for row in res:
            val = row['_id']
            row['_id'] = str(val)
            sobj = JSONEncoder().encode(row)
            print sobj
            print
    #        print_dict(row)
    #        print
    except:
        msg  = "\nYou didn't choose which database and collection to use"
        msg += "\nPlease use %s command" % PM.blue('use')
        print msg

def print_dict(idict):
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
        if  key != idict.keys()[-1]:
            sys.stdout.write(", ")
        else:
            sys.stdout.write("")
    sys.stdout.write(PM.red("}"))

# magic functions
def use(self, arg):
    """
    Set current database to provide db_name.collention_name
    """
    if  arg.find('.') == -1:
        IP.user_ns['db'] = IP.user_ns['connection'][arg]
        IP.user_ns['collection'] = None
        set_prompt('%s' % arg)
        return
    dbname, col = arg.split('.')
    # IP, PM are defined at load time
    IP.user_ns['db'] = IP.user_ns['connection'][dbname]
    IP.user_ns['collection'] = IP.user_ns['connection'][dbname][col]
    set_prompt('%s.%s' % (dbname, col))

def connect(self, arg):
    """
    Connect to MognoDB, provide host:port.
    """
    if  arg.find(':') == -1:
        msg = '\nProvided argument %s is not in host:port form' % arg
        raise Exception(PM.red(msg))
    host, port = arg.split(':')
    try:
        connection = Connection(host, port)
        IP.user_ns['connection'] = connection
        print db_content()
    except:
        print "Fail to connect to %s:%s" % (host, port)
        pass

def show(self, arg):
    """
    Show database info, including connection, db names and 
    list of collections.
    """
    print db_content()

def db_content():
    msg = ""
    try:
        connection = IP.user_ns['connection']
        msg = "\nConnection: %s" % connection
        for dbname in connection.database_names():
            db_msg = "\nDatabase: %s" % PM.blue(dbname)
            db = connection[dbname]
            cols = db.collection_names()
            if  cols:
                msg += '%s, collections: %s' \
                % (db_msg, PM.green(', '.join(db.collection_names())))
            else:
                msg += db_msg
    except:
        traceback.print_exc()
        pass
    return msg + '\n'

def mongohelp(self, arg):
    """
    Help for ipy_profile_mongo.
    """
    magic_list = [('use', use), ('show', show), ('connect', connect)]
    msg  = "\nAvailable commands:\n"
    for name, func in magic_list:
        msg += "%s\n%s\n" % (PM.blue(name), PM.green(func.__doc__))
    msg += "List of pre-defined variables to control your interactions "
    msg += "with MongoDB:\n"
    msg += PM.green("    connection, db, collection\n")
    print msg

def main():
    # global IP API
    ip = IP

    o = ip.options
    # autocall to "full" mode (smart mode is default, I like full mode)
    o.autocall = 2
    
    # Get pysh-like prompt for all profiles. 
    o.prompt_in1= '\C_LightBlue[\C_LightCyanMongoDB\C_LightBlue]\C_Green|\#> '
    o.prompt_in2= '\C_Green|\C_LightGreen\D\C_Green> '
    o.prompt_out= '<\#> '
    
    # Jason Orendorff's path class is handy to have in user namespace
    # if you are doing shell-like stuff
    try:
        ip.ex("from path import path" )
    except ImportError:
        pass
    
    ip.ex('import os')
    ip.ex("def up(): os.chdir('..')")

    # load stuff 
    ip.ex(inspect.getsource(PrintManager))
    ip.ex(inspect.getsource(db_content))
    python_text = """
try:
    import json # Python 2.6
    from json import JSONEncoder
except:
    import simplejson as json # prior 2.6 require simplejson
    from simplejson import JSONEncoder
from pymongo.connection import Connection
from pymongo.objectid import ObjectId
import sys
import traceback
from   IPython import Release
import IPython.ipapi
PM = PrintManager()
IP = IPython.ipapi.get()
db = None
collection = None
try:
    connection = Connection("localhost", 27017)
except:
    pass
    print "No MongoDB found on localhost:27017"
    print "To establish connection please use: " + PM.blue("connect")
"""
    ip.ex(python_text)
    ip.ex(inspect.getsource(print_res))
    ip.ex(inspect.getsource(print_dict))
    ip.ex(inspect.getsource(clean))
    ip.ex(inspect.getsource(set_prompt))
    ip.ex(inspect.getsource(das_clean))
        
    # define magic functions
    ip.expose_magic('mongohelp', mongohelp)
    ip.expose_magic('use', use)
    ip.expose_magic('show', show)
    ip.expose_magic('connect', connect)

    # I like my banner minimal.
    pyver  = sys.version.split('\n')[0]
    ipyver = Release.version
    msg    = "\nWelcome to pymongo shell \n[python %s, ipython %s]\n%s\n" \
            % (pyver, ipyver, os.uname()[3])
    msg   += "For help with pymongo use " + PM.blue("mongohelp")
    msg   += ", for python help use " + PM.blue("help") + " command\n"
    o.banner = msg + '\n' + db_content()
#    o.banner = "Py %s IPy %s\n" % (sys.version.split('\n')[0],Release.version)

    # make 'd' an alias for ls -F
    ip.magic('alias d ls -F --color=auto')
    
    # Make available all system commands through "rehashing" immediately. 
    # You can comment these lines out to speed up startup on very slow 
    # machines, and to conserve a bit of memory. Note that pysh profile does this
    # automatically
    ip.IP.default_option('cd','-q')
    
    # Remove all blank lines in between prompts, like a normal shell.
    o.prompts_pad_left="1"
    o.separate_in="0"
    o.separate_out="0"
    o.separate_out2="0"
    
    # now alias all syscommands
    syscmds = ip.db.get("syscmdlist",[] )
    if not syscmds:
        print textwrap.dedent("""
        System command list not initialized, probably the first run...
        running %rehashx to refresh the command list. Run %rehashx
        again to refresh command list (after installing new software etc.)
        """)
        ip.magic('rehashx')
        syscmds = ip.db.get("syscmdlist")
    for cmd in syscmds:
        #print "al",cmd
        noext, ext = os.path.splitext(cmd)
        ip.IP.alias_table[noext] = (0,cmd)

main()
