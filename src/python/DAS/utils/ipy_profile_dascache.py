#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902

# system modules
import os
import sys
import time
import types
import traceback

# ipython modules
from   IPython import Release
import IPython.ipapi
try:
    from   IPython.Extensions import ipipe
except ImportError:
    pass 
# import DAS modules
from DAS.utils.iprint import PrintManager
from DAS.core.das_couchcache import DASCouchcache

#import __main__ 
import ipy_defaults

# global IP API
ip = IPython.ipapi.get()

class ShellName(object):
  def __init__(self):
      self.prompt   = "das-sh"
      self.name     = 'das_help'
      self.dict     = {}
      self.funcList = []

def unregister():
    _id.prompt         = "das-sh"
    _id.name           = "das-sh"
    _id.dict[_id.name] = []
    _id.funcList       = []

def register(prompt, name, funcList=[]):
    setPrompt(prompt)
    _id.prompt = prompt
    _id.name   = name
    funcList.sort()
    _id.dict[name] = funcList
    if  funcList:
        _pm.print_blue("Available commands within %s sub-shell:" % prompt)
    if  funcList:
        if  not funcList.count('_exit'):
            funcList.append('_exit')
        for func in funcList:
            _pm.print_blue("%s %s" % (" "*10, func))
            if  not _id.funcList.count(func):
                _id.funcList.append(func)
    else:
        _id.funcList = funcList

def setPrompt(in1):
    if  in1=="das-sh":
        unregister()
    if  in1.find('|\#>')!=-1:
        in1 = in1.replace('|\#>', '').strip()
    IP = __main__.__dict__['__IP'] 
    prompt = getattr(IP.outputcache, 'prompt1') 
    prompt.p_template = in1 + " |\#> "
    prompt.set_p_str() 

def getPrompt():
    IP = __main__.__dict__['__IP'] 
    prompt = getattr(IP.outputcache, 'prompt1') 
    return IP.outputcache.prompt1.p_template

def cmsMonitor(self, arg):
    name   = 'monitor'
    prompt = 'monitor'
    if  _id.name != name:
        return register(prompt, name,['_ls'])
    aList = arg.split()
    arg = ' '.join(aList[1:])
    if aList[0] == "_ls":
        return _monitor.run(arg)
    elif aList[0] == "_close" or aList[0]=="_exit":
        setPrompt("das-sh")
    else:
        _pm.print_red("Not implemented yet")
     
def debug(self, arg):
    if  arg:
        _pm.print_blue("Set debug level to %s"%arg)
        _debug.set(arg)
    else:
        _pm.print_blue("Debug level is %s"%_debug.level)

def das_help(self, arg):
    """
    Provide simple help about available commands
    """
    global magic_list
    msg  = "Available commands:\n"
    for name, func in magic_list:
        msg += "%s\n%s\n" % (_pm.msg_blue(name), _pm.msg_green(func.__doc__))
    print msg

def plot(self, arg):
    name   = 'matplot'
    prompt = 'matplot'
    if  _id.name != name:
        return register(prompt, name, ['_plot'])
    aList = arg.split()
    arg = ' '.join(aList[1:])
    if aList[0] == "_plot":
        thread.start_new_thread(_plot.plot,(arg))
    elif aList[0] == "_close" or aList[0]=="_exit":
        setPrompt("das-sh")
    else:
        _pm.print_red("Not implemented yet")

#
# load managers
#
try:
    _pm       = PrintManager()
#    _plot     = PlotManager(_res, _pm)
    _id       = ShellName()
    config    = dict(couch_servers='http://localhost:5984', 
                        logger=None, couch_lifetime=600)
    _couchmgr = DASCouchcache(config)
except:
    traceback.print_exc()

def das_info(self, arg):
    """
    Provide db info
    """
    return _couchmgr.dbinfo()

def das_incache(self, arg):
    """
    Check if result for DAS query exists in couch db.
    Parameters: <das query> 
    """
    return _couchmgr.incache(arg)

def das_queries(self, arg):
    """
    Get queries which present currently in couch DB.
    """
    res = _couchmgr.get_all_queries()
    return set(res)

def das_get(self, arg):
    """
    Get results from couch for provided DAS query
    Parameters: <das query, find dataset where dataset=/a/b/c> 
    """
    try:
        args = eval(arg)
    except:
        args = arg
    if  type(args) is types.DictType:
        return _couchmgr.get_from_cache(**args)
    elif type(args) is types.StringType:
        return _couchmgr.get_from_cache(query=args)

def das_system(self, arg):
    """
    Retrieve results from cache for provided system, e.g. sitedb
    Parameters: <das sub-system, e.g. sitedb> 
    """
    return _couchmgr.list_queries_in(arg)

def das_between(self, arg):
    """
    Retrieve results from provided time stamp range
    Parameters: <min_time, max_time>
    Comments: times are seconds since epoch, max_time is optional. 
    """
    alist = arg.split()
    if  len(alist) == 1:
        alist.append(9999999999)
    if  not alist:
        _pm.print_red("Usage: das_between <min_time> <max_time>")
    else:
        return _couchmgr.list_between(long(alist[0]), long(alist[1]))

def couch_views(self, arg):
    """
    List registeted views in couch db.
    """
    res = _couchmgr.get_all_views()
    for design, definitions in res.items():
        print _pm.msg_blue("design: ") + design
        for row in definitions:
            for name, defdict in row.items():
                print _pm.msg_blue("view name: ") + name
                for key, val in defdict.items():
                    _pm.print_blue(key + ":")
                    _pm.print_green(val)

def create_view(self, arg):
    """
    Create couch db view
    Parameters: <db> <design> <view_dict>
    """
    alist = arg.split()
    if  len(alist) != 3:
        _pm.print_red("Usage: create_view <db> <design> <view_dict>")
    else:
        return _couchmgr.create_view(alist[0], alist[1], alist[2])

def delete_view(self, arg):
    """
    Delete couch db view
    Parameters: <db> <design> <view_name>
    """
    alist = arg.split()
    if  len(alist) != 3:
        _pm.print_red("Usage: delete_view <db> <design> <view_name>")
    else:
        return _couchmgr.delete_view(alist[0], alist[1], alist[2])

def delete_db(self, arg):
    """
    Delete DB in couch. By default DAS database called das
    Parameters: <db_name, e.g. das>
    """
    _couchmgr.delete_cache(arg)

def delete_system(self, arg):
    """
    Delete docs for given DAS sub-system
    Parameters: <db> <das sub-system, e.g. sitedb>
    """
    alist = arg.split()
    if  len(alist) != 2:
        _pm.print_red("Usage: delete_system <db> <system, e.g. sitedb>")
    else:
        dbname, system = alist
        _couchmgr.delete_cache(dbname, system)

magic_list = [
        ('das_help', das_help), 
        ('das_info', das_info),
        ('das_incache', das_incache),
        ('das_get', das_get),
        ('das_system', das_system),
        ('das_queries', das_queries),
        ('das_between', das_between),
        ('couch_views', couch_views),
        ('create_view', create_view),
        ('delete_view', delete_view),
        ('delete_db', delete_db),
]
#
# matplotlib
#
try:
    import pylab
    magic_list += [('plot', plot)]
except ImportError:
    pass
    

#
# Main function
#
def main():
    o = ip.options
    # load cms modules and expose them to the shell
    for m in magic_list:
        ip.expose_magic(m[0], m[1])

    # autocall to "full" mode (smart mode is default, I like full mode)
    o.autocall = 2
    
    # Jason Orendorff's path class is handy to have in user namespace
    # if you are doing shell-like stuff
    try:
        ip.ex("from path import path" )
    except ImportError:
        pass
    
    ip.ex('import os; import types; import time')
        
    # import ipipe support
    try:
        ip.ex("from ipipe import *")
    except ImportError:
        pass

    # import local logger
#    ip.ex("import cmssh_logger")

    # Set dbsh prompt
    o.prompt_in1= 'das-sh |\#> '
    o.prompt_in2= 'das-sh> '
    o.system_verbose = 0
    
    # define dbsh banner
#    ver    = "%s.%s" % (cmssh.__version__, cmssh.__revision__)
    ver    = "1.1"
    pyver  = sys.version.split('\n')[0]
    ipyver = Release.version
    msg    = "Welcome to das-sh \n[python %s, ipython %s]\n%s\n" \
            % (pyver, ipyver ,os.uname()[3])
    msg   += "For das-sh help use "
    msg   += _pm.msg_blue("das_help")
    msg   += ", for python help use help commands\n"
    o.banner = msg
    o.prompts_pad_left="1"
    # Remove all blank lines in between prompts, like a normal shell.
    o.separate_in="0"
    o.separate_out="0"
    o.separate_out2="0"
    
main()
