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
    cmdList = cmdDict.keys()
    cmdList.sort()
    sep = 0
    for i in cmdList:
        if  sep < len(str(i)): 
            sep = len(str(i))
    if  not arg:
        msg  = "Available commands:\n"
        for cmd in cmdList:
            msg += "%s%s %s\n" % (_pm.msg_green(cmd), " "*abs(sep-len(cmd)), cmdDict[cmd])
    else:
        cmd = arg.strip()
        if cmdDictExt.has_key(cmd):
           msg = "\n%s: %s\n"%(_pm.msg_green(cmd),cmdDictExt[cmd])
        elif cmdDict.has_key(cmd):
           msg = "\n%s: %s\n"%(_pm.msg_green(cmd),cmdDict[cmd])
        else:
           msg = _pm.msg_red("\nSuch command is not available\n")
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
    return _couchmgr.dbinfo()

def das_incache(self, arg):
    return _couchmgr.incache(arg)

def das_get(self, arg):
    try:
        args = eval(arg)
    except:
        args = arg
    if  type(args) is types.DictType:
        return _couchmgr.get_from_cache(**args)
    elif type(args) is types.StringType:
        return _couchmgr.get_from_cache(query=args)

def das_system(self, arg):
    return _couchmgr.list_queries_in(arg)

def das_between(self, arg):
    alist = arg.split()
    if  len(alist) == 1:
        alist.append(9999999999)
    return _couchmgr.list_between(long(alist[0]), long(alist[1]))

magic_list = [
        ('das_help', das_help), 
        ('das_info', das_info),
        ('das_incache', das_incache),
        ('das_get', das_get),
        ('das_system', das_system),
        ('das_between', das_between),
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
