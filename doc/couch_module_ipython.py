#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902
"""
An example of custom module for couch DB ipython admin tool.
"""
__revision__ = "$Id: couch_module_ipython.py,v 1.1 2009/06/18 17:57:10 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import IPython.ipapi
import inspect
from DAS.utils.iprint import PrintManager

### Magic commands
### DEBUG, URI, DB global variables are defined in ipython already
### you can use them or define your own.

def mysystem_system():
    """
    Retrieve results from my_db_name
    """
    host  = URI
    path  = '/my_db_name'
    kwds  = {} # parameters which can be passed to couch, e.g. starket
    data  = httplib_request(host, path, kwds, 'GET', DEBUG)
    if  pretty_print:
        print_data(data)
    else:
        return json.loads(data)

magic_list = [
        ('mysystem_system', mysystem_system),
]

PM = PrintManager()

def mysystem_help(self, arg):
    """
    Provide simple help about available DAS commands.
    """
    global magic_list
    msg  = "\nAvailable DAS commands:\n"
    for name, func in magic_list:
        msg += "%s\n%s\n" % (PM.msg_blue(name), PM.msg_green(func.__doc__))
    print msg

def mysystem_load():
    "Load all magic commads we defined to DAS module"
    ip = IPython.ipapi.get()
    ip.expose_magic('mysystem_help', mysystem_help)
    for m in magic_list:
        ip.ex(inspect.getsource(m[1]))
