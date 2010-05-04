#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0702,E1101
"""
DAS server based on CherryPy web framework. We define Root class and
pass it into CherryPy web server.
"""

__revision__ = "$Id: das_server.py,v 1.8 2010/04/05 20:15:24 valya Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import yaml
import logging 
from pprint import pformat
from optparse import OptionParser

# CherryPy modules
from cherrypy import log, tree, engine, tools
from cherrypy import config as cpconfig

# DAS modules
from DAS.utils.das_config import das_readconfig
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_web import DASWebService
from DAS.web.das_cache import DASCacheService
from DAS.web.das_doc import DASDocService
from DAS.web.das_admin import DASAdminService

class Root(object):
    """
    DAS web server class.
    """
    def __init__(self, model, config):
        self.model  = model
        self.config = config
        self.auth   = None
        
    def configure(self):
        """Configure server, CherryPy and the rest."""
        config = self.config[self.model]
        cpconfig["server.environment"] = config.get("environment", "production")
        cpconfig["server.thread_pool"] = int(config.get("thread_pool", 30))
        cpconfig["server.socket_port"] = int(config.get("port", 8080))
        cpconfig["server.socket_host"] = config.get("host", "0.0.0.0")
        cpconfig["server.socket_queue_size"] = \
                int(config.get("socket_queue_size", 15))
        cpconfig["tools.expires.secs"] = int(config.get("expires", 300))
        cpconfig["log.screen"] = bool(config.get("log_screen", True))
        cpconfig["log.access_file"] = config.get("access_log_file", None)
        cpconfig["log.error_file"] = config.get("error_log_file", None)
        #cpconfig.update ({'request.show_tracebacks': False})
        #cpconfig.update ({'request.error_response': self.handle_error})
        #cpconfig.update ({'tools.proxy.on': True})
        #cpconfig.update ({'proxy.tool.base': '%s:%s' 
#                                % (socket.gethostname(), opts.port)})
        log.error_log.setLevel(config.get("error_log_level", logging.DEBUG))
        log.access_log.setLevel(config.get("access_log_level", logging.DEBUG))

        cpconfig.update ({
                          'tools.expires.on': True,
                          'tools.response_headers.on':True,
                          'tools.etags.on':True,
                          'tools.etags.autotags':True,
                          'tools.encode.on': True,
                          'tools.gzip.on': True
                          })

        # Security module
#        if  self.config.has_key('security'):
#            from oidservice.oid_consumer import OidConsumer
#            cpconfig.update({'tools.sessions.on': True,
#                             'tools.sessions.name': 'oidconsumer_sid'})
#            secconfig = self.config['security']
#            tools.oid = OidConsumer(secconfig)
#            cpconfig.update({'tools.oid.on': True})
#            cpconfig.update({'tools.oid.role': secconfig.get('role', None)})
#            cpconfig.update({'tools.oid.group': secconfig.get('group', None)})
#            cpconfig.update({'tools.oid.site': secconfig.get('site', None)})
#            tree.mount(tools.oid.defhandler, 
#                secconfig.get('mount_point', '/das/auth'))
#            self.auth = tools.oid.defhandler

        log("loading config: %s" % cpconfig, 
                                   context=self.model, 
                                   severity=logging.DEBUG, 
                                   traceback=False)

    def start(self, blocking=True):
        """Configure and start the server."""
        self.configure()
        if  self.model == 'cache_server':
            config = self.config.get('cache_server', {})
            obj = DASCacheService(config) # mount cache server
            tree.mount(obj, '/')
        elif self.model == 'web_server':
            config = self.config.get('web_server', {})
            obj = DASWebService(config)
            tree.mount(obj, '/das') # mount web server
            sdir = os.environ['DAS_ROOT'] + '/doc/build/html'
            static_dict = { 
                            'tools.staticdir.on':True,
                            'tools.staticdir.dir':sdir,
            }
            conf = {'/': {'tools.staticdir.root':sdir},
                    '_static' : static_dict,
            }
            cpconfig.update(conf)
            obj = DASDocService(sdir)
            tree.mount(obj, '/das/doc') # mount doc server
            config = self.config.get('admin_server', {})
            obj = DASAdminService(config)
            tree.mount(obj, '/das/admin') # mount admint server
        else:
            obj = DASWebManager({}) # pass empty config dict
            tree.mount(obj, '/')
        engine.start()
        if  blocking:
            engine.block()
            
#    def stop(self):
#        """Stop the server."""
#        engine.exit()
#        engine.stop()
        
def main():
    """
    Start-up web server.
    """
    parser  = OptionParser()
    parser.add_option("-c", "--config", dest="config", default=False,
        help="provide cherrypy configuration file")
    parser.add_option("-s", "--server", dest="server", default=None,
        help="specify DAS server, e.g. web or cache")
    opts, _ = parser.parse_args()

    config  = das_readconfig()
    if  opts.config: # read provided configuration
        fdesc  = open(opts.config, 'r')
        config = yaml.load(fdesc.read())
        fdesc.close()

    # Choose which DAS server to start
    if  opts.server == 'cache':
        model = "cache_server"
    elif opts.server == 'web':
        model = "web_server"
    else:
        print "Please specify which DAS server you want to start, see --help"
        sys.exit(1)

    # Start DAS server
    print "\n### Start %s server with DAS configuration:" % model
    print pformat(config)
    root = Root(model, config)
    root.start()

if __name__ == "__main__":
    main()
