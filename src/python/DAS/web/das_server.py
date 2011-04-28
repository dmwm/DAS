#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0702,E1101
"""
DAS server based on CherryPy web framework. We define Root class and
pass it into CherryPy web server.
"""

__revision__ = "$Id: das_server.py,v 1.9 2010/04/07 18:21:35 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import yaml
import logging 
from pprint import pformat
from optparse import OptionParser

# CherryPy modules
from cherrypy import log, tree, engine
from cherrypy import config as cpconfig

# DAS modules
from DAS.utils.das_config import das_readconfig
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_web_srv import DASWebService
from cherrypy.process.plugins import PIDFile
from DAS.services.combined.dbs_phedex import DBSPhedexService

class Root(object):
    """
    DAS web server class.
    """
    def __init__(self, config):
        if  not isinstance(config, dict):
            raise Exception('Wrong config type')
        self.config = config
        self.auth   = None
        self.pid    = None
        
    def configure(self):
        """Configure server, CherryPy and the rest."""
        config = self.config['web_server']
        cpconfig["engine.autoreload_on"] = False

        cpconfig["server.environment"] = config.get("environment", "production")
        cpconfig["server.thread_pool"] = int(config.get("thread_pool", 30))
        cpconfig["server.socket_port"] = int(config.get("port", 8080))
        self.pid = config.get('pid', '/tmp/das_web.pid')

        cpconfig["server.socket_host"] = config.get("host", "0.0.0.0")
        cpconfig["server.socket_queue_size"] = \
                int(config.get("socket_queue_size", 100))
        cpconfig["tools.expires.secs"] = int(config.get("expires", 300))
        cpconfig["log.screen"] = bool(config.get("log_screen", True))
        cpconfig["log.access_file"] = config.get("access_log_file", None)
        cpconfig["log.error_file"] = config.get("error_log_file", None)
        cpconfig['request.show_tracebacks'] = False
        log.error_log.setLevel(config.get("error_log_level", logging.DEBUG))
        log.access_log.setLevel(config.get("access_log_level", logging.DEBUG))

        cpconfig.update ({
                          'tools.expires.on': True,
                          'tools.response_headers.on':True,
                          'tools.etags.on':True,
                          'tools.etags.autotags':True,
                          'tools.encode.on': True,
                          'tools.proxy.on': True,
                          'tools.gzip.on': True,
                          })

        log("loading config: %s" % cpconfig, 
                                   context='web', 
                                   severity=logging.DEBUG, 
                                   traceback=False)

    def start(self, blocking=True):
        """Configure and start the server."""
        self.configure()
        url_base = self.config['web_server']['url_base']
        config = self.config.get('web_server', {})
        config['engine'] = engine
        obj = DASWebService(config)
        tree.mount(obj, url_base) # mount web server

        # DBS/Phedex Service
# ENABLE once DBS3 is ready
#        uri  = self.config['mongodb']['dburi']
#        urls = {}
#        for url in self.config['dbs_phedex']['urls']:
#            if  url.find('phedex') != -1:
#                urls['phedex'] = url
#            else:
#                urls['dbs'] = url
#        obj = DBSPhedexService(uri, urls)
#        tree.mount(obj, '/dbs_phedex')

        print "### DAS web server, PID=%s" % self.pid
        print pformat(tree.apps)
        print pformat(config)
        pid = PIDFile(engine, self.pid)
        pid.subscribe()
        engine.start()
        if  blocking:
            engine.block()
        
def main():
    """
    Start-up web server.
    """
    parser  = OptionParser()
    parser.add_option("-c", "--config", dest="config", default=False,
        help="provide cherrypy configuration file")
    opts, _ = parser.parse_args()

    config  = das_readconfig()
    if  opts.config: # read provided configuration
        fdesc  = open(opts.config, 'r')
        config = yaml.load(fdesc.read())
        fdesc.close()

    # Start DAS server
    root = Root(config)
    root.start()

if __name__ == "__main__":
    main()
