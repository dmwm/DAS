#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0702,E1101
"""
DAS server based on CherryPy web framework. We define Root class and
pass it into CherryPy web server.
"""
from __future__ import print_function

__author__ = "Valentin Kuznetsov"

import sys

# python 3
if  sys.version.startswith('3.'):
    unicode = str
    basestring = str

# system modules
import logging
import threading
from pprint import pformat
from optparse import OptionParser

# CherryPy modules
from cherrypy import log, tree, engine
from cherrypy import config as cpconfig
from cherrypy.wsgiserver import CherryPyWSGIServer
from cherrypy.process.servers import ServerAdapter
from cherrypy.process.plugins import PIDFile
from cherrypy import _cptree
import cherrypy

# DAS modules
from DAS.utils.das_config import das_readconfig

# WMCore modules
from WMCore.WebTools.FrontEndAuth import FrontEndAuth
from WMCore.Configuration import Configuration, ConfigSection

class NullAuth(cherrypy.Tool):
    """
    Class similar to one from WMCore.WebTools.FrontEndAuth module, but withou
    extra logging information
    """
    def __init__(self, config):
        # Defines the hook point for cherrypy
        self._name = None
        self._point = 'before_request_body'
        self._priority = 60 # Just after the sessions being enabled

    def callable(self, role=[], group=[], site=[], authzfunc=None):
        if authzfunc:
            cherrypy.log.access_log.warning('\tusing authorisation function %s' % authzfunc.__name__)
        cherrypy.request.user = {'dn': 'None',
                                'method': 'Null Auth - totally insecure!',
                                'login': 'test',
                                'name': 'Test User',
                                'roles': {} }

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
        cpconfig["engine.timeout_monitor.on"] = config.get("timeout_monitor", False)

        cpconfig["server.environment"] = config.get("environment", "production")
        cpconfig["server.thread_pool"] = int(config.get("thread_pool", 30))
        cpconfig["server.socket_port"] = int(config.get("port", 8080))
        self.pid = config.get('pid', '/tmp/das_web.pid')

        cpconfig["server.socket_host"] = config.get("host", "0.0.0.0")
        cpconfig["server.socket_queue_size"] = \
                int(config.get("socket_queue_size", 100))
        cpconfig["tools.expires.secs"] = int(config.get("expires", 300))
        cpconfig["tools.sessions.timeout"] = int(config.get("session_timeout", 60))

        cpconfig["log.screen"] = bool(config.get("log_screen", True))
        cpconfig["log.access_file"] = config.get("access_log_file", None)
        cpconfig["log.error_file"] = config.get("error_log_file", None)
        cpconfig['request.show_tracebacks'] = False
        log.error_log.setLevel(config.get("error_log_level", logging.DEBUG))
        log.access_log.setLevel(config.get("access_log_level", logging.DEBUG))

        # SecurityModule config
        # Registers secmodv2 into cherrypy.tools so it can be used through
        # decorators
        class SecConfig(object):
            pass
        security = self.config['security']
        secconfig = SecConfig()
        secsection = ConfigSection('security')
        for key, val in security.items():
            setattr(secconfig, key, val)
        if  security.get('module', '') == '':
            print("### DAS behind NullAuth, should NOT be used in production")
            cherrypy.tools.secmodv2 = NullAuth(secconfig)
        else:
            print("### DAS behind FrontEndAuth")
            cherrypy.tools.secmodv2 = FrontEndAuth(secconfig)
            cherrypy.config.update({'tools.secmodv2.on': True,
                        'tools.secmodv2.role': security.get('role', ''),
                        'tools.secmodv2.group': security.get('group' ''),
                        'tools.secmodv2.site': security.get('site', '')})

        cpconfig.update ({\
                          'tools.expires.on': True,\
                          'tools.response_headers.on':True,\
                          'tools.etags.on':True,\
                          'tools.etags.autotags':True,\
                          'tools.encode.on': True,\
                          'tools.proxy.on': True,\
                          'tools.gzip.on': True,\
                          })

        log("loading config: %s" % cpconfig,\
                                   context='web',\
                                   severity=logging.DEBUG,\
                                   traceback=False)

    def setup_kws_server(self):
        """
        sets up the KWS server to run on a separate port
        """
        # based on http://docs.cherrypy.org/stable/refman/process/servers.html

        if not self.config['keyword_search']['kws_service_on']:
            return

        # load KWSWebService here since it will be loaded after self.configure
        # which brings secmodv2 into cherrypy.tools
        from DAS.web.kws_web_srv import KWSWebService
        kws_service = KWSWebService(self.config)
        kws_wsgi_app = _cptree.Tree()
        kws_wsgi_app.mount(kws_service, '/das')

        config = self.config['web_server']
        port = int(config.get("kws_port", 8214))
        host = config.get("kws_host", '0.0.0.0')

        kws_server = CherryPyWSGIServer(
            bind_addr=(host, port),
            wsgi_app=kws_wsgi_app,
            numthreads=int(config.get("thread_pool_kws", 10)),
            request_queue_size=cpconfig["server.socket_queue_size"],
            # below are cherrypy default settings...
            # max=-1,
            # timeout=10,
            # shutdown_timeout=5
        )
        srv_adapter = ServerAdapter(engine, kws_server)
        srv_adapter.subscribe()

    def start(self, blocking=True):
        """Configure and start the server."""
        self.configure()
        url_base = self.config['web_server']['url_base']
        config = self.config.get('web_server', {})
        config['engine'] = engine
        # import DASWebServices after we run self.configure which register secmodv2
        from DAS.web.das_web_srv import DASWebService
        obj = DASWebService(self.config)
        tree.mount(obj, url_base) # mount web server

        self.setup_kws_server()

        print("### DAS web server, PID=%s" % self.pid)
        print(pformat(tree.apps))
        print(pformat(self.config))
        pid = PIDFile(engine, self.pid)
        pid.subscribe()
        engine.start()
        print("### DAS server runs with %s threads" \
                % threading.active_count())
        threads = threading.enumerate()
        try:
            threads.sort()
        except:
            pass
        for thr in threads:
            print(thr)
        if  blocking:
            engine.block()

def main():
    """
    Start-up web server.
    """
    parser  = OptionParser()
    parser.add_option("-c", "--config", dest="config", default=False,\
        help="provide cherrypy configuration file")
    opts, _ = parser.parse_args()

    if  opts.config: # read provided configuration
        config = das_readconfig(opts.config, debug=True)
    else:
        config = das_readconfig(debug=True)

    # Start DAS server
    root = Root(config)
    root.start()

if __name__ == "__main__":
    main()
