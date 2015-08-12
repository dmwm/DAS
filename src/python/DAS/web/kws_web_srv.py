#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0511
# i.e. allow TODOs
"""
KWS web interface, based on WMCore/WebTools
"""
from __future__ import print_function
__author__ = "Vidmantas Zemleris"

# system modules
import threading

from pymongo.errors import ConnectionFailure


# DAS modules
from DAS.core.das_core import DASCore
from DAS.utils.url_utils import disable_urllib2Proxy
from DAS.utils.utils import print_exc, dastimestamp
from DAS.utils.thread import start_new_thread
from DAS.web.utils import dascore_monitor
from DAS.web.das_webmanager import DASWebManager

# do not require any of KWS prerequisites (e.g. nltk), in case it's disabled
try:
    # keyword search
    from DAS.web.das_kwd_search import KeywordSearchHandler
except Exception as _exc:
    KeywordSearchHandler = None
    print_exc(_exc)

#TODO: do we preserve all of these inputs once contacting KWS?
# disable default urllib2 proxy
disable_urllib2Proxy()

from cherrypy import expose, tools
from DAS.web.das_web_srv import DAS_WEB_INPUTS
from DAS.web.tools import enable_cross_origin
from DAS.web.utils import checkargs, set_no_cache_flags


class KWSWebService(DASWebManager):
    """
    DAS web service interface.
    """

    def __init__(self, dasconfig):
        DASWebManager.__init__(self, dasconfig)

        self.dasconfig = dasconfig
        self.dburi = self.dasconfig['mongodb']['dburi']
        self.dasmgr = None  # defined at run-time via self.init()
        self.kws = None  # defined at run-time via self.init()
        self.init()

        # Monitoring thread which performs auto-reconnection
        thname = 'dascore_monitor_kws'
        start_new_thread(thname, dascore_monitor, ({'das': self.dasmgr,
                                                    'uri': self.dburi},
                                                   self.init, 5))

    def init(self):
        """Init DAS web server, connect to DAS Core"""
        try:
            self.dasmgr = DASCore(multitask=False)
            self.dbs_instances = self.dasmgr.mapping.dbs_instances()
            self.dbs_global = self.dasmgr.mapping.dbs_global_instance()
            if  KeywordSearchHandler:
                self.kws = KeywordSearchHandler(self.dasmgr)
        except ConnectionFailure:
            tstamp = dastimestamp('')
            mythr = threading.current_thread()
            print("### MongoDB connection failure thread=%s, id=%s, time=%s" \
                  % (mythr.name, mythr.ident, tstamp))
        except Exception as exc:
            print_exc(exc)
            self.dasmgr = None
            self.kws = None

    @expose
    @tools.secmodv2()
    def index(self, *args, **kwargs):
        """Main page"""
        page = "DAS KWS Server"
        return self.page(page)

    def top(self):
        """
        Provide masthead for all web pages
        """
        return ''

    def bottom(self, div=""):
        """
        Provide footer for all web pages
        """
        return ''

    def _get_dbs_inst(self, inst):
        """
        returns dbsmngr for given instance
        """
        if inst in self.dbs_instances:
            return inst
        else:
            return self.dbs_global

    @expose
    @enable_cross_origin
    @checkargs(DAS_WEB_INPUTS)
    @tools.secmodv2()
    def kws_async(self, **kwargs):
        """
        Returns KeywordSearch results for AJAX call (for now as html snippet)
        """
        set_no_cache_flags()

        uinput = kwargs.get('input', '').strip()
        inst = kwargs.get('instance', '').strip()
        if not inst:
            return 'You must provide DBS instance name'

        # it is simplest to run KWS on cherrypy web threads, if more requests
        # come than size of thread pool; they'll have to wait. that looks OK...

        if not uinput:
            return 'The query must be not empty'

        if not self.kws:
            return 'Query suggestions are unavailable right now...'

        timeout = self.dasconfig['keyword_search']['timeout']
        show_scores = self.dasconfig['keyword_search'].get('show_scores', False)
        inst = self._get_dbs_inst(inst)  # validate dbs inst name or use default

        return self.kws.handle_search(self,
                                      query=uinput,
                                      dbs_inst=inst,
                                      is_ajax=True,
                                      timeout=timeout,
                                      show_score=show_scores)
