#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0201,W0703,R0914,R0902,W0702,R0201,R0904,R0912,R0911
"""
KWS web interface, based on WMCore/WebTools
"""
__author__ = "Vidmantas Zemleris"

# system modules
import re
import threading

from pymongo.errors import ConnectionFailure


# DAS modules
from DAS.core.das_core import DASCore
from DAS.utils.url_utils import disable_urllib2Proxy
from DAS.utils.utils import print_exc, dastimestamp
from DAS.utils.thread import start_new_thread
from DAS.web.utils import dascore_monitor
from DAS.web.das_webmanager import DASWebManager

# do not require any of KWS prerequisities (e.g. nltk), in case it's disabled
try:
    # keyword search
    from DAS.web.das_kwd_search import KeywordSearchHandler
except Exception as exc:
    print_exc(exc)

#TODO: do we preserve all of these inputs once contacting KWS?
# disable default urllib2 proxy
disable_urllib2Proxy()

from cherrypy import expose
from DAS.web.das_web_srv import DAS_WEB_INPUTS
from DAS.web.tools import enable_cross_origin
from DAS.web.utils import checkargs, set_no_cache_flags


class KWSWebService(DASWebManager):
    """
    DAS web service interface.
    """

    def __init__(self, dasconfig, main_das_app):
        DASWebManager.__init__(self, dasconfig)
        self.main_das_app = main_das_app

        config = dasconfig['web_server']
        self.dasconfig = dasconfig
        self.dburi = self.dasconfig['mongodb']['dburi']
        self.lifetime = self.dasconfig['mongodb']['lifetime']
        self.dasmgr = None # defined at run-time via self.init()
        self.kws = None # defined at run-time via self.init()
        self.init()

        # Monitoring thread which performs auto-reconnection
        thname = 'dbscore_monitor'
        start_new_thread(thname, dascore_monitor, ({'das': self.dasmgr,
                                                    'uri': self.dburi},
                                                   self.init, 5))

    def init(self):
        """Init DAS web server, connect to DAS Core"""
        try:
            self.dasmgr = DASCore(multitask=False) # engine=self.engine,
            self.kws = KeywordSearchHandler(self.dasmgr)
        except ConnectionFailure as _err:
            tstamp = dastimestamp('')
            mythr = threading.current_thread()
            print "### MongoDB connection failure thread=%s, id=%s, time=%s" \
                  % (mythr.name, mythr.ident, tstamp)
        except Exception as exc:
            print_exc(exc)
            self.dasmgr = None
            self.kws = None
            return

    def _get_dbsmgr(self, inst):
        # TODO: do we want to run DBSMngr separately?
        return self.main_das_app._get_dbsmgr(inst)

    @expose
    @enable_cross_origin
    @checkargs(DAS_WEB_INPUTS)
    def kws_async(self, **kwargs):
        """
        Returns KeywordSearch results for AJAX call (for now as html snippet)
        """
        set_no_cache_flags()

        uinput = kwargs.get('input', '').strip()
        inst = kwargs.get('instance', '').strip()
        if not inst:
            return 'you must provide DBS instance name'

        # it is simplest to run KWS on cherrypy web threads, if more requests
        # come than size of thread pool; they'll have to wait. that looks OK...

        if not uinput:
            return 'the query must be not empty'

        if not self.kws:
            return 'Query suggestions are unavailable right now...'

        timeout = self.dasconfig['keyword_search']['timeout']
        show_scores = self.dasconfig['keyword_search'].get('show_scores', False)
        dbsmngr = self._get_dbsmgr(inst)

        return self.kws.handle_search(self,
                                      query=uinput,
                                      dbsmngr=dbsmngr,
                                      is_ajax=True,
                                      timeout=timeout,
                                      show_score=show_scores)