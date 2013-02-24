#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0201,W0703,R0914,R0902,W0702,R0201,R0904,R0912,R0911
"""
DAS web interface, based on WMCore/WebTools
"""

__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import time
import thread
import cherrypy
import threading

from datetime import date
from cherrypy import expose, HTTPError
from cherrypy.lib.static import serve_file
from bson.objectid import ObjectId
from pymongo.errors import AutoReconnect

# DAS modules
import DAS
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_operators, das_filters
from DAS.core.das_ql import DAS_DB_KEYWORDS
from DAS.core.das_ply import das_parser_error
from DAS.core.das_query import DASQuery
from DAS.core.das_mongocache import DASLogdb
from DAS.utils.utils import getarg
from DAS.utils.ddict import DotDict
from DAS.utils.utils import genkey, print_exc, dastimestamp
from DAS.utils.thread import start_new_thread
from DAS.utils.das_db import db_gridfs
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.web.utils import free_text_parser, threshold
from DAS.web.utils import set_no_cache_flags
from DAS.web.utils import checkargs, das_json, gen_error_msg
from DAS.web.utils import dascore_monitor, gen_color, choose_select_key
from DAS.web.tools import exposedasjson
from DAS.web.tools import jsonstreamer
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code
from DAS.web.autocomplete import autocomplete_helper
from DAS.web.help_cards import help_cards
from DAS.web.request_manager import RequestManager
from DAS.web.dbs_daemon import DBSDaemon
from DAS.web.cms_representation import CMSRepresentation
from DAS.services.sitedb2.sitedb2_service import SiteDBService
from DAS.utils.global_scope import SERVICES
import DAS.utils.jsonwrapper as json

from DAS.core.das_query import WildcardMultipleMatchesException
import urllib

# TODO: move this to an appropriate place
from DAS.web.utils import HtmlString

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'collection', 'name',
            'reason', 'instance', 'view', 'query', 'fid', 'pid', 'next']
DAS_PIPECMDS = das_aggregators() + das_filters()

def onhold_worker(dasmgr, taskmgr, reqmgr, limit):
    "Worker daemon to process onhold requests"
    if  not dasmgr or not taskmgr or not reqmgr:
        return
    print "\n### Start onhold_worker"
    jobs = []
    while True:
        try:
            while jobs:
                try:
                    reqmgr.remove(jobs.pop(0))
                except:
                    break
            nrequests = reqmgr.size()
            for rec in reqmgr.items_onhold():
                dasquery  = DASQuery(rec['uinput'])
                addr      = rec['ip']
                if  (nrequests - taskmgr.nworkers()) < limit:
                    _evt, pid = taskmgr.spawn(\
                        dasmgr.call, dasquery, \
                            uid=addr, pid=dasquery.qhash)
                    jobs.append(pid)
                    reqmgr.remove_onhold(str(rec['_id']))
        except AutoReconnect:
            pass
        except Exception as err:
            print_exc(err)
        time.sleep(5)
    print "### END onhold_worker", time.time()

class DASWebService(DASWebManager):
    """
    DAS web service interface.
    """
    def __init__(self, dasconfig):
        DASWebManager.__init__(self, dasconfig)
        config = dasconfig['web_server']
        self.pid_pat     = re.compile(r'^[a-z0-9]{32}')
        self.base        = config['url_base']
        self.interval    = config.get('status_update', 2500)
        self.engine      = config.get('engine', None)
        nworkers         = config['web_workers']
        self.hot_thr     = config.get('hot_threshold', 3000)
        self.dasconfig   = dasconfig
        self.dburi       = self.dasconfig['mongodb']['dburi']
        self.lifetime    = self.dasconfig['mongodb']['lifetime']
        self.queue_limit = config.get('queue_limit', 50)
        qtype            = config.get('qtype', 'Queue')
        if  qtype not in ['Queue', 'PriorityQueue']:
            msg = 'Wrong queue type, qtype=%s' % qtype
            raise Exception(msg)
        if  self.engine:
            thr_name = 'DASWebService:PluginTaskManager'
            self.taskmgr = PluginTaskManager(bus=self.engine, \
                    nworkers=nworkers, name=thr_name, qtype=qtype)
            self.taskmgr.subscribe()
        else:
            thr_name = 'DASWebService:TaskManager'
            self.taskmgr = TaskManager(nworkers=nworkers, name=thr_name, \
                    qtype=qtype)
        self.adjust      = config.get('adjust_input', False)
        self.dasmgr      = None # defined at run-time via self.init()
        self.reqmgr      = None # defined at run-time via self.init()
        self.daskeys     = []   # defined at run-time via self.init()
        self.colors      = {}   # defined at run-time via self.init()
        self.dbs_url     = None # defined at run-time via self.init()
        self.dbs_global  = None # defined at run-time via self.init()
        self.dataset_daemon = config.get('dbs_daemon', False)
        self.dbsmgr      = {} # dbs_urls vs dbs_daemons, defined at run-time
        self.daskeyslist = [] # list of DAS keys
        self.init()

        # Monitoring thread which performs auto-reconnection
        thname = 'dbscore_monitor'
        start_new_thread(thname, dascore_monitor, \
                ({'das':self.dasmgr, 'uri':self.dburi}, self.init, 5))

    def process_requests_onhold(self):
        "Process requests which are on hold"
        try:
            limit = self.queue_limit/2
            start_new_thread('onhold_monitor', onhold_worker, \
                (self.dasmgr, self.taskmgr, self.reqmgr, limit))
        except Exception as exc:
            print_exc(exc)

    def dbs_daemon(self, config):
        """Start DBS daemon if it is requested via DAS configuration"""
        try:
            main_dbs_url = self.dbs_url
            self.dbs_urls = []
            print "\n### DBS URL:", self.dbs_url
            print "### DBS instances:", self.dbs_instances
            if  not self.dbs_url or not self.dbs_instances:
                return # just quit
            for inst in self.dbs_instances:
                self.dbs_urls.append(\
                        main_dbs_url.replace(self.dbs_global, inst))
            interval  = config.get('dbs_daemon_interval', 3600)
            dbsexpire = config.get('dbs_daemon_expire', 3600)
            dbs_config  = {'expire': dbsexpire}
            if  self.dataset_daemon:
                for dbs_url in self.dbs_urls:
                    dbsmgr = DBSDaemon(dbs_url, self.dburi, dbs_config)
                    self.dbsmgr[dbs_url] = dbsmgr
                    def dbs_updater(_dbsmgr, interval):
                        """DBS updater daemon"""
                        while True:
                            try:
                                _dbsmgr.update()
                            except:
                                pass
                            time.sleep(interval)
                    print "### Start DBSDaemon for %s" % dbs_url
                    thname = 'dbs_updater:%s' % dbs_url
                    start_new_thread(thname, dbs_updater, (dbsmgr, interval, ))
        except Exception as exc:
            print_exc(exc)

    def init(self):
        """Init DAS web server, connect to DAS Core"""
        try:
            self.logcol     = DASLogdb(self.dasconfig)
            self.reqmgr     = RequestManager(self.dburi, lifetime=self.lifetime)
            self.dasmgr     = DASCore(engine=self.engine)
            self.repmgr     = CMSRepresentation(self.dasconfig, self.dasmgr)
            self.daskeys    = self.dasmgr.das_keys()
            self.gfs        = db_gridfs(self.dburi)
            self.daskeys.sort()
            self.dasmapping = self.dasmgr.mapping
            self.dbs_url    = self.dasmapping.dbs_url()
            self.dbs_global = self.dasmapping.dbs_global_instance()
            self.dbs_instances = self.dasmapping.dbs_instances()
            self.dasmapping.init_presentationcache()
            self.colors = {'das':gen_color('das')}
            for system in self.dasmgr.systems:
                self.colors[system] = gen_color(system)
            self.sitedbmgr = SERVICES.get('sitedb2', None) # SiteDB from global scope
            # Start DBS daemon
            if  self.dataset_daemon:
                self.dbs_daemon(self.dasconfig['web_server'])
            if  not self.daskeyslist:
                keylist = [r for r in self.dasmapping.das_presentation_map()]
                keylist.sort(key=lambda r: r['das'])
                self.daskeyslist = keylist
        except Exception as ConnectionFailure:
            tstamp = dastimestamp('')
            thread = threading.current_thread()
            print "### MongoDB connection failure thread=%s, id=%s, time=%s" \
                    % (thread.name, thread.ident, tstamp)
        except Exception as exc:
            print_exc(exc)
            self.dasmgr  = None
            self.reqmgr  = None
            self.dbs_url = None
            self.dbs_global = None
            self.dbs_instances = []
            self.daskeys = []
            self.colors  = {}
            return
        # Start Onhold_request daemon
        if  self.dasconfig['web_server'].get('onhold_daemon', False):
            self.process_requests_onhold()

    def logdb(self, query):
        """
        Make entry in Logging DB
        """
        qhash = genkey(query)
        args  = cherrypy.request.params
        doc = dict(qhash=qhash,
                date=int(str(date.fromtimestamp(time.time())).replace('-', '')),
                headers=cherrypy.request.headers,
                method=cherrypy.request.method,
                path=cherrypy.request.path_info,
                args=args,
                ip=cherrypy.request.remote.ip,
                hostname=cherrypy.request.remote.name,
                port=cherrypy.request.remote.port)
        self.logcol.insert('web', doc)

    def get_nhits(self):
        "Return number of hits per day client made"
        tsec  = time.mktime(date.timetuple(date.today()))
        spec  = {'ip': cherrypy.request.remote.ip, 'ts': {'$gte': tsec},
                 'args.pid': {'$exists': False}, # do not count pid requests
                 'path': '/cache'} # requests from das_client calls
        nhits = self.logcol.find(spec, count=True)
        return nhits

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def redirect(self, **kwargs):
        """
        Represent DAS redirect page
        """
        dmsg = 'You do not have permission to access the resource requested.'
        msg  = kwargs.get('reason', dmsg)
        if  msg:
            msg = 'Reason: ' + msg
        page = self.templatepage('das_redirect', msg=msg)
        return self.page(page, response_div=False)

    def bottom(self, response_div=True):
        """
        Define footer for all DAS web pages
        """
        return self.templatepage('das_bottom', div=response_div,
                version=DAS.version)

    def page(self, content, ctime=None, response_div=True):
        """
        Define footer for all DAS web pages
        """
        page  = self.top()
        page += content
        page += self.templatepage('das_bottom', ctime=ctime, 
                                  version=DAS.version, div=response_div)
        return page

    @expose
    @checkargs(DAS_WEB_INPUTS + ['section', 'highlight'])
    def faq(self, **kwargs):
        """
        represent DAS FAQ.
        """
        section = kwargs.get('section', None)
        highlight = kwargs.get('highlight', None)
        guide = self.templatepage('dbsql_vs_dasql', 
                    operators=', '.join(das_operators()))
        daskeys = self.templatepage('das_keys', daskeys=self.daskeyslist)
        page = self.templatepage('das_faq', guide=guide, daskeys=daskeys,
                section=section, highlight=highlight,
                operators=', '.join(das_operators()), 
                aggregators=', '.join(das_aggregators()))
        return self.page(page, response_div=False)

    @expose
    def cli(self):
        """
        Serve DAS CLI file download.
        """
        dasroot = '/'.join(__file__.split('/')[:-3])
        clifile = os.path.join(dasroot, 'DAS/tools/das_client.py')
        return serve_file(clifile, content_type='text/plain')

    @expose
    def movetodas(self):
        "Placeholder page for DBS to DAS migration"
        style = \
            "width:600px;margin-left:auto;margin-right:auto;padding-top:20px"
        page  = """<div style="%s">""" % style
        page += "Dear user,<br/>DBS Data Discovery page is depricated.<br/>"
        page += "Please migrate to Data Aggregation Service located at"
        page += "<p>https://cmsweb.cern.ch/das/</p>"
        page += "<em>CMS HTTP group.</em>"
        page += "</div>"""
        return page

    @expose
    def opensearch(self):
        """
        Serve DAS opensearch file.
        """
        if  self.base and self.base.find('http://') != -1:
            base = self.base
        else:
            base = 'http://cmsweb.cern.ch/das'
        desc = self.templatepage('das_opensearch', base=base)
        cherrypy.response.headers['Content-Type'] = \
                'application/opensearchdescription+xml'
        return desc

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def services(self):
        """
        represent DAS services
        """
        dasdict = {}
        daskeys = []
        for system, keys in self.dasmgr.mapping.daskeys().iteritems():
            if  system not in self.dasmgr.systems:
                continue
            tmpdict = {}
            for key in keys:
                tmpdict[key] = self.dasmgr.mapping.lookup_keys(system, key) 
                if  key not in daskeys:
                    daskeys.append(key)
            dasdict[system] = dict(keys=dict(tmpdict), 
                apis=self.dasmgr.mapping.list_apis(system))
        mapreduce = [r for r in self.dasmgr.rawcache.get_map_reduce()]
        page = self.templatepage('das_services', dasdict=dasdict, 
                        daskeys=daskeys, mapreduce=mapreduce)
        return self.page(page, response_div=False)

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def api(self, name):
        """
        Return DAS mapping record about provided API.
        """
        record = self.dasmgr.mapping.api_info(name)
        page   = "<b>DAS mapping record</b>"
        page  += das_json(record)
        return self.page(page, response_div=False)

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def default(self, *args, **kwargs):
        """
        Default method.
        """
        return self.index(args, kwargs)

    def adjust_input(self, kwargs):
        """
        Adjust user input wrt common DAS keyword patterns, e.g.
        Zee -> dataset=*Zee*, T1_US -> site=T1_US*. This method
        only works if self.adjust is set in configuration of DAS server.
        This method can be customization for concrete DAS applications via
        external free_text_parser function (part of DAS.web.utils module)
        """
        if  not self.adjust:
            return
        uinput = kwargs.get('input', '')
        query_part = uinput.split('|')[0]
        if  query_part == 'queries' or query_part == 'records':
            return
        new_input = free_text_parser(uinput, self.daskeys)
        if  uinput and new_input == uinput:
            selkey = choose_select_key(uinput, self.daskeys, 'dataset')
            if  selkey and len(new_input) > len(selkey) and \
                new_input[:len(selkey)] != selkey:
                new_input = selkey + ' ' + new_input
        kwargs['input'] = new_input



    def _get_dbsmgr_for_db_instance(self, str_dbsinst):
        """
        Given a string representation of DBS instance, returns DBSManager
        instance which "knows" how to look up datasets
        (further for performance reasons of searching by wildcard and substring,
        we may want to store then even outside MongoDB).
        """
        # TODO: instance selection shall be more clean
        if  self.dataset_daemon:
            dbs_urls = [d for d in self.dbsmgr.keys() if d.find(str_dbsinst) != -1]
            if  len(dbs_urls) == 1:
                return self.dbsmgr[dbs_urls[0]]
        return None


    def generate_dasquery(self, uinput, inst, html_error=True):
        """
        Check provided input as valid DAS input query.
        Returns status and content (either error message or valid DASQuery)
        """
        def helper(msg, html_error=None):
            """Helper function which provide error template"""
            if  not html_error:
                return msg
            guide = self.templatepage('dbsql_vs_dasql', 
                        operators=', '.join(das_operators()))
            page = self.templatepage('das_ambiguous', msg=msg, base=self.base,
                        guide=guide)
            return page
        if  not uinput:
            return 1, helper('No input query')
        # Generate DASQuery object, if it fails we catch the exception and
        # wrap it for upper layer (web interface)
        try:
            dasquery = DASQuery(uinput, instance=inst,
                    active_dbsmgr = self._get_dbsmgr_for_db_instance(inst))
        except Exception as err:
            # allow html in the exception message
            exc_message = str(err)
            if  isinstance(err.message, HtmlString):
                exc_message = err.message

            # Wildcard exception has to be processed here,
            # because only this class knows about Web UI!
            if  isinstance(err, WildcardMultipleMatchesException):
                options = []
                for dpat, query in err.options.items():
                    # TODO: get view and limit
                    params = cherrypy.request.params.copy()
                    params['input'] = query
                    das_url = '/das/request?' + urllib.urlencode(params)
                    make_link_to_query = \
                        lambda q: "<a href='%s'>%s</a>"\
                           % (das_url,  q.replace(dpat, '<b>%s</b>' % dpat))
                    options.append(make_link_to_query(query))

                exc_message = HtmlString(err.message + '<br>\n'.join(options))
            das_parser_error(uinput, 'WildcardMultipleMatchedException')
            return 1, helper(exc_message, html_error)

        fields = dasquery.mongo_query.get('fields', [])
        if  not fields:
            fields = []
        spec   = dasquery.mongo_query.get('spec', {})
        for word in fields+spec.keys():
            found = 0
            if  word in DAS_DB_KEYWORDS:
                found = 1
            for key in self.daskeys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                msg = 'Provided input does not contain a valid DAS key'
                return 1, helper(msg, html_error)
        if  isinstance(uinput, dict): # DASQuery w/ {'spec':{'_id:id}}
            pass
        elif uinput.find('queries') != -1:
            pass
        elif uinput.find('records') != -1:
            pass
        else: # normal user DAS query
            try:
                service_map = dasquery.service_apis_map()
            except Exception as exc:
                msg = 'Fail to lookup DASQuery service API map'
                print msg
                print_exc(exc)
                return 1, helper(msg, html_error)
            if  not service_map:
                msg  = "None of the API's registered in DAS "
                msg += "can resolve this query"
                return 1, helper(msg, html_error)
        return 0, dasquery

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def index(self, *args, **kwargs):
        """
        represents DAS web interface. 
        It uses das_searchform template for
        input form and yui_table for output Table widget.
        """
        uinput = getarg(kwargs, 'input', '') 
        return self.page(self.form(uinput=uinput, cards=True))

    def form(self, uinput='', instance=None, view='list', cards=False):
        """
        provide input DAS search form
        """
        if  "'" in uinput: # e.g. file.creation_date>'20120101 12:01:01'
            uinput = uinput.replace("'", '"')
        if  not instance:
            instance = self.dbs_global
        cards = self.templatepage('das_cards', base=self.base, show=cards, \
                width=900, height=220, cards=help_cards(self.base))
        daskeys = self.templatepage('das_keys', daskeys=self.daskeyslist)
        page  = self.templatepage('das_searchform', input=uinput, \
                init_dbses=list(self.dbs_instances), daskeys=daskeys, \
                base=self.base, instance=instance, view=view, cards=cards)
        return page

    @expose
    def error(self, msg, wrap=True):
        """
        Show error message.
        """
        page = self.templatepage('das_error', msg=str(msg))
        if  wrap:
            page  = self.page(self.form() + page)
        return page

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def gridfs(self, **kwargs):
        """
        Retieve records from GridFS
        """
        time0 = time.time()
        if  not kwargs.has_key('fid'):
            code = web_code('No file id')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        fid  = kwargs.get('fid')
        data = {'status':'requested', 'fid':fid}
        try:
            fds = self.gfs.get(ObjectId(fid))
            return fds.read()
        except Exception as exc:
            print_exc(exc)
            code = web_code('Exception')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return json.dumps(data)

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def records(self, *args, **kwargs):
        """
        Retieve all records id's.
        """
        try:
            recordid = None
            if  args:
                recordid = args[0]
                spec = {'_id':ObjectId(recordid)}
                fields = None
                query = dict(fields=fields, spec=spec)
            elif  kwargs and kwargs.has_key('_id'):
                spec = {'_id': ObjectId(kwargs['_id'])}
                fields = None
                query = dict(fields=fields, spec=spec)
            else: # return all ids
                query = dict(fields=None, spec={})

            res      = ''
            time0    = time.time()
            idx      = getarg(kwargs, 'idx', 0)
            limit    = getarg(kwargs, 'limit', 10)
            coll     = kwargs.get('collection', 'merge')
            inst     = kwargs.get('instance', self.dbs_global)
            form     = self.form(uinput="")
            check, content = self.generate_dasquery(query, inst)
            if  check:
                return self.page(form + content, ctime=time.time()-time0)
            dasquery = content # returned content is valid DAS query
            nresults = self.dasmgr.rawcache.nresults(dasquery, coll)
            gen      = self.dasmgr.rawcache.get_from_cache\
                (dasquery, idx=idx, limit=limit, collection=coll)
            if  recordid: # we got id
                for row in gen:
                    res += das_json(row)
            else:
                for row in gen:
                    rid  = row['_id']
                    del row['_id']
                    res += self.templatepage('das_record', \
                            id=rid, collection=coll, daskeys=', '.join(row))
            if  recordid:
                page  = res
            else:
                url   = '/das/records?'
                if  nresults:
                    page = self.templatepage('das_pagination', \
                        nrows=nresults, idx=idx, limit=limit, url=url)
                else:
                    page = 'No results found, nresults=%s' % nresults
                page += res

            ctime   = (time.time()-time0)
            page = self.page(form + page, ctime=ctime)
            return page
        except Exception as exc:
            print_exc(exc)
            return self.error(gen_error_msg(kwargs))

    @jsonstreamer
    def datastream(self, kwargs):
        """Stream DAS data into JSON format"""
        head = kwargs.get('head', dict(timestamp=time.time()))
        if  not head.has_key('mongo_query'):
            head['mongo_query'] = head['dasquery'].mongo_query \
                if head.has_key('dasquery') else {}
        if  head.has_key('dasquery'):
            del head['dasquery']
        if  head.has_key('args'):
            del head['args']
        data = kwargs.get('data', [])
        return head, data

    def get_data(self, kwargs):
        """
        Invoke DAS workflow and get data from the cache.
        """
        head   = dict(timestamp=time.time())
        head['args'] = kwargs
        uinput = kwargs.get('input', '')
        inst   = kwargs.get('instance', self.dbs_global)
        idx    = getarg(kwargs, 'idx', 0)
        limit  = getarg(kwargs, 'limit', 0) # do not impose limit
        coll   = kwargs.get('collection', 'merge')
        dasquery = kwargs.get('dasquery', None)
        time0  = time.time()
        if  dasquery:
            dasquery = DASQuery(dasquery, instance=inst)
        else:
            check, content = \
                    self.generate_dasquery(uinput, inst, html_error=False)
            if  check:
                head.update({'status': 'fail', 'reason': content,
                             'ctime': time.time()-time0, 'input': uinput})
                data = []
                return head, data
            dasquery = content # returned content is valid DAS query
        try:
            nres = self.dasmgr.nresults(dasquery, coll)
            data = \
                self.dasmgr.get_from_cache(dasquery, idx, limit)
            if  dasquery.aggregators:
                # aggregators split DAS record into sub-system and then
                # apply aggregator functions, therefore we need to correctly
                # account for nresults. Resolve generator into list and take
                # its length as nresults value.
                data = [r for r in data]
                nres = len(data)
            head.update({'status':'ok', 'nresults':nres,
                         'ctime': time.time()-time0, 'dasquery': dasquery})
        except Exception as exc:
            print_exc(exc)
            head.update({'status': 'fail', 'reason': str(exc),
                         'ctime': time.time()-time0, 'dasquery': dasquery})
            data = []
        head.update({'incache':self.dasmgr.incache(dasquery, coll='cache'),
                     'apilist':self.dasmgr.apilist(dasquery)})
        return head, data

    def busy(self):
        """
        Check server load and report busy status if
        nrequests - nworkers > queue limit
        """
        nrequests = self.reqmgr.size()
        if  (nrequests - self.taskmgr.nworkers()) > self.queue_limit:
            msg = '#request=%s, queue_limit=%s, #workers=%s' \
                    % (nrequests, self.taskmgr.nworkers(), self.queue_limit)
            print dastimestamp('DAS WEB SERVER IS BUSY '), msg
            return True
        return False

    def busy_page(self, uinput=None):
        """DAS server busy page layout"""
        page = "<h3>DAS server is busy, please try later</h3>"
        form = self.form(uinput)
        return self.page(form + page)

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def cache(self, **kwargs):
        """
        DAS web cache interface. Fire up new process for new requests and
        record its pid. The client is in charge to keep track of pid.
        The new process uses DAS core call to request the data into cache.
        Since query are cached the repeated call with the same query
        has no cost to DAS core.
        """
        # do not allow caching
        set_no_cache_flags()

        uinput = kwargs.get('input', '').strip()
        if  not uinput:
            head = {'status': 'fail', 'reason': 'No input found',
                    'args': kwargs, 'ctime': 0, 'input': uinput}
            data = []
            return self.datastream(dict(head=head, data=data))
        self.adjust_input(kwargs)
        pid    = kwargs.get('pid', '')
        inst   = kwargs.get('instance', self.dbs_global)
        uinput = kwargs.get('input', '')
        data   = []
        check, content = self.generate_dasquery(uinput, inst)
        if  check:
            head = dict(timestamp=time.time())
            head.update({'status': 'fail',
                         'reason': 'Fail to create DASQuery object',
                         'ctime': 0})
            return self.datastream(dict(head=head, data=data))
        dasquery = content # returned content is valid DAS query
        status, _qhash = self.dasmgr.get_status(dasquery)
        if  status == 'ok':
            kwargs['dasquery'] = dasquery
            self.reqmgr.remove(dasquery.qhash)
            head, data = self.get_data(kwargs)
            return self.datastream(dict(head=head, data=data))
        kwargs['dasquery'] = dasquery.storage_query
        if  not pid and self.busy():
            head = dict(timestamp=time.time())
            head.update({'status': 'busy', 'reason': 'DAS server is busy',
                         'ctime': 0})
            return self.datastream(dict(head=head, data=data))
        if  pid:
            if  not self.pid_pat.match(str(pid)) or len(str(pid)) != 32:
                head = {'status': 'fail', 'reason': 'Invalid pid',
                        'args': kwargs, 'ctime': 0, 'input': uinput}
                data = []
                return self.datastream(dict(head=head, data=data))
            elif self.taskmgr.is_alive(pid):
                return pid
            else: # process is done, get data
                self.reqmgr.remove(pid)
                head, data = self.get_data(kwargs)
                return self.datastream(dict(head=head, data=data))
        else:
            config = self.dasconfig.get('cacherequests', {})
            thr = threshold(self.sitedbmgr, self.hot_thr, config)
            nhits = self.get_nhits()
            if  nhits > thr: # exceed threshold
                if  self.busy(): # put request onhold, server is busy
                    tstamp = time.time() + 60*(nhits/thr) + (nhits%thr)
                    pid  = dasquery.qhash
                    self.reqmgr.add_onhold(\
                        pid, uinput, cherrypy.request.remote.ip, tstamp)
                    head = {'status':'onhold',
                            'mongo_query':dasquery.mongo_query,
                            'pid':pid, 'nresults':0, 'ctime':0,
                            'ts':time.time()}
                    data = []
                    return self.datastream(dict(head=head, data=data))
            addr = cherrypy.request.headers.get('Remote-Addr')
            _evt, pid = self.taskmgr.spawn(\
                self.dasmgr.call, dasquery, uid=addr, pid=dasquery.qhash)
            self.logdb(uinput) # put entry in log DB once we place a request
            self.reqmgr.add(pid, kwargs)
            return pid

    def get_page_content(self, kwargs, complete_msg=True):
        """Retrieve page content for provided set of parameters"""
        page = ''
        try:
            view = kwargs.get('view', 'list')
            if  view == 'plain':
                if  kwargs.has_key('limit'):
                    del kwargs['limit']
            if  view in ['json', 'xml', 'plain'] and complete_msg:
                page = 'Request comlpeted. Reload the page ...'
            else:
                head, data = self.get_data(kwargs)
                func = getattr(self, view + "view")
                page = func(head, data)
        except HTTPError as _err:
            raise 
        except Exception as exc:
            print_exc(exc)
            msg  = gen_error_msg(kwargs)
            page = self.templatepage('das_error', msg=msg)
        return page

    @expose
    def makepy(self, dataset, instance):
        """
        Request to create CMSSW py snippet for a given dataset
        """
        pat = re.compile('/.*/.*/.*')
        if  not pat.match(dataset):
            msg = 'Invalid dataset name'
            return self.error(msg)
        query = "file dataset=%s instance=%s | grep file.name" \
                % (dataset, instance)
        try:
            data   = self.dasmgr.result(query, idx=0, limit=0)
        except Exception as exc:
            print_exc(exc)
            msg    = 'Exception: %s\n' % str(exc)
            msg   += 'Unable to retrieve data for query=%s' % query
            return self.error(msg)
        lfns = []
        for rec in data:
            filename = DotDict(rec).get('file.name')
            if  filename not in lfns:
                lfns.append(filename)
        page = self.templatepage('das_files_py', lfnList=lfns, pfnList=[])
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return page

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def request(self, **kwargs):
        """
        Request data from DAS cache.
        """
        # do not allow caching
        set_no_cache_flags()

        uinput  = kwargs.get('input', '').strip()
        if  not uinput:
            kwargs['reason'] = 'No input found'
            return self.redirect(**kwargs)

        time0   = time.time()
        self.adjust_input(kwargs)
        view    = kwargs.get('view', 'list')
        inst    = kwargs.get('instance', self.dbs_global)
        uinput  = kwargs.get('input', '')

        if  self.busy():
            return self.busy_page(uinput)

        self.logdb(uinput)

        form    = self.form(uinput=uinput, instance=inst, view=view)
        check, content = self.generate_dasquery(uinput, inst)
        if  check:
            if  view == 'list' or view == 'table':
                return self.page(form + content, ctime=time.time()-time0)
            else:
                return content
        dasquery = content # returned content is valid DAS query
        status, _qhash = self.dasmgr.get_status(dasquery)
        if  status == 'ok':
            kwargs['dasquery'] = dasquery
            page = self.get_page_content(kwargs, complete_msg=False)
            ctime = (time.time()-time0)
            if  view == 'list' or view == 'table':
                return self.page(form + page, ctime=ctime)
            return page
        else:
            kwargs['dasquery'] = dasquery.storage_query
            addr = cherrypy.request.headers.get('Remote-Addr')
            _evt, pid = self.taskmgr.spawn(self.dasmgr.call, dasquery,
                    uid=addr, pid=dasquery.qhash)
            self.reqmgr.add(pid, kwargs)
            if  self.taskmgr.is_alive(pid):
                page = self.templatepage('das_check_pid', method='check_pid',
                        uinput=uinput, view=view,
                        base=self.base, pid=pid, interval=self.interval)
            else:
                page = self.get_page_content(kwargs)
                self.reqmgr.remove(pid)
        ctime = (time.time()-time0)
        return self.page(form + page, ctime=ctime)

    @expose
    def requests(self):
        """Return list of all current requests in DAS queue"""
        page = ""
        count = 0
        for row in self.reqmgr.items():
            tst = time.strftime("%Y%m%d %H:%M:%S GMT", time.gmtime(row['ts']))
            page += '<li>%s placed at %s<br/>%s</li>' \
                        % (row['_id'], tst, row['kwds'])
            count += 1
        if  page:
            page = "<ul>%s</ul>" % page
        else:
            page = "The request queue is empty"
        if  count:
            page += '<div>Total: %s requests</div>' % count
        return self.page(page)

    @expose
    @checkargs(['pid'])
    def check_pid(self, pid):
        """
        Check status of given pid. This is a server callback
        function for ajaxCheckPid, see js/ajax_utils.js
        """
        # do not allow caching
        set_no_cache_flags()

        img  = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
        page = ''
        try:
            if  self.taskmgr.is_alive(pid):
                page = img + " processing PID=%s" % pid
            else:
                self.reqmgr.remove(pid)
                page  = 'Request PID=%s is completed' % pid
                page += ', please wait for results to load'
        except Exception as err:
            msg = 'check_pid fails for pid=%s' % pid
            print dastimestamp('DAS WEB ERROR '), msg
            print_exc(err)
            self.reqmgr.remove(pid)
            self.taskmgr.remove(pid)
            return self.error(gen_error_msg({'pid':pid}), wrap=False)
        return page

    def listview(self, head, data):
        """DAS listview data representation"""
        return self.repmgr.listview(head, data)

    def tableview(self, head, data):
        """DAS tabular view data representation"""
        return self.repmgr.tableview(head, data)

    def plainview(self, head, data):
        """DAS plain view data representation"""
        return self.repmgr.plainview(head, data)

    def xmlview(self, head, data):
        """DAS XML data representation"""
        return self.repmgr.xmlview(head, data)

    def jsonview(self, head, data):
        """DAS JSON data representation"""
        return self.repmgr.jsonview(head, data)

    @exposedasjson
    @checkargs(['query', 'dbs_instance'])
    def autocomplete(self, **kwargs):
        """
        Provides autocomplete functionality for DAS web UI.
        """
        query = kwargs.get("query", "").strip()
        result = autocomplete_helper(query, self.dasmgr, self.daskeys)
        dataset = [r for r in result if r['value'].find('dataset=')!=-1]
        dbsinst = kwargs.get('dbs_instance', self.dbs_global)
        if  self.dataset_daemon and len(dataset):
            dbs_urls = [d for d in self.dbsmgr.keys() if d.find(dbsinst) != -1]
            if  len(dbs_urls) == 1:
                dbsmgr = self.dbsmgr[dbs_urls[0]]
                if  query.find('dataset=') != -1:
                    query = query.replace('dataset=', '')
                for row in dbsmgr.find(query):
                    result.append({'css': 'ac-info',
                                   'value': 'dataset=%s' % row,
                                   'info': 'dataset'})
        return result

