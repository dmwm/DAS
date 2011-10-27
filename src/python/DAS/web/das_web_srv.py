#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0201,W0703
"""
DAS web interface, based on WMCore/WebTools
"""

__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import time
import thread
import urllib
import urlparse
import cherrypy

from cherrypy import expose, HTTPError
from cherrypy.lib.static import serve_file
from pymongo.objectid import ObjectId

# DAS modules
import DAS
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_operators, das_filters
from DAS.core.das_ply import das_parser_error
from DAS.utils.utils import getarg
from DAS.utils.ddict import DotDict
from DAS.utils.utils import genkey, print_exc
from DAS.utils.das_db import db_connection, db_gridfs
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.web.utils import free_text_parser
from DAS.web.utils import checkargs, das_json, gen_error_msg
from DAS.web.utils import dascore_monitor, gen_color, choose_select_key
from DAS.web.tools import exposedasjson
from DAS.web.tools import request_headers, jsonstreamer
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code
from DAS.web.autocomplete import autocomplete_helper
from DAS.web.help_cards import help_cards
from DAS.web.request_manager import RequestManager
from DAS.web.dbs_daemon import DBSDaemon
from DAS.web.cms_representation import CMSRepresentation
from DAS.web.das_representation import DASRepresentation

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'collection', 'name', 'dir', 
        'instance', 'format', 'view', 'skey', 'query', 'fid', 'pid', 'next']
DAS_PIPECMDS = das_aggregators() + das_filters()

class DASWebService(DASWebManager):
    """
    DAS web service interface.
    """
    def __init__(self, dasconfig):
        DASWebManager.__init__(self, dasconfig)
        config = dasconfig['web_server']
        self.base        = config['url_base']
        self.interval    = 3000 # initial update interval in msec
        self.engine      = config.get('engine', None)
        nworkers         = config['number_of_workers']
        self.dasconfig   = dasconfig
        self.dburi       = self.dasconfig['mongodb']['dburi']
        self.lifetime    = self.dasconfig['mongodb']['lifetime']
        self.queue_limit = config.get('queue_limit', 50)
        if  self.engine:
            self.taskmgr = PluginTaskManager(bus=self.engine, nworkers=nworkers)
            self.taskmgr.subscribe()
        else:
            self.taskmgr = TaskManager(nworkers=nworkers)
        self.adjust      = config.get('adjust_input', False)

        self.init()
        # Define representation layer, to be overwritten at self.init
        self.repmgr      = DASRepresentation(config)

        # Monitoring thread which performs auto-reconnection
        thread.start_new_thread(dascore_monitor, \
                ({'das':self.dasmgr, 'uri':self.dburi}, self.init, 5))

        # Obtain DBS global instance or set it as None
        if  self.dasconfig.has_key('dbs'):
            self.dbs_global = \
                self.dasconfig['dbs'].get('dbs_global_instance', None)
            self.dbs_instances = \
                self.dasconfig['dbs'].get('dbs_instances', [])
        else:
            self.dbs_global = None
            self.dbs_instances = []

        # Start DBS daemon
        self.dataset_daemon = config.get('dbs_daemon', False)
        if  self.dataset_daemon:
            self.dbs_daemon(config)

    def dbs_daemon(self, config):
        """Start DBS daemon if it is requested via DAS configuration"""
        try:
            main_dbs_url = self.dasconfig['dbs']['dbs_global_url']
            self.dbs_urls = []
            for inst in self.dbs_instances:
              self.dbs_urls.append(main_dbs_url.replace(self.dbs_global, inst))
            interval  = config.get('dbs_daemon_interval', 3600)
            dbsexpire = config.get('dbs_daemon_expire', 3600)
            self.dbsmgr = {} # dbs_urls vs dbs_daemons
            if  self.dataset_daemon:
                for dbs_url in self.dbs_urls:
                    dbsmgr = DBSDaemon(dbs_url, self.dburi, expire=dbsexpire)
                    self.dbsmgr[dbs_url] = dbsmgr
                    def dbs_updater(_dbsmgr, interval):
                        """DBS updater daemon"""
                        while True:
                            try:
                                _dbsmgr.update()
                            except:
                                pass
                            time.sleep(interval)
                    print "Start DBSDaemon for %s" % dbs_url
                    thread.start_new_thread(dbs_updater, (dbsmgr, interval, ))
        except Exception as exc:
            print_exc(exc)

    def init(self):
        """Init DAS web server, connect to DAS Core"""
        capped_size = self.dasconfig['loggingdb']['capped_size']
        logdbname   = self.dasconfig['loggingdb']['dbname']
        logdbcoll   = self.dasconfig['loggingdb']['collname']
        try:
            self.con        = db_connection(self.dburi)
            if  logdbname not in self.con.database_names():
                dbname      = self.con[logdbname]
                options     = {'capped':True, 'size': capped_size}
                dbname.create_collection('db', **options)
                self.warning('Created %s.%s, size=%s' \
                % (logdbname, logdbcoll, capped_size))
            self.logcol     = self.con[logdbname][logdbcoll]
            self.reqmgr     = RequestManager(self.dburi, lifetime=self.lifetime)
            self.dasmgr     = DASCore(engine=self.engine)
            config          = self.dasconfig['web_server']
            self.repmgr     = CMSRepresentation(config, self.dasmgr)
            self.daskeys    = self.dasmgr.das_keys()
            self.gfs        = db_gridfs(self.dburi)
            self.daskeys.sort()
            self.dasmapping = self.dasmgr.mapping
            self.dasmapping.init_presentationcache()
            self.colors = {}
            for system in self.dasmgr.systems:
                self.colors[system] = gen_color(system)
        except Exception as exc:
            print_exc(exc)
            self.dasmgr = None
            self.daskeys = []
            self.colors = {}

    def logdb(self, query):
        """
        Make entry in Logging DB
        """
        qhash = genkey(query)
        doc = dict(qhash=qhash, timestamp=time.time(),
                headers=cherrypy.request.headers,
                method=cherrypy.request.method,
                path=cherrypy.request.path_info,
                args=cherrypy.request.params,
                ip=cherrypy.request.remote.ip,
                hostname=cherrypy.request.remote.name,
                port=cherrypy.request.remote.port)
        self.logcol.insert(doc)

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def redirect(self, *args, **kwargs):
        """
        Represent DAS redirect page
        """
        msg  = kwargs.get('reason', '')
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
    @checkargs(DAS_WEB_INPUTS)
    def faq(self, *args, **kwargs):
        """
        represent DAS FAQ.
        """
        guide = self.templatepage('dbsql_vs_dasql', 
                    operators=', '.join(das_operators()))
        page = self.templatepage('das_faq', guide=guide,
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
    def services(self, *args, **kwargs):
        """
        represent DAS services
        """
        dasdict = {}
        daskeys = []
        for system, keys in self.dasmgr.mapping.daskeys().items():
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
    def api(self, name, **kwargs):
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

    def check_input(self, uinput):
        """
        Check provided input as valid DAS input query.
        Returns status and content.
        """
        def helper(msg):
            """Helper function which provide error template"""
            guide = self.templatepage('dbsql_vs_dasql', 
                        operators=', '.join(das_operators()))
            page = self.templatepage('das_ambiguous', msg=msg, base=self.base,
                        guide=guide)
            return page
        if  not uinput:
            return 1, helper('No input query')
        # check provided input. If at least one word is not part of das_keys
        # return ambiguous template.
        try:
            mongo_query = self.dasmgr.mongoparser.parse(uinput, \
                                add_to_analytics=False)
        except Exception as exc:
            return 1, helper(das_parser_error(uinput, str(exc)))
        fields = mongo_query.get('fields', [])
        if  not fields:
            fields = []
        spec   = mongo_query.get('spec', {})
        if  not fields+spec.keys():
            msg = 'Provided input does not resolve into a valid set of keys'
            return 1, helper(msg)
        for word in fields+spec.keys():
            found = 0
            if  word == 'queries':
                found = 1
            for key in self.daskeys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                msg = 'Provided input does not contain a valid DAS key'
                return 1, helper(msg)
        try:
            service_map = self.dasmgr.mongoparser.service_apis_map(mongo_query)
            if  uinput.find('queries') != -1:
                pass
            elif  uinput.find('records') == -1 and not service_map:
                return 1, helper(\
                "None of the API's registered in DAS can resolve this query")
        except Exception as exc:
            print_exc(exc)
        return 0, mongo_query

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
        if  not instance:
            instance = self.dbs_global
        cards = self.templatepage('das_cards', base=self.base, show=cards, \
                width=900, height=220, cards=help_cards(self.base))
        page  = self.templatepage('das_searchform', input=uinput, \
                init_dbses=list(self.dbs_instances), \
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
    def gridfs(self, *args, **kwargs):
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
            data['status'] = 'success'
            data['data']   = fds.read()
        except Exception as exc:
            print_exc(exc)
            code = web_code('Exception')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        data['ctime'] = time.time() - time0
        return data

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
            coll     = getarg(kwargs, 'collection', 'merge')
            nresults = self.dasmgr.rawcache.nresults(query, coll)
            gen      = self.dasmgr.rawcache.get_from_cache\
                (query, idx=idx, limit=limit, collection=coll, adjust=False)
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

            form    = self.form(uinput="")
            ctime   = (time.time()-time0)
            page = self.page(form + page, ctime=ctime)
            return page
        except Exception as exc:
            print_exc(exc)
            return self.error(gen_error_msg(kwargs))

    @jsonstreamer
    def datastream(self, kwargs):
        """Stream DAS data into JSON format"""
        head = kwargs.get('head', request_headers())
        data = kwargs.get('data', [])
        return head, data

    def get_data(self, kwargs):
        """
        Invoke DAS workflow and get data from the cache.
        """
        head   = request_headers()
        head['args'] = kwargs
        uinput = getarg(kwargs, 'input', '') 
        inst   = kwargs.get('instance', self.dbs_global)
        if  inst:
            uinput = ' instance=%s %s' % (inst, uinput)
        idx    = getarg(kwargs, 'idx', 0)
        limit  = getarg(kwargs, 'limit', 0) # do not impose limit
        skey   = getarg(kwargs, 'skey', '')
        sdir   = getarg(kwargs, 'dir', 'asc')
        coll   = getarg(kwargs, 'collection', 'merge')
        time0  = time.time()
        try:
            mquery = self.dasmgr.mongoparser.parse(uinput, False) 
            nres   = self.dasmgr.in_raw_cache_nresults(mquery, coll)
            # if MongoDB fails during merging step revoke it again
            if  not nres:
                self.dasmgr.rawcache.merge_records(mquery)
                nres = self.dasmgr.in_raw_cache_nresults(mquery, coll)
            data   = self.dasmgr.result(mquery, idx, limit, skey, sdir)
            head.update({'status':'ok', 'nresults':nres, 
                         'mongo_query': mquery, 'ctime': time.time()-time0})
        except Exception as exc:
            print_exc(exc)
            head.update({'status': 'fail', 'reason': str(exc),
                         'ctime': time.time()-time0})
            data = []
        return head, data

    def busy(self):
        """
        Check number server load and report busy status if it's
        above threashold = queue size - nworkers
        """
        nrequests = self.reqmgr.size()
        if  (nrequests - self.taskmgr.nworkers()) > self.queue_limit:
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
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'
        self.adjust_input(kwargs)
        pid    = kwargs.get('pid', '')
        uinput = kwargs.get('input', '').strip()
        inst   = kwargs.get('instance', self.dbs_global)
        if  inst:
            uinput = ' instance=%s %s' % (inst, uinput)
        if  not pid and self.busy():
            data = []
            head = request_headers()
            head.update({'status': 'busy', 'reason': 'DAS server is busy',
                         'ctime': 0})
            return self.datastream(dict(head=head, data=data))
        if  pid:
            if  self.taskmgr.is_alive(pid):
                return pid
            else: # process is done, get data
                self.reqmgr.remove(pid)
                head, data = self.get_data(kwargs)
                return self.datastream(dict(head=head, data=data))
        else:
            addr = cherrypy.request.headers.get('Remote-Addr')
            _evt, pid = self.taskmgr.spawn(self.dasmgr.call, uinput, addr)
            self.reqmgr.add(pid, kwargs)
            return pid

    def get_page_content(self, kwargs):
        """Retrieve page content for provided set of parameters"""
        try:
            view = kwargs.get('view', 'list')
            if  view == 'plain':
                if  kwargs.has_key('limit'):
                    del kwargs['limit']
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
            mquery = self.dasmgr.mongoparser.parse(query, False) 
            data   = self.dasmgr.result(mquery, idx=0, limit=0)
        except Exception as exc:
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

    def adjust_input(self, kwargs):
        """
        Adjust user input if self.adjust flag is enable it. 
        This method can be customization for concrete DAS applications via
        external free_text_parser function (part of DAS.web.utils module)
        """
        if  not self.adjust:
            return
        uinput = kwargs.get('input', '')
        new_input = free_text_parser(uinput, self.daskeys)
        if  new_input == uinput:
            selkey = choose_select_key(uinput, self.daskeys, 'dataset')
            if  selkey and len(new_input) > len(selkey) and \
                new_input[:len(selkey)] != selkey:
                new_input = selkey + ' ' + new_input
        kwargs['input'] = new_input

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def request(self, **kwargs):
        """
        Request data from DAS cache.
        """
        # do not allow caching
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'

        time0   = time.time()
        self.adjust_input(kwargs)
        uinput  = getarg(kwargs, 'input', '').strip()
        inst    = kwargs.get('instance', self.dbs_global)
        view    = kwargs.get('view', 'list')
        form    = self.form(uinput=uinput, instance=inst, view=view)
        if  inst:
            uinput = ' instance=%s %s' % (inst, uinput)
        self.logdb(uinput)
        if  self.busy():
            return self.busy_page(uinput)
        check, content = self.check_input(uinput)
        if  check:
            if  view == 'list' or view == 'table':
                return self.page(form + content, ctime=time.time()-time0)
            else:
                return content
        mongo_query = content # check_input will return mongo_query upon success
        kwargs['query'] = mongo_query
        status = self.dasmgr.get_status(mongo_query)
        if  status == 'ok':
            page = self.get_page_content(kwargs)
        else: 
            addr = cherrypy.request.headers.get('Remote-Addr')
            _evt, pid = self.taskmgr.spawn(self.dasmgr.call, uinput, addr)
            self.reqmgr.add(pid, kwargs)
            if  self.taskmgr.is_alive(pid):
                # no data in raw cache
                img   = '<img src="%s/images/loading.gif" alt="loading"/>'\
                            % self.base
                page  = img + ' request PID=%s, please wait...' \
                            % pid
                page += ', <a href="/das/?%s">stop</a> request' \
                        % urllib.urlencode({'input':uinput})
                page += '<script type="text/javascript">'
                page += """setTimeout('ajaxCheckPid("%s", "%s", "%s")', %s)""" \
                        % (self.base, pid, self.interval, self.interval)
                page += '</script>'
            else:
                page = self.get_page_content(kwargs)
                self.reqmgr.remove(pid)
        ctime = (time.time()-time0)
        if  view == 'list' or view == 'table':
            return self.page(form + page, ctime=ctime)
        return page

    @expose
    def requests(self):
        """Return list of all current requests in DAS queue"""
        page = ""
        count = 0
        for row in self.reqmgr.items():
            page += '<li>%s placed at %s<br/>%s</li>' \
                        % (row['_id'], row['timestamp'], row['kwds'])
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
        Check status of given pid and return appropriate page content.
        This is a server callback function for ajaxCheckPid, see
        js/ajax_utils.js
        """
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'
        doc = self.reqmgr.get(pid)
        img = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
        if  doc:
            kwargs = doc
            if  self.taskmgr.is_alive(pid):
                page = img + " processing PID=%s" % pid
            else:
                page = self.get_page_content(kwargs)
                self.reqmgr.remove(pid)
        else:
            url = cherrypy.request.headers.get('Referer')
            if  url:
                kwargs = {}
                for key, val in \
                urlparse.parse_qsl(urlparse.urlparse(url).query):
                    kwargs[key] = val
                page = self.get_page_content(kwargs)
            else:
                page = "Request %s not found, please reload the page" % pid
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

