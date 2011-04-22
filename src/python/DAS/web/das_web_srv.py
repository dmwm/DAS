#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface, based on WMCore/WebTools
"""

__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import sys
import time
import thread
import urllib
import cherrypy
import traceback

from itertools import groupby
from cherrypy import expose, HTTPError
from cherrypy.lib.static import serve_file
from pymongo.objectid import ObjectId

# DAS modules
import DAS
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_operators, das_filters
from DAS.core.das_ply import das_parser_error
from DAS.utils.utils import getarg, access, size_format, DotDict, genkey
from DAS.utils.logger import DASLogger, set_cherrypy_logger
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection, db_gridfs
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.web.utils import json2html, web_time, quote
from DAS.web.utils import ajax_response, checkargs
from DAS.web.utils import dascore_monitor, gen_color
from DAS.web.tools import exposedasjson, exposetext
from DAS.web.tools import request_headers, jsonstreamer
from DAS.web.tools import exposedasplist
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code
from DAS.web.autocomplete import autocomplete_helper

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'collection', 'name', 'dir', 
        'instance', 'format', 'view', 'skey', 'query', 'fid', 'pid', 'next']
DAS_PIPECMDS = das_aggregators() + das_filters()

def make_links(links, value, inst):
    """
    Make new link for provided query links and passed value.
    """
    if  isinstance(value, list):
        values = value
    else:
        values = [value]
    for link in links:
        for val in values:
            if  link.has_key('query'):
                dasquery = link['query'] % val
                uinput = urllib.quote(dasquery)
                url = '/das/request?input=%s&instance=%s&idx=0&limit=10' \
                            % (uinput, inst)
                if  link['name']:
                    key = link['name']
                else:
                    key = val
                url = """<a href="%s">%s</a>""" % (quote(url), key)
                yield url
            elif link.has_key('link'):
                if  link['name']:
                    key = link['name']
                else:
                    key = val
                url = link['link'] % val
                url = """<a href="%s">%s</a>""" % (quote(url), key)
                yield url

def add_filter_values(row, filters):
    """Add filter values for a given row"""
    page = ''
    if filters:
        for flt in filters:
            if  flt.find('<') == -1 and flt.find('>') == -1:
                values = set([str(r) for r in DotDict(row).get_values(flt)])
                val = ', '.join(values)
                if  val:
                    if  flt.lower() == 'run.run_number':
                        if  isinstance(val, str) or isinstance(val, unicode):
                            val = int(val.split('.')[0])
                    page += "<br />Filter <b>%s:</b> %s" % (flt, val)
                else:
                    page += "<br />Filter <b>%s</b>" % flt
    return page

def adjust_values(func, gen, links):
    """
    Helper function to adjust values in UI.
    It groups values for identical key, make links for provided mapped function,
    represent "Number of" keys as integers and represents size values in GB format.
    The mapped function is the one from das_mapping_db which convert
    UI key into triplet of das key, das access key and link, see 
    das_mapping_db:daskey_from_presentation
    """
    rdict = {}
    for uikey, value in [k for k, _g in groupby(gen)]:
        val = quote(value)
        if  rdict.has_key(uikey):
            existing_val = rdict[uikey]
            if  not isinstance(existing_val, list):
                existing_val = [existing_val]
            if  val not in existing_val:
                rdict[uikey] = existing_val + [val]
        else:
            rdict[uikey] = val
    page = ""
    to_show = []
    for key, val in rdict.items():
        lookup = func(key)
        if  lookup:
            if  isinstance(val, list):
                value = ', '.join([str(v) for v in val])
            elif  key.lower().find('size') != -1 and val:
                value = size_format(val)
            elif  key.find('Number of ') != -1 and val:
                value = int(val)
            elif  key.find('Run number') != -1 and val:
                value = int(val)
            else:
                value = val
            if  isinstance(value, list) and isinstance(value[0], str):
                value = ', '.join(value)
            to_show.append((key, value))
        else:
            if  key == 'result' and isinstance(val, dict) and \
                val.has_key('value'): # result of aggregation function
                if  rdict.has_key('key') and \
                    rdict['key'].find('.size') != -1:
                    val = size_format(val['value'])
                elif isinstance(val['value'], float):
                    val = '%.2f' % val['value']
                else:
                    val = val['value']
            to_show.append((key, val))
    if  to_show:
        page += '<br />'
        page += ', '.join(["<b>%s</b>: %s" % (k, v) for k, v in to_show])
    if  links:
        page += '<br />' + links
    return page

def das_json(record, pad=''):
    """
    Wrap provided jsonhtml code snippet into div/pre blocks. Provided jsonhtml
    snippet is sanitized by json2html function.
    """
    page  = """<div class="code"><pre>"""
    page += json2html(record, pad)
    page += "</pre></div>"
    return page

class DASWebService(DASWebManager):
    """
    DAS web service interface.
    """
    def __init__(self, config):
        DASWebManager.__init__(self, config)
        self.base   = config['url_base']
        self.next   = 3000 # initial next update status in miliseconds
        self.engine = config.get('engine', None)
        logfile     = config['logfile']
        loglevel    = config['loglevel']
        nworkers    = config['number_of_workers']
        self.logger = DASLogger(logfile=logfile, verbose=loglevel)
        set_cherrypy_logger(self.logger.handler, loglevel)
        msg = "DASSearch::init is started with base=%s" % self.base
        self.logger.info(msg)
        self.dasconfig   = das_readconfig()
        self.dburi       = self.dasconfig['mongodb']['dburi']
        self.queue_limit = config.get('queue_limit', 50)
        self._requests   = {} # to hold incoming requests pid/kwargs
        if  self.engine:
            self.taskmgr = PluginTaskManager(bus=self.engine, nworkers=nworkers)
            self.taskmgr.subscribe()
        else:
            self.taskmgr = TaskManager(nworkers=nworkers)

        self.init()
        # Monitoring thread which performs auto-reconnection
        thread.start_new_thread(dascore_monitor, \
                ({'das':self.dasmgr, 'uri':self.dburi}, self.init, 5))

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
            self.dasmgr     = DASCore(engine=self.engine)
            self.daskeys    = self.dasmgr.das_keys()
            self.gfs        = db_gridfs(self.dburi)
            self.daskeys.sort()
            self.dasmapping = self.dasmgr.mapping
            self.colors = {}
            for system in self.dasmgr.systems:
                self.colors[system] = gen_color(system)
        except Exception, _exp:
            traceback.print_exc()
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
        def helper(myinput, msg):
            """Helper function which provide error template"""
            guide = self.templatepage('dbsql_vs_dasql', 
                        operators=', '.join(das_operators()))
            page = self.templatepage('das_ambiguous', msg=msg, base=self.base,
                        input=myinput, guide=guide,
                        entities=', '.join(self.daskeys),
                        operators=', '.join(das_operators()))
            return page
        if  not uinput:
            return 1, helper(uinput, 'No input query')
        # check provided input. If at least one word is not part of das_keys
        # return ambiguous template.
        try:
            mongo_query = self.dasmgr.mongoparser.parse(uinput, \
                                add_to_analytics=False)
        except Exception, err:
            return 1, helper(uinput, das_parser_error(uinput, str(err)))
        fields = mongo_query.get('fields', [])
        if  not fields:
            fields = []
        spec   = mongo_query.get('spec', {})
        if  not fields+spec.keys():
            msg = 'Provided input does not resolve into a valid set of keys'
            return 1, helper(uinput, msg)
        for word in fields+spec.keys():
            found = 0
            if  word == 'queries':
                found = 1
            for key in self.daskeys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                msg = 'Provided input does not contain a valid DAS key'
                return 1, helper(uinput, msg)
        try:
            service_map = self.dasmgr.mongoparser.service_apis_map(mongo_query)
            if  uinput.find('queries') != -1:
                pass
            elif  uinput.find('records') == -1 and not service_map:
                return 1, helper(uinput, \
                "None of the API's registered in DAS can resolve this query")
        except:
            traceback.print_exc()
            pass
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
        return self.page(self.form(uinput=uinput))

    def form(self, uinput=None, instance='cms_dbs_prod_global', view='list'):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=uinput, \
                base=self.base, instance=instance, view=view)
        return page

    def gen_error_msg(self, kwargs):
        """
        Generate standard error message.
        """
        self.logger.error(traceback.format_exc())
        error  = "My request to DAS is failed\n\n"
        error += "Input parameters:\n"
        for key, val in kwargs.items():
            error += '%s: %s\n' % (key, val)
        error += "Exception type: %s\nException value: %s\nTime: %s" \
                    % (sys.exc_info()[0], sys.exc_info()[1], web_time())
        error = error.replace("<", "").replace(">", "")
        return error

    @expose
    def error(self, msg, wrap=True):
        """
        Show error message.
        """
        page = self.templatepage('das_error', msg=msg)
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
        except:
            self.logger.error(traceback.format_exc())
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
        except:
            return self.error(self.gen_error_msg(kwargs))

    def convert2ui(self, idict, not2show=None):
        """
        Convert input row (dict) into UI presentation
        """
        for key in idict.keys():
            if  key == 'das' or key.find('_id') != -1:
                continue
            for item in self.dasmapping.presentation(key):
                try:
                    daskey = item['das']
                    if  not2show and not2show == daskey:
                        continue
                    uikey  = item['ui']
                    for value in access(idict, daskey):
                        yield uikey, value
                except:
                    yield key, idict[key]

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
        inst   = kwargs.get('instance', 'cms_dbs_prod_global')
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
            data   = self.dasmgr.result(mquery, idx, limit, skey, sdir)
            nres   = self.dasmgr.in_raw_cache_nresults(mquery, coll)
            head.update({'status':'ok', 'nresults':nres, 
                         'mongo_query': mquery, 'ctime': time.time()-time0})
        except Exception, exp:
            traceback.print_exc()
            head.update({'status': 'fail', 'reason': str(exp),
                         'ctime': time.time()-time0})
            data = []
        return head, data

    def busy(self):
        """
        Check number server load and report busy status if it's
        above threashold = queue size - nworkers
        """
        nrequests = len(self._requests.keys())
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
        pid    = kwargs.get('pid', '')
        uinput = kwargs.get('input', '').strip()
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
                try:
                    del self._requests[pid]
                except:
                    pass
                head, data = self.get_data(kwargs)
                return self.datastream(dict(head=head, data=data))
        else:
            _evt, pid = self.taskmgr.spawn(self.dasmgr.call, uinput)
            self._requests[pid] = kwargs
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
        except HTTPError, _err:
            raise 
        except:
            traceback.print_exc()
            msg   = 'Wrong view. '
            msg  += self.gen_error_msg(kwargs)
            page  = self.templatepage('das_error', msg=msg)
        return page

    @expose
    def makepy(self, dataset):
        """
        Request to create CMSSW py snippet for a given dataset
        """
        pat = re.compile('/.*/.*/.*')
        if  not pat.match(dataset):
            msg = 'Invalid dataset name'
            return self.error(msg)
        query = "file dataset=%s | grep file.name" % dataset
        try:
            mquery = self.dasmgr.mongoparser.parse(query, False) 
            data   = self.dasmgr.result(mquery, idx=0, limit=0)
        except Exception, _exp:
            msg    = 'Unable to retrieve data for query=%s' % query
            return self.error(msg)
        lfns = []
        for rec in data:
            filename = DotDict(rec)._get('file.name')
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
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'

        time0   = time.time()
        uinput  = getarg(kwargs, 'input', '').strip()
        inst    = kwargs.get('instance', 'cms_dbs_prod_global')
        if  inst:
            uinput = ' instance=%s %s' % (inst, uinput)
        self.logdb(uinput)
        if  self.busy():
            return self.busy_page(uinput)
        view    = kwargs.get('view', 'list')
        form    = self.form(uinput=uinput, instance=inst, view=view)
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
            _evt, pid = self.taskmgr.spawn(self.dasmgr.call, uinput)
            self._requests[pid] = kwargs
            if  self.taskmgr.is_alive(pid):
                # no data in raw cache
                img   = '<img src="%s/images/loading.gif" alt="loading"/>'\
                            % self.base
                page  = img + ' request PID=%s, please wait...' \
                            % pid
                page += ', <a href="/das/?%s">stop</a> request' \
                        % urllib.urlencode({'input':uinput})
                page += '<script type="application/javascript">'
                page += """setTimeout('ajaxCheckPid("%s", "%s", "%s")', %s)""" \
                        % (self.base, pid, self.next, self.next)
                page += '</script>'
            else:
                page = self.get_page_content(kwargs)
        ctime = (time.time()-time0)
        if  view == 'list' or view == 'table':
            return self.page(form + page, ctime=ctime)
        return page

    @expose
    def requests(self):
        """Return list of all current requests in DAS queue"""
        page = ""
        for pid, kwds in self._requests.items():
            page += '<li>%s<br/>%s</li>' % (pid, kwds)
        if  page:
            page = "<ul>%s</ul>" % page
        else:
            page = "The request queue is empty"
        return self.page(page)

    @expose
    @checkargs(['pid', 'next'])
    def check_pid(self, pid, next):
        """
        Place AJAX request to obtain status about given process pid.
        """
        limit = 30000 # 1 minute, max check status limit
        next  = int(next)
        if  next < limit and next*2 < limit:
            next *= 2
        else:
            next = limit
        img  = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
        req  = """
        <script type="application/javascript">
        setTimeout('ajaxCheckPid("%s", "%s", "%s")', %s)
        </script>""" % (self.base, pid, next, next)
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'
        if  self._requests.has_key(pid):
            kwargs = self._requests[pid]
            if  self.taskmgr.is_alive(pid):
                sec   = next/1000
                page  = img + " processing PID=%s, " % pid
                page += "next check in %s sec, please wait ..." % sec
                page += ', <a href="/das/">stop</a> request' 
                page += req
            else:
                page  = self.get_page_content(kwargs)
                try:
                    del self._requests[pid]
                except:
                    pass
        else:
            page = "Request %s not found, please reload the page" % pid
        page = ajax_response(page)
        return page
    
    def systems(self, slist):
        """Colorize provided sub-systems"""
        page = ""
        if  not self.colors:
            return page
        pads = "padding-left:7px; padding-right:7px"
        for system in slist:
            page += '<span style="background-color:%s;%s">&nbsp;</span>' \
                % (self.colors[system], pads)
        return page

    def sort_dict(self, titles, pkey):
        """Return dict of daskey/mapkey for given list of titles"""
        tdict = {}
        for uikey in titles:
            pdict = self.dasmapping.daskey_from_presentation(uikey)
            if  pdict and pdict.has_key(pkey):
                mapkey = pdict[pkey]['mapkey']
            else:
                mapkey = uikey
            tdict[uikey] = mapkey
        return tdict

    def pagination(self, total, kwargs):
        """
        Consutruct pagination part of the page.
        """
        idx     = getarg(kwargs, 'idx', 0)
        limit   = getarg(kwargs, 'limit', 10)
        query   = getarg(kwargs, 'query', {})
        page    = ''
        if  total > 0:
            params = {} # will keep everything except idx/limit
            for key, val in kwargs.items():
                if  key != 'idx' and key != 'limit' and key != 'query':
                    params[key] = val
            url   = "%s/request?%s" \
                    % (self.base, urllib.urlencode(params, doseq=True))
            page += self.templatepage('das_pagination', \
                nrows=total, idx=idx, limit=limit, url=url)
        else:
            try:
                del query['spec']['das.primary_key'] # this is used for look-up
            except:
                pass
            service_map = self.dasmgr.mongoparser.service_apis_map(query)
            page = self.templatepage('das_noresults', service_map=service_map)
        return page

    def listview(self, head, data):
        """
        Helper function to make listview page.
        """
        kwargs  = head['args']
        total   = head['nresults']
        inst    = getarg(kwargs, 'instance', 'cms_dbs_prod_global')
        query   = getarg(kwargs, 'query', {})
        filters = query.get('filters')
        page    = self.pagination(total, kwargs)
        page  += self.templatepage('das_colors', colors=self.colors)
        style  = 'white'
        for row in data:
            try:
                mongo_id = row['_id']
            except:
                msg = 'Fail to process row\n%s' % str(row)
                raise Exception(msg)
            page += '<div class="%s"><hr class="line" />' % style
            links = ""
            pkey  = None
            lkey  = None
            if  row.has_key('das') and row['das'].has_key('primary_key'):
                pkey = row['das']['primary_key']
                lkey = pkey.split('.')[0]
                try:
                    pval = DotDict(row)._get(pkey)
                    if  pkey == 'run.run_number':
                        pval = int(pval)
                    ifield  = urllib.urlencode(\
                        {'input':'%s=%s' % (lkey, pval), 'instance':inst})
                    if  pval:
                        page += '<b>%s</b>: <a href="/das/request?%s">%s</a>'\
                                    % (lkey.capitalize(), ifield, pval)
                    else:
                        page += '<b>%s</b>: N/A' % lkey.capitalize()
                    plist = self.dasmgr.mapping.presentation(lkey)
                    linkrec = None
                    for item in plist:
                        if  item.has_key('link'):
                            linkrec = item['link']
                            break
                    if  linkrec and pval and pval != 'N/A':
                        links = ', '.join(make_links(linkrec, pval, inst))\
                                    + '.'
                except:
                    pval = 'N/A'
            gen   = self.convert2ui(row, pkey)
            if  self.dasmgr:
                func  = self.dasmgr.mapping.daskey_from_presentation
                page += add_filter_values(row, filters)
                page += adjust_values(func, gen, links)
            pad   = ""
            try:
                systems = self.systems(row['das']['system'])
                if  row['das']['system'] == ['combined'] or \
                    row['das']['system'] == [u'combined']:
                    if  lkey:
                        systems = self.systems(row[lkey]['combined'])
            except KeyError:
                systems = "" # we don't store systems for aggregated records
            except:
                traceback.print_exc()
                systems = "" # we don't store systems for aggregated records
            jsonhtml = das_json(row, pad)
            if  not links:
                page += '<br />'
            page += self.templatepage('das_row', systems=systems, \
                    sanitized_data=jsonhtml, id=mongo_id, rec_id=mongo_id)
            page += '</div>'
        page += '<div align="right">DAS cache server time: %5.3f sec</div>' \
                % head['ctime']
        return page

    def tableview(self, head, data):
        """
        Represent data in tabular view.
        """
        kwargs  = head['args']
        total   = head['nresults']
        uinput  = getarg(kwargs, 'input', '').strip()
        idx     = getarg(kwargs, 'idx', 0)
        limit   = getarg(kwargs, 'limit', 10)
        sdir    = getarg(kwargs, 'dir', '')
        inst    = getarg(kwargs, 'instance', 'cms_dbs_prod_global')
        query   = kwargs['query']
        titles  = []
        page    = self.pagination(total, kwargs)
        if  query.has_key('filters'):
            for flt in query['filters']:
                if  flt.find('=') != -1 or flt.find('>') != -1 or \
                    flt.find('<') != -1:
                    continue
                titles.append(flt)
        style   = 1
        tpage   = ""
        pkey    = None
        for row in data:
            rec  = []
            if  not pkey and row.has_key('das') and \
                row['das'].has_key('primary_key'):
                pkey = row['das']['primary_key'].split('.')[0]
            if  query.has_key('filters'):
                for flt in query['filters']:
                    rec.append(DotDict(row)._get(flt))
            else:
                gen = self.convert2ui(row)
                titles = []
                for uikey, val in gen:
                    skip = 0
                    if  not query.has_key('filters'):
                        if  uikey in titles:
                            skip = 1
                        else:
                            titles.append(uikey)
                    if  not skip:
                        rec.append(val)
            if  style:
                style = 0
            else:
                style = 1
            link = '<a href="/das/records/%s?collection=merge">link</a>' \
                        % quote(str(row['_id'])) # cgi.escape the id
            tpage += self.templatepage('das_table_row', rec=rec, tag='td', \
                        style=style, encode=1, record=link)
        sdict  = self.sort_dict(titles, pkey)
        if  sdir == 'asc':
            sdir = 'desc'
        elif sdir == 'desc':
            sdir = 'asc'
        else: # default sort direction
            sdir = 'asc' 
        args   = {'input':uinput, 'idx':idx, 'limit':limit, 'instance':inst, \
                         'view':'table', 'dir': sdir}
        theads = []
        for title in titles:
            args.update({'skey':sdict[title]})
            url = '<a href="/das/request?%s">%s</a>' \
                % (urllib.urlencode(args), title)
            theads.append(url)
        theads.append('Record')
        thead = self.templatepage('das_table_row', rec=theads, tag='th', \
                        style=0, encode=0, record=0)
        self.sort_dict(titles, pkey)
        page += '<br />'
        page += '<table class="das_table">' + thead + tpage + '</table>'
        page += '<br />'
        page += '<div align="right">DAS cache server time: %5.3f sec</div>' \
                % head['ctime']
        return page

    @exposetext
    def plainview(self, head, data):
        """
        provide DAS plain view for queries with filters
        """
        query   = head['args']['query']
        fields  = query.get('fields', None)
        filters = query.get('filters', None)
        results = ""
        for row in data:
            if  filters:
                for flt in filters:
                    if  flt.find('=') != -1 or flt.find('>') != -1 or \
                        flt.find('<') != -1:
                        continue
                    try:
                        for obj in DotDict(row).get_values(flt):
                            results += str(obj) + '\n'
                    except:
                        pass
                results += '\n'
            else:
                for item in fields:
                    systems = self.dasmgr.systems
                    mapkey  = self.dasmapping.find_mapkey(systems[0], item)
                    try:
                        if  not mapkey:
                            mapkey = '%s.name' % item
                        key, att = mapkey.split('.')
                        if  row.has_key(key):
                            val = row[key]
                            if  isinstance(val, dict):
                                results += val.get(att, '')
                            elif isinstance(val, list):
                                for item in val:
                                    results += item.get(att, '')
                                    results += '\n'
                    except:
                        pass
                results += '\n'

        return results

    @exposedasplist
    def xmlview(self, head, data):
        """
        provide DAS XML
        """
        result = dict(head)
        result['data'] = [r for r in data]
        return result

    @exposedasjson
    def jsonview(self, head, data):
        """
        provide DAS JSON
        """
        result = dict(head)
        result['data'] = [r for r in data]
        return result

    @exposedasjson
    @checkargs(['query'])
    def autocomplete(self, **kwargs):
        """
        Provides autocomplete functionality for DAS web UI.
        """
        query = kwargs.get("query", "").strip()
        return autocomplete_helper(query, self.dasmgr, self.daskeys)
