#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface, based on WMCore/WebTools
"""

__revision__ = "$Id: das_web.py,v 1.6 2010/05/03 19:49:33 valya Exp $"
__version__ = "$Revision: 1.6 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import re
import sys
import time
import thread
import urllib
import urllib2
import inspect
import cherrypy
import traceback

import yaml
from pprint import pformat

from itertools import groupby
from cherrypy import expose, HTTPError
from cherrypy.lib.static import serve_file
from pymongo.objectid import ObjectId

# DAS modules
import DAS
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_operators
from DAS.core.das_ply import das_parser_error
from DAS.utils.utils import getarg, access, size_format, DotDict, genkey
from DAS.utils.logger import DASLogger, set_cherrypy_logger
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection, db_gridfs
from DAS.utils.task_manager import TaskManager, PluginTaskManager
from DAS.web.utils import urllib2_request, json2html, web_time, quote
from DAS.web.utils import ajax_response, checkargs, get_ecode
from DAS.web.utils import wrap2dasxml, wrap2dasjson
from DAS.web.utils import dascore_monitor, yui2das, yui_name, gen_color
from DAS.web.tools import exposedasjson, exposetext
from DAS.web.tools import request_headers, jsonstreamer
from DAS.web.tools import exposejson, exposedasplist
from DAS.core.das_ql import das_aggregators, das_filters
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code

import DAS.utils.jsonwrapper as json

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'show', 'collection', 'name', 'dir',
                  'format', 'view', 'skey', 'query', 'fid', 'pid', 'next']

RE_DBSQL_0 = re.compile(r"^find")
RE_DBSQL_1 = re.compile(r"^find\s+(\w+)")
RE_DBSQL_2 = re.compile(r"^find\s+(\w+)\s+where\s+([\w.]+)\s*(=|in|like)\s*(.*)")
RE_DATASET = re.compile(r"^/\w+")
RE_SITE = re.compile(r"^T[0123]_")
RE_SUBKEY = re.compile(r"^([a-z_]+\.[a-zA-Z_]+)")
RE_KEYS = re.compile(r"""([a-z_]+)\s?(?:=|in|between|last)\s?(".*?"|'.*?'|[^\s]+)|([a-z_]+)""")
RE_COND_0 = re.compile(r"^([a-z_]+)")
RE_HASPIPE = re.compile(r"^.*?\|")
RE_PIPECMD = re.compile(r"^.*?\|\s*(\w+)$")
RE_AGGRECMD = re.compile(r"^.*?\|\s*(\w+)\(([\w.]+)$")
RE_FILTERCMD = re.compile(r"^.*?\|\s*(\w+)\s+(?:[\w.]+\s*,\s*)*([\w.]+)$")


DAS_PIPECMDS = das_aggregators() + das_filters()

def make_links(links, value):
    """
    Make new link for provided query links and passed value.
    """
    if  isinstance(value, list):
        values = value
    else:
        values = [value]
    for link in links:
        name, query = link.items()
        for val in values:
            dasquery = link['query'] % val
            uinput = urllib.quote(dasquery)
            url = '/das/request?input=%s' % uinput
            if  link['name']:
                key = link['name']
            else:
                key = val
            url = """<a href="%s">%s</a>""" % (quote(url), key)
            yield (key, val, url)

def add_filter_values(row, filters):
    """Add filter values for a given row"""
    page = ''
    if filters:
        for filter in filters:
            if  filter.find('<') == -1 and filter.find('>') == -1:
                val   = DotDict(row)._get(filter)
                page += "<b>%s:</b> %s<br />" % (filter, val)
    return page

def adjust_values(func, gen):
    """
    Helper function to adjust values in UI.
    It groups values for identical key, make links for provided mapped function,
    represent "Number of" keys as integers and represents size values in GB format.
    The mapped function is the one from das_mapping_db which convert
    UI key into triplet of das key, das access key and link, see 
    das_mapping_db:daskey_from_presentation
    """
    rdict = {}
    for uikey, value in [k for k, g in groupby(gen)]:
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
    links = {}
    keys = rdict.keys()
    if  keys:
        keys.sort()
#    for key, val in rdict.items():
    for key in keys:
        val = rdict[key]
        lookup = func(key)
        if  lookup:
            daskey, _, link = lookup
            if  daskey and link:
                value = val
                for kkk, vvv, lll in make_links(link, val):
                    if  not links.has_key(kkk):
                        links[kkk] = [(daskey, vvv, lll)]
                    else:
                        existing   = links[kkk]
                        links[kkk] = existing + [(daskey, vvv, lll)]

            elif  isinstance(val, list):
                if  isinstance(val[0], str) or isinstance(val[0], unicode):
                    value = ', '.join(val)
                else:
                    value = val
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
            page += "<b>%s:</b> %s<br />" % (key, value)
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
            page += "<b>%s:</b> %s<br />" % (key, val)
    if  links:
        page += "<b>Links:</b> "
        link_page = ""
        for key, val in links.items():
            if  len(val) == 1:
                _, _, link = val[0]
                if  link_page:
                    link_page += ', '
                link_page += link
            else:
                for item in val:
                    daskey, value, link = item
                    if  link_page:
                        link_page += ', '
                    link_page += "%s (%s=%s)" % (link, daskey, value)
        page += link_page + '<br />'
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
    def __init__(self, config={}):
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
        except:
            traceback.print_exc()
            self.dasmgr = None
            self.daskeys = []
            self.colors = {}

    def logdb(self, query):
        """
        Make entry in Logging DB
        """
        qhash = genkey(query)
        headers = cherrypy.request.headers
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
    @checkargs(DAS_WEB_INPUTS)
    def cli(self, *args, **kwargs):
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
        show   = kwargs.get('show', 'json')
        page   = "<b>DAS mapping record</b>"
        if  show == 'json':
            page += das_json(record)
        elif show == 'code':
            code  = pformat(record, indent=1, width=100)
            page += self.templatepage('das_json', jsoncode=code)
        else:
            code  = yaml.dump(record, width=100, indent=4, 
                        default_flow_style=False)
            page += self.templatepage('das_json', jsoncode=code)
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
            mongo_query = self.dasmgr.mongoparser.parse(uinput,\
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
            for key in self.daskeys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                msg = 'Provided input does not contain a valid DAS key'
                return 1, helper(uinput, msg)
        try:
            service_map = self.dasmgr.mongoparser.service_apis_map(mongo_query)
            if  uinput != 'records' and not service_map:
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
        return self.page(self.form())

    def form(self, input=None):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=input, base=self.base)
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
        fid = kwargs.get('fid')
        data.update({'status':'requested', 'fid':fid})
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
            format = ''
            if  args:
                recordid = args[0]
                spec = {'_id':ObjectId(recordid)}
                fields = None
                query = dict(fields=fields, spec=spec)
                if  len(args) == 2:
                    format = args[1]
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
            show     = getarg(kwargs, 'show', 'json')
            coll     = getarg(kwargs, 'collection', 'merge')
            nresults = self.dasmgr.rawcache.nresults(query, coll)
            gen      = self.dasmgr.rawcache.get_from_cache\
                (query, idx=idx, limit=limit, collection=coll, adjust=False)
            if  recordid: # we got id
                for row in gen:
                    if  show == 'json':
                        res += das_json(row)
                    elif show == 'code':
                        code  = pformat(row, indent=1, width=100)
                        res += self.templatepage('das_json', jsoncode=code)
                    else:
                        code = yaml.dump(row, width=100, indent=4, 
                                    default_flow_style=False)
                        res += self.templatepage('das_json', jsoncode=code)
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

            form    = self.form(input="")
            ctime   = (time.time()-time0)
            page = self.page(form + page, ctime=ctime)
            return page
        except:
            return self.error(self.gen_error_msg(kwargs))

    def convert2ui(self, idict):
        """
        Convert input row (dict) into UI presentation
        """
        for key in idict.keys():
            if  key == 'das' or key.find('_id') != -1:
                continue
            for item in self.dasmapping.presentation(key):
                try:
                    daskey = item['das']
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
        idx    = getarg(kwargs, 'idx', 0)
        limit  = getarg(kwargs, 'limit', 10)
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

    def busy_page(self, input=None):
        """DAS server busy page layout"""
        page = "<h3>DAS server is busy, please try later</h3>"
        form = self.form(input)
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
            head, data = self.get_data(kwargs)
            view = kwargs.get('view', 'list')
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
        self.logdb(uinput)
        if  self.busy():
            return self.busy_page(uinput)
        form    = self.form(input=uinput)
        view    = kwargs.get('view', 'list')
        check, content = self.check_input(uinput)
        if  check:
            if  view == 'list' or view == 'plain':
                return self.page(form + content, ctime=time.time()-time0)
            else:
                return content
        mongo_query = content # check_input will return mongo_query upon success
        kwargs['query'] = mongo_query
        status = self.dasmgr.get_status(mongo_query)
        if  status == 'ok':
            page = self.get_page_content(kwargs)
        elif view != 'list':
            msg  = 'You cannot request data in %s format before processing.\n'\
                        % view
            msg += 'Please choose list view and search for data first.'
            page = self.error(msg)
        else: 
            _evt, pid = self.taskmgr.spawn(self.dasmgr.call, uinput)
            self._requests[pid] = kwargs
            if  self.taskmgr.is_alive(pid):
                # no data in raw cache
                img   = '<img src="%s/images/loading.gif" alt="loading"/>'\
                            % self.base
                page  = img + ' request PID=%s, please wait...' \
                            % pid
                page += ', <a href="/das/">stop</a> request' 
                page += '<script type="application/javascript">'
                page += """setTimeout('ajaxCheckPid("%s", "%s", "%s")', %s)""" \
                        % (self.base, pid, self.next, self.next)
                page += '</script>'
            else:
                page = self.get_page_content(kwargs)
        ctime = (time.time()-time0)
        if  view == 'list' or view == 'plain':
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
            page = "Request %s not found" % pid
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

    def listview(self, head, data):
        """
        Helper function to make listview page.
        """
        kwargs  = head['args']
        total   = head['nresults']
        time0   = time.time()
        uinput  = getarg(kwargs, 'input', '').strip()
        idx     = getarg(kwargs, 'idx', 0)
        limit   = getarg(kwargs, 'limit', 10)
        show    = getarg(kwargs, 'show', 'json')
        query   = getarg(kwargs, 'query', {})
        filters = query.get('filters')
        page    = ''
        if  total > 0:
            params = {} # will keep everything except idx/limit
            for key, val in kwargs.items():
                if  key != 'idx' and key != 'limit':
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
            return self.templatepage('das_noresults', service_map=service_map)
        page  += self.templatepage('das_colors', colors=self.colors)
        style  = 'white'
        for row in data:
            id    = row['_id']
            page += '<div class="%s"><hr class="line" />' % style
            if  row.has_key('das'):
                if  row['das'].has_key('primary_key'):
                    pkey  = row['das']['primary_key']
                    page += '<b>DAS key:</b> %s<br />' % pkey.split('.')[0]
            gen   = self.convert2ui(row)
            if  self.dasmgr:
                func  = self.dasmgr.mapping.daskey_from_presentation
                page += add_filter_values(row, filters)
                page += adjust_values(func, gen)
            pad   = ""
            try:
                systems = self.systems(row['das']['system'])
            except:
                systems = "" # we don't store systems for aggregated records
            if  show == 'json':
                jsonhtml = das_json(row, pad)
                page += self.templatepage('das_row', systems=systems, \
                        sanitized_data=jsonhtml, id=id, rec_id=id)
            elif show == 'code':
                code  = pformat(row, indent=1, width=100)
                data  = self.templatepage('das_json', jsoncode=code)
                page += self.templatepage('das_row', systems=systems, \
                        sanitized_data=data, id=id, rec_id=id)
            else:
                code  = yaml.dump(row, width=100, indent=4, 
                                default_flow_style=False)
                data  = self.templatepage('das_json', jsoncode=code)
                page += self.templatepage('das_row', systems=systems, \
                        sanitized_data=data, id=id, rec_id=id)
            page += '</div>'
        page += '<div align="right">DAS cache server time: %5.3f sec</div>' \
                % head['ctime']
        return page

    def check_request4view(self, kwargs):
        """
        Check that request is suitable for given view
        """
        query = kwargs.get('input', None)
        if  query.find(' grep ') == -1:
            code = web_code('Query is not suitable for this view')
            raise HTTPError(500, 'DAS error, code=%s' % code)

    def records4filter(self, kwargs):
        """
        Retrieve records for given filter
        """
        query = kwargs.get('input', None)
        try:
            query = self.dasmgr.mongoparser.parse(query)
        except:
            code = web_code('DAS parser error')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        _head, data = self.get_data(kwargs)
        results  = ""
        for row in data:
            record = {}
            for filter in query['filters']:
                if  filter.find('=') != -1 or filter.find('>') != -1 or \
                    filter.find('<') != -1:
                    continue
                try:
                    record[yui_name(filter)] = DotDict(row)._get(filter)
                except:
                    pass
            yield record

    @exposejson
    @checkargs(DAS_WEB_INPUTS)
    def table_records(self, **kwargs):
        """
        Provide JSON in YUI compatible format to be used in DynamicData table
        widget, see
        http://developer.yahoo.com/yui/examples/datatable/dt_dynamicdata.html
        """
        uinput = kwargs.get('input', '').strip()
        check, query = self.check_input(uinput)
        if  check:
            code = web_code('Server error')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        kwargs['query'] = query
        if  kwargs.has_key('skey'):
            kwargs['skey'] = yui2das(kwargs['skey'])
        rowlist  = [record for record in self.records4filter(kwargs)]
        idx      = getarg(kwargs, 'idx', 0)
        limit    = getarg(kwargs, 'limit', 10)
        head, _data = self.get_data(kwargs)
        total    = head['nresults']
        jsondict = {'recordsReturned': len(rowlist),
                   'totalRecords': total, 'startIndex':idx,
                   'sort':'true', 'dir':'asc',
                   'pageSize': limit,
                   'records': rowlist}
        return jsondict

    def tableview(self, head, data):
        """
        Represent data in tabular view.
        """
        kwargs  = head['args']
        total   = head['nresults']
        self.check_request4view(kwargs)
        query   = kwargs['query']
        if  not query.has_key('filters'):
            code = web_code('Query is not suitable for this view')
            raise HTTPError(500, 'DAS error, code=%s' % code)
        titles  = []
        for filter in query['filters']:
            if  filter.find('=') != -1 or filter.find('>') != -1 or \
                filter.find('<') != -1:
                continue
            titles.append(yui_name(filter))
        time0   = time.time()
        uinput  = getarg(kwargs, 'input', '').strip()
        limit   = getarg(kwargs, 'limit', 10)
        form    = self.form(input=uinput)
        coldefs = ""
        for title in titles:
            coldefs += '{key:"%s",label:"%s",sortable:true,resizeable:true},' \
                        % (title, title)
        coldefs = "[%s]" % coldefs[:-1] # remove last comma
        coldefs = coldefs.replace("},{","},\n{")
        names   = {'titlelist':titles, 'base': self.base,
                   'coldefs':coldefs, 'rowsperpage':limit,
                   'total':total, 'tag':'mytag',
                   'input':urllib.urlencode(dict(input=uinput))}
        page    = self.templatepage('das_table', **names)
        ctime   = (time.time()-time0)
        page    = self.page(form + page, ctime=ctime)
        return page

    @exposetext
    def filterview(self, kwargs):
        """
        provide DAS plain view for queries with filters
        """
        self.check_request4view(kwargs)
        results = ""
        for record in self.records4filter(kwargs):
            for val in record.values():
                results += val
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

    @exposetext
    def plainview(self, head, data):
        """
        provide DAS plain view
        """
        page = ""
        for item in data:
            item  = str(item).replace('[','').replace(']','')
            page += "%s\n" % item.replace("'","")
        return page

    @exposedasjson
    @checkargs(['query'])
    def autocomplete(self, **kwargs):
        """
        Interface to the DAS keylearning system, for a 
        as-you-type suggestion system. This is a call for AJAX
        in the page rather than a user-visible one.
        
        This returns a list of JS objects, formatted like:
        {'css': '<ul> css class', 'value': 'autocompleted text',
         'info': '<html> text'}
         
        Some of the work done here could be moved client side, and
        only calls that actually require keylearning look-ups
        forwarded. Given the number of REs used, this may be necessary
        if load increases.
        """
        
        query = kwargs.get("query", "").strip()
        result = []
        if RE_DBSQL_0.match(query):
            #find...
            match1 = RE_DBSQL_1.match(query) 
            match2 = RE_DBSQL_2.match(query)
            if match1:
                daskey = match1.group(1)
                if daskey in self.daskeys:
                    if match2:
                        operator = match2.group(3)
                        value = match2.group(4)
                        if operator == '=' or operator == 'like':
                            result.append({'css': 'ac-warinig sign', 'value':'%s=%s' % (daskey, value),
                                           'info': "This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key, and the condition should <b>probably</b> be expressed like this." % (daskey)})
                        else:
                            result.append({'css': 'ac-warinig sign', 'value':daskey,
                                           'info': "This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key. However, I'm not sure how to interpret the condition (<b>%s %s<b>)." % (daskey, operator, value)})
                    else:
                        result.append({'css': 'ac-warinig sign', 'value': daskey,
                                       'info': 'This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key.' % daskey})
                else:
                    result.append({'css': 'ac-error sign', 'value': '',
                                   'info': "This appears to be a DBS-QL query, and the key (<b>%s</b>) isn't known to DAS." % daskey})
                    
                    key_search = self.dasmgr.keylearning.key_search(daskey)
                    #do a key search, and add info elements for them here
                    for keys, members in key_search.items():
                        result.append({'css': 'ac-info', 'value': ' '.join(keys),
                                       'info': 'Possible keys <b>%s</b> (matching %s).' % (', '.join(keys), ', '.join(members))})
                    if not key_search:
                        result.append({'css': 'ac-error sign', 'value': '',
                                       'info': 'No matches found for <b>%s</b>.' % daskey})
                        
                    
            else:
                result.append({'css': 'ac-error sign', 'value': '',
                               'info': 'This appears to be a DBS-QL query. DAS queries are of the form <b>key</b><span class="faint">[ operator value]</span>'})
        elif RE_DATASET.match(query):
            #/something...
            result.append({'css': 'ac-warinig sign', 'value':'dataset=%s' % query,
                           'info':'''This appears to be a dataset query. The correct syntax is <b>dataset=/some/dataset</b> <span class="faint">| grep dataset.<i>field</i></span>'''})
        elif RE_SITE.match(query):
            #T{0123}_...
            result.append({'css': 'ac-warinig sign', 'value':'site=%s' % query,
                           'info':'''This appears to be a site query. The correct syntax is <b>site=TX_YY_ZZZ</b> <span class="faint">| grep site.<i>field</i></span>'''})    
        elif RE_HASPIPE.match(query):
            keystr = query.split('|')[0]
            keys = set()
            for keymatch in RE_KEYS.findall(keystr):
                if keymatch[0]:
                    keys.add(keymatch[0])
                else:
                    keys.add(keymatch[2])
            keys = list(keys)
            if not keys:
                result.append({'css':'ac-error sign', 'value': '',
                               'info': "You seem to be trying to write a pipe command without any keys."})
            
            pipecmd = RE_PIPECMD.match(query)
            filtercmd = RE_FILTERCMD.match(query)
            aggrecmd = RE_AGGRECMD.match(query)
            
            if pipecmd:
                cmd = pipecmd.group(1)
                precmd = query[:pipecmd.start(1)]
                matches = filter(lambda x: x.startswith(cmd), DAS_PIPECMDS)
                if matches:
                    for match in matches:
                        result.append({'css': 'ac-info', 'value': '%s%s' % (precmd, match),
                                       'info': 'Function match <b>%s</b>' % (match)})
                else:
                    result.append({'css': 'ac-warinig sign', 'value': precmd,
                                   'info': 'No aggregation or filter functions match <b>%s</b>.' % cmd})
            elif aggrecmd:
                cmd = aggrecmd.group(1)
                if not cmd in das_aggregators():
                    result.append({'css':'ac-error sign', 'value': '',
                                   'info': 'Function <b>%s</b> is not a known DAS aggregator.' % cmd})
                
            elif filtercmd:
                cmd = filtercmd.group(1)
                if not cmd in das_filters():
                    result.append({'css':'ac-error sign', 'value': '',
                                   'info': 'Function <b>%s</b> is not a known DAS filter.' % cmd})
            
            if aggrecmd or filtercmd:
                match = aggrecmd if aggrecmd else filtercmd
                subkey = match.group(2)
                prekey = query[:match.start(2)]
                members = self.dasmgr.keylearning.members_for_keys(keys)
                if members:
                    matches = filter(lambda x: x.startswith(subkey), members)
                    if matches:
                        for match in matches:
                            result.append({'css': 'ac-info', 'value': prekey+match,
                                           'info': 'Possible match <b>%s</b>' % match})
                    else:
                        result.append({'css': 'ac-warinig sign', 'value': prekey,
                                       'info': 'No data members match <b>%s</b> (but this could be a gap in keylearning coverage).' % subkey})
                else:
                    result.append({'css': 'ac-warinig sign', 'value': prekey,
                                   'info': 'No data members found for keys <b>%s</b> (but this might be a gap in keylearning coverage).' % ' '.join(keys)})
                
            
        elif RE_SUBKEY.match(query):
            subkey = RE_SUBKEY.match(query).group(1)
            daskey = subkey.split('.')[0]
            if daskey in self.daskeys:
                if self.dasmgr.keylearning.has_member(subkey):
                    result.append({'css': 'ac-warinig sign', 'value': '%s | grep %s' % (daskey, subkey),
                                   'info': 'DAS queries should start with a top-level key. Use <b>grep</b> to see output for one data member.'})
                else:
                    result.append({'css': 'ac-warinig sign', 'value': '%s | grep %s' % (daskey, subkey),
                                   'info': "DAS queries should start with a top-level key. Use <b>grep</b> to see output for one data member. DAS doesn't know anything about the <b>%s</b> member but keylearning might be incomplete." % (subkey)})
                    key_search = self.dasmgr.keylearning.key_search(subkey, daskey)
                    for keys, members in key_search.items():
                        for member in members:
                            result.append({'css': 'ac-info', 'value':'%s | grep %s' % (daskey, member),
                                           'info': 'Possible member match <b>%s</b> (for daskey <b>%s</b>)' % (member, daskey)})
                    if not key_search:
                        result.append({'css': 'ac-error sign', 'value': '',
                                       'info': 'No matches found for <b>%s</b>.' % (subkey)})
                            
            else:
                result.append({'css': 'ac-error sign', 'value': '',
                               'info': "Das queries should start with a top-level key. <b>%s</b> is not a valid DAS key." % daskey})
                key_search = self.dasmgr.keylearning.key_search(subkey)
                for keys, members in key_search.items():
                    result.append({'css': 'ac-info', 'value': ' '.join(keys),
                                   'info': 'Possible keys <b>%s</b> (matching <b>%s</b>).' % (', '.join(keys), ', '.join(members))})
                if not key_search:
                    result.append({'css': 'ac-error sign', 'value': '',
                                   'info': 'No matches found for <b>%s</b>.' % subkey})
                    
        elif RE_KEYS.match(query):
            keys = set()
            for keymatch in RE_KEYS.findall(query):
                if keymatch[0]:
                    keys.add(keymatch[0])
                else:
                    keys.add(keymatch[2])
            for key in keys:
                if not key in self.daskeys:
                    result.append({'css':'ac-error sign', 'value': '',
                                   'info': 'Key <b>%s</b> is not known to DAS.' % key})
                    key_search = self.dasmgr.keylearning.key_search(query)
                    for keys, members in key_search.items():
                        result.append({'css': 'ac-info', 'value': ' '.join(keys),
                                       'info': 'Possible keys <b>%s</b> (matching <b>%s</b>).' % (', '.join(keys), ', '.join(members))})
                    if not key_search:
                        result.append({'css': 'ac-error sign', 'value': '',
                                       'info': 'No matches found for <b>%s</b>.' % query})
        else:
            #we've no idea what you're trying to accomplish, do a search
            key_search = self.dasmgr.keylearning.key_search(query)
            for keys, members in key_search.items():
                result.append({'css': 'ac-info', 'value': ' '.join(keys),
                               'info': 'Possible keys <b>%s</b> (matching <b>%s</b>).' % (', '.join(keys), ', '.join(members))})
            if not key_search:
                result.append({'css': 'ac-error sign', 'value': '',
                               'info': 'No matches found for <b>%s</b>.' % query})
            
        return result
