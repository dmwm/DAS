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

# DAS modules
import DAS
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_operators
from DAS.utils.utils import getarg, access, size_format
from DAS.utils.logger import DASLogger, set_cherrypy_logger
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection, connection_monitor
from DAS.web.utils import urllib2_request, json2html, web_time, quote
from DAS.web.utils import ajax_response, checkargs, get_ecode
from DAS.web.utils import wrap2dasxml, wrap2dasjson
from DAS.web.tools import exposedasjson, exposetext
from DAS.web.tools import exposejson, exposedasplist
from DAS.core.das_ql import das_aggregators, das_filters
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code

import DAS.utils.jsonwrapper as json

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'show', 'collection', 'name',
                  'format', 'sort', 'dir', 'view', 'method']


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
    for key, val in rdict.items():
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
                if  isinstance(val[0], str):
                    value = ', '.join(val)
                else:
                    value = val
            elif  key.find('size') != -1:
                value = size_format(val)
            elif  key.find('Number of ') != -1:
                value = int(val)
            else:
                value = val
            if  isinstance(value, list) and isinstance(value[0], str):
                value = ', '.join(value)
            page += "<b>%s:</b> %s<br />" % (key, value)
        else:
            if  key == 'result' and isinstance(val, dict) and \
                val.has_key('value'): # result of aggregation function
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
        self.cachesrv   = config['cache_server_url']
        self.base       = config['url_base']
        self.status_update = config['status_update']
        logfile  = config['logfile']
        loglevel = config['loglevel']
        self.logger  = DASLogger(logfile=logfile, verbose=loglevel)
        set_cherrypy_logger(self.logger.handler, loglevel)
        self.pageviews  = ['xml', 'list', 'json', 'yuijson'] 
        msg = "DASSearch::init is started with base=%s" % self.base
        self.logger.info(msg)
        dasconfig = das_readconfig()
        dburi = dasconfig['mongodb']['dburi']

        self.init()
        # Monitoring thread which performs auto-reconnection
        thread.start_new_thread(connection_monitor, (dburi, self.init, 5))

    def init(self):
        """Init DAS web server, connect to DAS Core"""
        try:
            self.dasmgr     = DASCore()
            self.daskeys    = self.dasmgr.das_keys()
            self.daskeys.sort()
            self.dasmapping = self.dasmgr.mapping
        except:
            traceback.print_exc()
            self.dasmgr = None
            self.daskeys = []

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
        page = self.templatepage('das_faq', 
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
        clifile = os.path.join(dasroot, 'DAS/tools/das_cache_client.py')
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
        desc = self.tempaltepage('das_opensearch', base=base)
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
        Check provided input for valid DAS keys.
        """
        def helper(myinput, msg):
            """Helper function which provide error template"""
            return self.templatepage('das_ambiguous', msg=msg, base=self.base,
                        input=myinput, entities=', '.join(self.daskeys),
                        operators=', '.join(das_operators()))
        if  not uinput:
            return helper(uinput, 'No input query')
        # check provided input. If at least one word is not part of das_keys
        # return ambiguous template.
        try:
            mongo_query = self.dasmgr.mongoparser.parse(uinput)
        except:
            msg  = 'Fail in mongo parser, unable to parse input query.\n'
            return helper(uinput, msg)
        fields = mongo_query.get('fields', [])
        if  not fields:
            fields = []
        spec   = mongo_query.get('spec', {})
        if  not fields+spec.keys():
            msg = 'Provided input does not resolve into valid set of keys'
            return helper(uinput, msg)
        for word in fields+spec.keys():
            found = 0
            for key in self.daskeys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                msg = 'Provided input does not contain valid DAS key'
                return helper(uinput, msg)
        return

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
        try:
            recid = args[0]
            time0    = time.time()
            url      = self.cachesrv
            show     = getarg(kwargs, 'show', 'json')
            coll     = getarg(kwargs, 'collection', 'merge')
            params   = {'fid':recid}
            path     = '/rest/gridfs'
            headers  = {"Accept": "application/json"}
            try:
                data = urllib2_request('GET', url+path, params, headers=headers)
                result = json.loads(data)
            except urllib2.HTTPError, httperror:
                err = get_ecode(httperror.read())
                self.logger.error(err)
                result = {'status':'fail', 'reason': err}
            except:
                self.logger.error(traceback.format_exc())
                result = {'status':'fail', 'reason':traceback.format_exc()}
        except:
                self.logger.error(traceback.format_exc())
                result = {'status':'fail', 'reason':traceback.format_exc()}
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return result

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
                spec = {'_id':recordid}
                fields = None
                query = dict(fields=fields, spec=spec)
                if  len(args) == 2:
                    format = args[1]
            elif  kwargs and kwargs.has_key('_id'):
                spec = {'_id': kwargs['_id']}
                fields = None
                query = dict(fields=fields, spec=spec)
            else: # return all ids
                query = dict(fields=None, spec={})

            time0    = time.time()
            url      = self.cachesrv
            idx      = getarg(kwargs, 'idx', 0)
            limit    = getarg(kwargs, 'limit', 10)
            show     = getarg(kwargs, 'show', 'json')
            coll     = getarg(kwargs, 'collection', 'merge')
            nresults = self.nresults({'input':json.dumps(query), 'collection':coll})
            params   = {'query':json.dumps(query), 'idx':idx, 'limit':limit, 
                        'collection':coll}
            path     = '/rest/records'
            headers  = {"Accept": "application/json"}
            try:
                data = urllib2_request('GET', url+path, params, headers=headers)
                result = json.loads(data)
            except urllib2.HTTPError, httperror:
                err = get_ecode(httperror.read())
                self.logger.error(err)
                result = {'status':'fail', 'reason': err}
            except:
                self.logger.error(traceback.format_exc())
                result = {'status':'fail', 'reason':traceback.format_exc()}
            res = ""
            if  result['status'] == 'success':
                if  recordid: # we got id
                    for row in result['data']:
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
                    for row in result['data']:
                        rid  = row['_id']
                        del row['_id']
                        res += self.templatepage('das_record', \
                                id=rid, daskeys=', '.join(row))
            else:
                res = result['status']
                if  res.has_key('reason'):
                    return self.error(res['reason'])
                else:
                    msg = 'Uknown error, kwargs=' % kwargs
                    return self.error(msg)
            if  recordid:
                if  format:
                    if  format == 'xml':
                        return wrap2dasxml(result['data'])
                    elif  format == 'json':
                        return wrap2dasjson(result['data'])
                    else:
                        return self.error('Unsupported data format %s' % format)
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

    def nresults(self, kwargs):
        """
        invoke DAS search call, parse results and return them to
        web methods
        """
        url     = self.cachesrv
        uinput  = kwargs.get('input', '')
        coll    = kwargs.get('collection', 'merge')
        params  = {'query':uinput, 'collection': coll}
        path    = '/rest/nresults'
        headers = {"Accept": "application/json"}
        try:
            data = urllib2_request('GET', url+path, params, headers=headers)
            record = json.loads(data)
        except urllib2.HTTPError, httperror:
            err = get_ecode(httperror.read())
            self.logger.error(err)
            record = {'status':'fail', 'reason': err}
        except:
            self.logger.error(traceback.format_exc())
            record = {'status':'fail', 'reason':traceback.format_exc()}
        if  record['status'] == 'success':
            return record['nresults']
        else:
            msg = "nresults returns status: %s" % str(record)
            self.logger.info(msg)
        return -1

    def send_request(self, method, kwargs):
        "Send POST request to server with provided parameters"
        url     = self.cachesrv
        uinput  = getarg(kwargs, 'input', '')
        format  = getarg(kwargs, 'format', '')
        idx     = getarg(kwargs, 'idx', 0)
        limit   = getarg(kwargs, 'limit', 10)
        skey    = getarg(kwargs, 'sort', '')
        sdir    = getarg(kwargs, 'dir', 'asc')
        params  = {'query':uinput, 'idx':idx, 'limit':limit, 
                  'skey':skey, 'order':sdir}
        if  method == 'POST':
            path    = '/rest/create'
        elif  method == 'GET':
            path    = '/rest/request'
        else:
            raise Exception('Unsupported method %s' % method)
        headers = {'Accept': 'application/json', 
                   'Content-type': 'application/json'} 
        try:
            data = urllib2_request(method, url+path, params, headers=headers)
            result = json.loads(data)
        except urllib2.HTTPError, httperror:
            err = get_ecode(httperror.read())
            self.logger.error(err)
            result = {'status':'fail', 'reason': err}
        except:
            self.logger.error(traceback.format_exc())
            result = {'status':'fail', 'reason':traceback.format_exc()}
        return result

    def result(self, kwargs):
        """
        invoke DAS search call, parse results and return them to
        web methods
        """
        result  = self.send_request('GET', kwargs)
        res = []
        if  isinstance(result, str):
            data = json.loads(result)
        else:
            data = result
        if  data['status'] == 'success':
            res    = data['data']
        return res
        
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

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def request(self, *args, **kwargs):
        """
        Request data from DAS cache.
        """
        # do not allow caching
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'

        time0   = time.time()
        uinput  = getarg(kwargs, 'input', '')
        form    = self.form(input=uinput)
        view    = kwargs.get('view', 'list')
        # self.status sends request to Cache Server
        # Cache Server uses das_core to retrieve status
        status  = self.check_data(input=uinput)
        if  status == 'no data':
            # no data in raw cache, send POST request
            self.send_request('POST', kwargs)
            img   = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
            page  = img + ' there is no data in a cache yet, please wait...'
            page += ', <a href="/das/">stop</a> request' 
            page += """<script type="application/javascript">"""
            page += """setTimeout('ajaxStatus("%s")', %s)</script>""" \
                        % (self.base, self.status_update)
            view  = 'list'
        elif status == 'fail':
            msg   = "DAS cache server fails to process your request"
            msg   = self.gen_error_msg(kwargs)
            page  = self.templatepage('das_error', msg=msg)
            view  = 'list'
        else:
            try:
                func = getattr(self, view + "view") 
                page = func(kwargs)
            except:
                traceback.print_exc()
                msg   = 'Wrong view'
                msg  += self.gen_error_msg(kwargs)
                page  = self.templatepage('das_error', msg=msg)
        ctime = (time.time()-time0)
        if  view == 'list' or view == 'plain':
            return self.page(form + page, ctime=ctime)
        else:
            return page
        
    def listview(self, kwargs):
        """
        Helper function to make listview page.
        """
        time0   = time.time()
        uinput  = getarg(kwargs, 'input', '')
        idx     = getarg(kwargs, 'idx', 0)
        limit   = getarg(kwargs, 'limit', 10)
        show    = getarg(kwargs, 'show', 'json')
        # call self.result before self.nresults
        # since it invokes send_request, which by itself invokes
        # DAS core result function and trigger data movement from
        # raw cache into merge cache
        rows    = self.result(kwargs) # call it before nresults, since
        total   = self.nresults(kwargs)
        if  total:
            params = {} # will keep everything except idx/limit
            for key, val in kwargs.items():
                if  key != 'idx' and key != 'limit':
                    params[key] = val
            url = "%s/request?%s" \
                    % (self.base, urllib.urlencode(params, doseq=True))
            page = self.templatepage('das_pagination', \
                nrows=total, idx=idx, limit=limit, url=url)
        else:
            return 'No results found in DAS cache'
        nrows   = len(rows)
        style   = "white"
        for row in rows:
            id    = row['_id']
            page += '<div class="%s"><hr class="line" />' % style
            if  row.has_key('das'):
                if  row['das'].has_key('primary_key'):
                    pkey  = row['das']['primary_key']
                    page += '<b>DAS key:</b> %s<br />' % pkey.split('.')[0]
            gen   = self.convert2ui(row)
            if  self.dasmgr:
                func  = self.dasmgr.mapping.daskey_from_presentation
                page += adjust_values(func, gen)
            pad   = ""
            if  show == 'json':
                jsonhtml = das_json(row, pad)
                page += self.templatepage('das_row', \
                        sanitized_data=jsonhtml, id=id, rec_id=id)
            elif show == 'code':
                code  = pformat(row, indent=1, width=100)
                data  = self.templatepage('das_json', jsoncode=code)
                page += self.templatepage('das_row', \
                        sanitized_data=data, id=id, rec_id=id)
            else:
                code  = yaml.dump(row, width=100, indent=4, 
                                default_flow_style=False)
                data  = self.templatepage('das_json', jsoncode=code)
                page += self.templatepage('das_row', \
                        sanitized_data=data, id=id, rec_id=id)
            page += '</div>'
        return page

    @exposedasplist
    def xmlview(self, kwargs):
        """
        provide DAS XML
        """
        rows = self.result(kwargs)
        return rows

    @exposedasjson
    def jsonview(self, kwargs):
        """
        provide DAS JSON
        """
        rows = self.result(kwargs)
        return rows

    @exposetext
    def plainview(self, kwargs):
        """
        provide DAS plain view
        """
        rows, total, form = self.result(kwargs)
        page = ""
        for item in rows:
            item  = str(item).replace('[','').replace(']','')
            page += "%s\n" % item.replace("'","")
        return page

    @exposejson
    @checkargs(DAS_WEB_INPUTS)
    def yuijson(self, **kwargs):
        """
        Provide JSON in YUI compatible format to be used in DynamicData table
        widget, see
        http://developer.yahoo.com/yui/examples/datatable/dt_dynamicdata.html
        """
        rows = self.result(kwargs)
        rowlist = []
        id = 0
        for row in rows:
            das = row['das']
            if  isinstance(das, dict):
                das = [das]
            resdict = {}
            for jdx in range(0, len(das)):
                item = das[jdx]
                resdict[id] = id
                for idx in range(0, len(item['system'])):
                    api    = item['api'][idx]
                    system = item['system'][idx]
                    key    = item['selection_keys'][idx]
                    data   = row[key]
                    if  isinstance(data, list):
                        data = data[idx]
                    # I need to extract from DAS object the values for UI keys
                    for item in self.dasmapping.presentation(key):
                        daskey = item['das']
                        uiname = item['ui']
                        if  not resdict.has_key(uiname):
                            resdict[uiname] = ""
                        # look at key attributes, which may be compound as well
                        # e.g. block.replica.se
                        if  isinstance(data, dict):
                            result = dict(data)
                        elif isinstance(data, list):
                            result = list(data)
                        else:
                            result = data
                        res = ""
                        try:
                            for elem in daskey.split('.')[1:]:
                                if  result.has_key(elem):
                                    res  = result[elem]
                                    resdict[uiname] = res
                        except:
                            pass
            if  resdict not in rowlist:
                rowlist.append(resdict)
            id += 1
        idx      = getarg(kwargs, 'idx', 0)
        limit    = getarg(kwargs, 'limit', 10)
        total    = len(rowlist) 
        jsondict = {'recordsReturned': len(rowlist),
                   'totalRecords': total, 'startIndex':idx,
                   'sort':'true', 'dir':'asc',
                   'pageSize': limit,
                   'records': rowlist}
        return jsondict

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def check_data(self, **kwargs):
        """
        Check status of user request in DAS cache.
        We return either ok/no data/fail.
        """
        uinput  = kwargs.get('input', '')
        uinput  = urllib.unquote_plus(uinput)
        view    = kwargs.get('view', 'list')
        params  = {'query':uinput}
        path    = '/rest/status'
        url     = self.cachesrv
        headers = {'Accept': 'application/json'}
        try:
            res  = urllib2_request('GET', url+path, params, headers=headers)
            data = json.loads(res)
        except urllib2.HTTPError, httperror:
            err = get_ecode(httperror.read())
            self.logger.error(err)
            data = {'status':'fail', 'reason': err}
        except:
            self.logger.error(traceback.format_exc())
            data = {'status':'fail'}
        if  data['status']:
            status = data['status']
        else:
            status = 'no data'
        return status

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def status(self, **kwargs):
        """
        Place AJAX request to obtain status about given query
        """
        img  = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
        req  = """
        <script type="application/javascript">
        setTimeout('ajaxStatus("%s")', %s)
        </script>""" % (self.base, self.status_update)

        def set_header():
            "Set HTTP header parameters"
            tstamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            cherrypy.response.headers['Expire'] = tstamp
            cherrypy.response.headers['Cache-control'] = 'no-cache'

        uinput  = kwargs.get('input', '')
        uinput  = urllib.unquote_plus(uinput)
        params  = {'query':uinput}
        path    = '/rest/status'
        url     = self.cachesrv
        headers = {'Accept': 'application/json'}
        try:
            res  = urllib2_request('GET', url+path, params, headers=headers)
            data = json.loads(res)
        except urllib2.HTTPError, httperror:
            err = get_ecode(httperror.read())
            self.logger.error(err)
            data = {'status':'fail', 'reason': err}
        except:
            self.logger.error(traceback.format_exc())
            data = {'status':'fail'}
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        if  data['status'] == 'ok':
            # we got data
            kwargs['input'] = uinput
            page = self.listview(kwargs)
        elif data['status'] == 'fail':
            # we fail, stop here and show message to ther user
            page  = 'Request failed. '
            page += self.error(self.gen_error_msg(kwargs), wrap=False)
        else:
            # we still acquiring the data, continue with AJAX request
            page  = img + ' ' + str(data['status']) + ', please wait...'
            page += ', <a href="/das/">stop</a> request' 
            page += req
        set_header()
        page = ajax_response(page)
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
