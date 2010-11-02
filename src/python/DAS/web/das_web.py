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
import sys
import time
import urllib
import urllib2
import cherrypy
import traceback
import thread

import yaml
from pprint import pformat

from itertools import groupby
from cherrypy import expose, HTTPError
from cherrypy.lib.static import serve_file
from json import JSONEncoder

# DAS modules
import DAS
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_operators
from DAS.utils.utils import getarg, access
from DAS.utils.logger import DASLogger, set_cherrypy_logger
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection, connection_monitor
from DAS.web.utils import urllib2_request, json2html, web_time
from DAS.web.utils import ajax_response, checkargs, get_ecode
from DAS.web.tools import exposedasjson, exposetext
from DAS.web.tools import exposejson, exposedasplist
from DAS.web.das_webmanager import DASWebManager
from DAS.web.das_codes import web_code

import DAS.utils.jsonwrapper as json

DAS_WEB_INPUTS = ['input', 'idx', 'limit', 'show', 'collection', 
                  'format', 'sort', 'dir', 'ajax', 'view', 'method']

class DASWebService(DASWebManager):
    """
    DAS web service interface.
    """
    def __init__(self, config={}):
        DASWebManager.__init__(self, config)
        self.cachesrv   = config['cache_server_url']
        self.base       = config['url_base']
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
            self.dasmgr = None

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
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        services = self.dasmgr.keys()
        srv = ""
        for key in services.keys():
            srv += "%s, " % key
        srv = srv[:-2] # remove last comma
        page += self.templatepage('das_bottom', ctime=ctime, services=srv,
                                  version=DAS.version,
                                  timestamp=timestamp, div=response_div)
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
        desc = """<?xml version="1.0"?>
<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/"
                       xmlns:moz="http://www.mozilla.org/2006/browser/search/">
<ShortName>DAS search</ShortName>
<Description>CMS DAS Search</Description>
<Image type="image/png">%s/images/cms_logo.png</Image>
<Url type="text/html" method="get" template="%s/?input={searchTerms}&amp;view=list&amp;show=json"/>
<moz:SearchForm>%s</moz:SearchForm>
</OpenSearchDescription>""" % (base, base, base)
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
            jsoncode = {'jsoncode': json2html(record, "")}
            page += self.templatepage('das_json', **jsoncode)
        elif show == 'code':
            code  = pformat(record, indent=1, width=100)
            page += self.templatepage('das_code', code=code)
        else:
            code  = yaml.dump(record, width=100, indent=4, 
                        default_flow_style=False)
            page += self.templatepage('das_code', code=code)
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
            msg = sys.exc_info()[1]
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
        try:
            if  not args and not kwargs:
                page = self.form()
                return self.page(page)
            uinput  = getarg(kwargs, 'input', '')
            results = self.check_input(uinput)
            if  results:
                return self.page(self.form() + results)
            view = getarg(kwargs, 'view', 'list')
            if  args:
                return getattr(self, args[0][0])(args[1])
            if  view not in self.pageviews:
                raise Exception("Page view '%s' is not supported" % view)
            return getattr(self, '%sview' % view)(**kwargs)
        except:
            return self.error(self.gen_error_msg(kwargs))

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def form(self, input=None, msg=None):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=input, msg=msg, 
                                        base=self.base)
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
    def error(self, msg):
        """
        Show error message.
        """
        error = self.templatepage('das_error', msg=msg)
        page  = self.page(self.form() + error)
        return page

    def wrap2dasjson(self, data):
        """DAS JSON wrapper"""
        encoder = JSONEncoder()
        cherrypy.response.headers['Content-Type'] = "text/json"
        try:
            jsondata = encoder.encode(data)
            return jsondata
        except:
            return dict(error="Failed to JSONtify obj '%s' type '%s'" \
                % (data, type(data)))

    def wrap2dasxml(self, data):
        """DAS XML wrapper.
        Return data in XML plist format, 
        see http://docs.python.org/library/plistlib.html#module-plistlib
        """
        plist_str = plistlib.writePlistToString(data)
        cherrypy.response.headers['Content-Type'] = "application/xml"
        return plist_str

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
                            jsoncode = {'jsoncode': json2html(row, "")}
                            res += self.templatepage('das_json', **jsoncode)
                        elif show == 'code':
                            code  = pformat(row, indent=1, width=100)
                            res += self.templatepage('das_code', code=code)
                        else:
                            code = yaml.dump(row, width=100, indent=4, 
                                        default_flow_style=False)
                            res += self.templatepage('das_code', code=code)
                else:
                    for row in result['data']:
                        rid  = row['_id']
                        del row['_id']
                        record = dict(id=rid, daskeys=', '.join(row))
                        res += self.templatepage('das_record', **record)
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
                        return self.wrap2dasxml(result['data'])
                    elif  format == 'json':
                        return self.wrap2dasjson(result['data'])
                    else:
                        return self.error('Unsupported data format %s' % format)
                page  = res
            else:
                url   = '/das/records?'
                idict = dict(nrows=nresults, idx=idx, 
                            limit=limit, results=res, url=url)
                if  nresults:
                    page = self.templatepage('das_pagination', **idict)
                else:
                    page = 'No results found'

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
        
    @exposedasplist
    def xmlview(self, *args, **kwargs):
        """
        provide DAS XML
        """
        rows = self.result(kwargs)
        return rows

    @exposedasjson
    def jsonview(self, *args, **kwargs):
        """
        provide DAS JSON
        """
        rows = self.result(kwargs)
        return rows

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
    def listview(self, **kwargs):
        """
        provide DAS list view
        """
        # force to load the page all the time
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'

        time0   = time.time()
        ajaxreq = getarg(kwargs, 'ajax', 0)
        uinput  = getarg(kwargs, 'input', '')
        idx     = getarg(kwargs, 'idx', 0)
        limit   = getarg(kwargs, 'limit', 10)
        show    = getarg(kwargs, 'show', 'json')
        form    = self.form(input=uinput)
        # self.status sends request to Cache Server
        # Cache Server uses das_core to retrieve status
        status  = self.status(input=uinput, ajax='0')
        if  status == 'no data':
            # no data in raw cache, send POST request
            self.send_request('POST', kwargs)
            ctime = (time.time()-time0)
            page  = self.status(input=uinput)
            page  = self.page(form + page, ctime=ctime)
            return page
        elif status == 'fail':
            kwargs['reason'] = 'Unable to get status from data-service'
            return self.error(self.gen_error_msg(kwargs))

        total   = self.nresults(kwargs)
        rows    = self.result(kwargs)
        nrows   = len(rows)
        page    = ""
        style   = "white"
        for row in rows:
            id    = row['_id']
            page += '<div class="%s"><hr class="line" />' % style
            if  row.has_key('das'):
                if  row['das'].has_key('primary_key'):
                    pkey  = row['das']['primary_key']
                    page += '<b>DAS key:</b> %s<br />' % pkey.split('.')[0]
            gen   = self.convert2ui(row)
            for uikey, value in [k for k, g in groupby(gen)]:
                page += "<b>%s</b>: %s<br />" % (uikey, value)
            pad   = ""
            if  show == 'json':
                jsoncode = {'jsoncode': json2html(row, pad)}
                jsonhtml = self.templatepage('das_json', **jsoncode)
                jsondict = dict(data=jsonhtml, id=id, rec_id=id)
                page += self.templatepage('das_row', **jsondict)
            elif show == 'code':
                code  = pformat(row, indent=1, width=100)
                data  = self.templatepage('das_code', code=code)
                datadict = {'data':data, 'id':id, 'rec_id':id}
                page += self.templatepage('das_row', **datadict)
            else:
                code  = yaml.dump(row, width=100, indent=4, 
                                default_flow_style=False)
                data  = self.templatepage('das_code', code=code)
                datadict = {'data':data, 'id':id, 'rec_id':id}
                page += self.templatepage('das_row', **datadict)
            page += '</div>'
        ctime = (time.time()-time0)
        url   = "%s/?view=list&show=%s&input=%s&ajax=%s" \
        % (self.base, show, uinput, ajaxreq)
        idict = dict(nrows=total, idx=idx,
                    limit=limit, results=page, url=url)
        if  total:
            page = self.templatepage('das_pagination', **idict)
        else:
            page = 'No results found'
        return self.page(form + page, ctime=ctime)

    @exposetext
    @checkargs(DAS_WEB_INPUTS)
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
    def tableview(self, **kwargs):
        """
        provide DAS table view
        """
        kwargs['format'] = 'html'
        uinput  = getarg(kwargs, 'input', '')
        ajaxreq = getarg(kwargs, 'ajax', 0)
        form    = self.form(input=uinput)
        time0   = time.time()
        total   = self.nresults(kwargs)
        if  not total:
            ctime   = (time.time()-time0)
            form    = self.form(input)
            page    = self.templatepage('not_ready')
            page    = self.page(form + page, ctime=ctime)
            return page

        # find out which selection keys were used
        selkeys = uinput.replace('find ', '').split(' where ')[0].split(',')
        uikeys  = []
        for key in selkeys:
            res = self.dasmapping.presentation(key)
            uikeys += [item['ui'] for item in res]
        titles = ["id"] + uikeys
        coldefs = ""
        for title in titles:
            coldefs += '{key:"%s",label:"%s",sortable:true,resizeable:true},' \
                        % (title, title)
        coldefs = "[%s]" % coldefs[:-1] # remove last comma
        coldefs = coldefs.replace("},{","},\n{")
        limit   = getarg(kwargs, 'limit', 10)
        names   = {'titlelist':titles,
                   'coldefs':coldefs, 'rowsperpage':limit,
                   'total':total, 'tag':'mytag', 'ajax':ajaxreq,
                   'input':urllib.urlencode(dict(input=uinput))}
        page    = self.templatepage('das_table', **names)
        ctime   = (time.time()-time0)
        page    = self.page(form + page, ctime=ctime)
        return page

    @expose
    @checkargs(DAS_WEB_INPUTS)
    def status(self, **kwargs):
        """
        Place request to obtain status about given query
        """
        img  = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
        req  = """
        <script type="application/javascript">
        setTimeout('ajaxStatus("%s")',3000)
        </script>""" % self.base

        def set_header():
            "Set HTTP header parameters"
            tstamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            cherrypy.response.headers['Expire'] = tstamp
            cherrypy.response.headers['Cache-control'] = 'no-cache'

        uinput  = kwargs.get('input', '')
        uinput  = urllib.unquote_plus(uinput)
        ajax    = kwargs.get('ajax', 1)
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
        if  int(ajax):
            cherrypy.response.headers['Content-Type'] = 'text/xml'
            if  data['status'] == 'ok':
                page  = '<script type="application/javascript">reload()</script>'
            elif data['status'] == 'fail':
                page  = '<script type="application/javascript">reload()</script>'
                page += self.error(self.gen_error_msg(kwargs))
            else:
                page  = img + ' ' + str(data['status']) + ', please wait...'
                img_stop = ''
                page += ', <a href="/das/">stop</a> request' 
                page += req
                set_header()
            page = ajax_response(page)
        else:
            try:
                page = data['status']
            except:
                page = traceback.format_exc()
        return page
