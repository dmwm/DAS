#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface, based on WMCore/WebTools
"""
from __future__ import print_function

__revision__ = "$Id: DASSearch.py,v 1.1 2010/03/18 17:52:25 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import time
import types
import urllib
import cherrypy
import traceback

import yaml
from pprint import pformat

from itertools import groupby
from cherrypy import expose, tools
from cherrypy.lib.static import serve_file

#try:
    # WMCore/WebTools modules
#    from WMCore.WebTools.Page import TemplatedPage
#    from WMCore.WebTools.Page import exposedasjson, exposetext
#    from WMCore.WebTools.Page import exposejson, exposedasplist
#except:
    # stand-alone version
#    from DAS.web.tools import exposedasjson, exposetext
#    from DAS.web.tools import exposejson, exposedasplist

# DAS modules
from DAS.web.tools import exposedasjson, exposetext
from DAS.web.tools import exposejson, exposedasplist
from DAS.core.das_core import DASCore
from DAS.core.das_ql import das_aggregators, das_filters, das_operators
from DAS.core.das_parser import parser
from DAS.utils.utils import getarg, access
from DAS.web.das_webmanager import DASWebManager
from DAS.web.utils import urllib2_request, json2html, web_time

import DAS.utils.jsonwrapper as json

if sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")

def ajax_response_orig(msg, tag="_response", element="object"):
    """AJAX response wrapper"""
    page  = """<ajax-response><response type="%s" id="%s">""" % (element, tag)
    page += msg
    page += "</response></ajax-response>"
    print(page)
    return page

def ajax_response(msg):
    """AJAX response wrapper"""
    page  = """<ajax-response>"""
    page += "<div>" + msg + "</div>"
    page += "</ajax-response>"
    return page

class DASSearch(DASWebManager):
    """
    DAS web interface.
    """
    def __init__(self, config={}):
        DASWebManager.__init__(self, config)
        try:
            # try what is supplied from WebTools framework
            cdict         = self.config.dictionary_()
            self.cachesrv = cdict.get('cache_server_url', 
                                'http://localhost:8211')
            self.base     = '/dascontrollers'
        except:
            # stand-alone version
            self.cachesrv = config.get('cache_server_url',
                                'http://localhost:8211')
            self.base     = '/das'
        self.dasmgr     = DASCore()
        self.daskeys    = self.dasmgr.das_keys()
        self.daskeys.sort()
        self.dasmapping = self.dasmgr.mapping
        self.daslogger  = self.dasmgr.logger
        self.pageviews  = ['xml', 'list', 'json', 'yuijson'] 
        msg = "DASSearch::init is started with base=%s" % self.base
        self.daslogger.debug(msg)
        print(msg)

    def top(self):
        """
        Define masthead for all DAS web pages
        """
        return self.templatepage('das_top', base=self.base)

    def bottom(self, response_div=True):
        """
        Define footer for all DAS web pages
        """
        return self.templatepage('das_bottom', div=response_div)

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
                                  timestamp=timestamp, div=response_div)
        return page

    @expose
    def faq(self, *args, **kwargs):
        """
        represent DAS FAQ.
        """
        page = self.templatepage('das_faq', 
                operators=', '.join(das_operators()), 
                aggregators=', '.join(das_aggregators()))
        return self.page(page, response_div=False)

    @expose
    def help(self, *args, **kwargs):
        """
        represent DAS help
        """
        page = self.templatepage('das_help')
        return self.page(page, response_div=False)

    @expose
    def cli(self, *args, **kwargs):
        """
        Serve DAS CLI file download.
        """
        clifile = os.path.join(os.environ['DAS_ROOT'], 
                'src/python/DAS/tools/das_cache_client.py')
        return serve_file(clifile, content_type='text/plain')

    @expose
    def services(self, *args, **kwargs):
        """
        represent DAS services
        """
        dasdict = {}
        daskeys = []
        for system, keys in self.dasmgr.mapping.daskeys().items():
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
    def default(self, *args, **kwargs):
        """
        Default method.
        """
        return self.index(args, kwargs)

    def check_input(self, uinput):
        """
        Check provided input for valid DAS keys.
        """
        error = self.templatepage('das_ambiguous',
                    input=uinput, entities=', '.join(self.daskeys))
        if  not uinput:
            return error
        # check provided input. If at least one word is not part of das_keys
        # return ambiguous template.
        mongo_query = self.dasmgr.mongoparser.parse(uinput)
        fields = mongo_query.get('fields', [])
        if  not fields:
            fields = []
        spec   = mongo_query.get('spec', {})
        if  not fields+spec.keys():
            return error
        for word in fields+spec.keys():
            found = 0
            for key in das_keys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                return error
        return

    @expose
    def index(self, *args, **kwargs):
        """
        represents DAS web interface. 
        It uses das_searchform template for
        input form and yui_table for output Table widget.
        """
        try:
            if  not args and not kwargs:
#                msg  = self.templatepage('das_help', 
#                        services    = ', '.join(self.dasmgr.keys()),
#                        keywords    = ', '.join(self.dasmgr.das_keys()),
#                        operators   = ', '.join(das_operators()),
#                        aggregators = ', '.join(das_aggregators()),
#                        filters     = ', '.join(das_filters()) 
#                        )
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
            return getattr(self, '%sview' % view)(kwargs)
        except:
            return self.error(self.gen_error_msg(kwargs))

    @expose
    def form(self, uinput=None, msg=None):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=uinput, msg=msg, 
                                        base=self.base)
        return page

    def gen_error_msg(self, kwargs):
        """
        Generate standard error message.
        """
        self.daslogger.error(traceback.format_exc())
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

    @exposedasjson
    def wrap2dasjson(self, data):
        """DAS JSON wrapper"""
        return data

    @exposedasplist
    def wrap2dasxml(self, data):
        """DAS XML wrapper"""
        return data

    @expose
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
            elif  kwargs and '_id' in kwargs:
                spec = {'_id': kwargs['_id']}
                fields = None
                query = dict(fields=fields, spec=spec)
            else: # return all ids
                query = dict(fields=None, spec={})

            nresults = self.nresults(query)
            time0    = time.time()
            url      = self.cachesrv
            idx      = getarg(kwargs, 'idx', 0)
            limit    = getarg(kwargs, 'limit', 10)
            show     = getarg(kwargs, 'show', 'json')
            coll     = getarg(kwargs, 'collection', 'merge')
#            params   = {'query':json.dumps(query), 'idx':idx, 'limit':limit}
#            path     = '/rest/request'
            params   = {'query':json.dumps(query), 'idx':idx, 'limit':limit, 
                        'collection':coll}
            path     = '/rest/records'
            headers  = {"Accept": "application/json"}
            try:
                data = urllib2_request('GET', url+path, params, headers=headers)
                result = json.loads(data)
            except:
                self.daslogger.error(traceback.format_exc())
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
                if  'reason' in res:
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
                page  = self.templatepage('das_pagination', **idict)

            form    = self.form(uinput="")
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
        uinput  = getarg(kwargs, 'input', '')
        params  = {'query':uinput}
        path    = '/rest/nresults'
        headers = {"Accept": "application/json"}
        try:
            data = urllib2_request('GET', url+path, params, headers=headers)
            record = json.loads(data)
        except:
            self.daslogger.error(traceback.format_exc())
            record = {'status':'fail', 'reason':traceback.format_exc()}
        if  record['status'] == 'success':
            return record['nresults']
        else:
            msg = "nresults returns status: %s" % str(record)
            self.daslogger.info(msg)
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
        except:
            self.daslogger.error(traceback.format_exc())
            result = {'status':'fail', 'reason':traceback.format_exc()}
        return result

    def result(self, kwargs):
        """
        invoke DAS search call, parse results and return them to
        web methods
        """
        result  = self.send_request('GET', kwargs)
        res = []
        if  type(result) is bytes:
            data = json.loads(result)
        else:
            data = result
        if  data['status'] == 'success':
            res    = data['data']
        return res
        
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

    def convert2ui(self, idict):
        """
        Convert input row (dict) into UI presentation
        """
        for key in idict.keys():
            if  key == 'das' or key == '_id' or key == 'das_id':
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
    def listview(self, kwargs):
        """
        provide DAS list view
        """
        # force to load the page all the time
        cherrypy.response.headers['Cache-Control'] = 'no-cache'
        cherrypy.response.headers['Pragma'] = 'no-cache'

        time0   = time.time()
        ajaxreq = getarg(kwargs, 'ajax', 0)
        uinput  = getarg(kwargs, 'input', '')
        limit   = getarg(kwargs, 'limit', 10)
        show    = getarg(kwargs, 'show', 'json')
        form    = self.form(uinput=uinput)
        # self.status sends request to Cache Server
        # Cache Server uses das_core to retrieve status
        status  = self.status(input=uinput, ajax=0)
        if  status == 'no data':
            # no data in raw cache, send POST request
            self.send_request('POST', kwargs)
            ctime = (time.time()-time0)
#            page    = self.templatepage('not_ready')
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
        ndict   = {'nrows':total, 'limit':limit}
        page    = self.templatepage('das_nrecords', **ndict)
#        for nrecord in range(0, len(rows)):
#            row = rows[nrecord]
#            style = "white"
#            if  nrecord % 2:
#                style = "white"
#            else:
#                style = "gray" 
        style = "white"
        for row in rows:
            id    = row['_id']
            page += '<div class="%s"><hr class="line" />' % style
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
                datadict = {'data':data, 'id':id, rec_id:id}
                page += self.templatepage('das_row', **datadict)
            else:
                code  = yaml.dump(row, width=100, indent=4, 
                                default_flow_style=False)
                data  = self.templatepage('das_code', code=code)
                datadict = {'data':data, 'id':id, rec_id:id}
                page += self.templatepage('das_row', **datadict)
            page += '</div>'
        ctime   = (time.time()-time0)
        return self.page(form + page, ctime=ctime)

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
            if  type(das) is dict:
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
                    if  type(data) is list:
                        data = data[jdx]
                    if  type(data) is list:
                        data = data[idx]
                    # I need to extract from DAS object the values for UI keys
                    for item in self.dasmapping.presentation(key):
                        daskey = item['das']
                        uiname = item['ui']
                        if  uiname not in resdict:
                            resdict[uiname] = ""
                        # look at key attributes, which may be compound as well
                        # e.g. block.replica.se
                        if  type(data) is dict:
                            result = dict(data)
                        elif type(data) is list:
                            result = list(data)
                        else:
                            result = data
                        res = ""
                        try:
                            for elem in daskey.split('.')[1:]:
                                if  elem in result:
                                    res  = result[elem]
                                    resdict[uiname] = res
                        except:
                            pass
#                    pad = ""
#                    jsoncode = {'jsoncode': json2html(data, pad)}
#                    jsonhtml = self.templatepage('das_json', **jsoncode)
#                    jsondict = {'id':id, 'system':system, 'api':api, key:jsonhtml}
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
    def tableview(self, kwargs):
        """
        provide DAS table view
        """
        kwargs['format'] = 'html'
        uinput  = getarg(kwargs, 'input', '')
        ajaxreq = getarg(kwargs, 'ajax', 0)
        form    = self.form(uinput=uinput)
        time0   = time.time()
        total   = self.nresults(kwargs)
        if  not total:
            ctime   = (time.time()-time0)
            form    = self.form(uinput)
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
    def status(self, **kwargs):
        """
        Place request to obtain status about given query
        """
        img  = '<img src="%s/images/loading.gif" alt="loading"/>' % self.base
        req  = """
        <script type="application/javascript">
        setTimeout('ajaxStatus()',3000)
        </script>"""

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
        except:
            self.daslogger.error(traceback.format_exc())
            data = {'status':'fail'}
        if  ajax:
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

#    @expose
#    @tools.cernoid()
#    def secure(self, *args, **kwargs):
#        return "TEST secure page"

#    @expose
#    def auth(self, *args, **kwargs):
#        return "auth page"
