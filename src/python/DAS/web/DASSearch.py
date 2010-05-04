#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface, based on WMCore/WebTools
"""

__revision__ = "$Id: DASSearch.py,v 1.35 2010/01/07 21:29:02 valya Exp $"
__version__ = "$Revision: 1.35 $"
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

from itertools import groupby
from cherrypy import expose
from cherrypy.lib.static import serve_file

# WMCore/WebTools modules
from WMCore.WebTools.Page import TemplatedPage
from WMCore.WebTools.Page import exposedasjson, exposetext
from WMCore.WebTools.Page import exposejson, exposedasplist

# DAS modules
from DAS.core.das_core import DASCore
from DAS.utils.utils import getarg, access
from DAS.web.utils import urllib2_request, json2html, web_time
import DAS.utils.jsonwrapper as json

import sys
if sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")

def ajax_response_orig(msg, tag="_response", element="object"):
    """AJAX response wrapper"""
    page  = """<ajax-response><response type="%s" id="%s">""" % (element, tag)
    page += msg
    page += "</response></ajax-response>"
    print page
    return page

def ajax_response(msg):
    """AJAX response wrapper"""
    page  = """<ajax-response>"""
    page += "<div>" + msg + "</div>"
    page += "</ajax-response>"
    return page

class DASSearch(TemplatedPage):
    """
    DAS web interface.
    """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        cdict           = self.config.dictionary_()
        self.cachesrv   = getarg(cdict, 'cache_server_url', 
                            'http://localhost:8011')
        self.dasmgr     = DASCore()
        self.dasmapping = self.dasmgr.mapping
        self.daslogger  = self.dasmgr.logger
        self.pageviews  = ['xml', 'list', 'json', 'yuijson'] 

    def top(self):
        """
        Define masthead for all DAS web pages
        """
        return self.templatepage('das_top')

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
        page = self.templatepage('das_faq')
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
        das_keys = self.dasmgr.das_keys()
        if  not uinput:
            return self.templatepage('das_ambiguous', 
                    input=uinput, entities=','.join(das_keys))
        # check provided input. If at least one word is not part of das_keys
        # return ambiguous template.
        for word in uinput.split():
            found = 0
            for key in das_keys:
                if  word.find(key) != -1:
                    found = 1
            if  not found:
                return self.templatepage('das_ambiguous', 
                        input=uinput, entities=', '.join(das_keys))
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
            self.daslogger.error(traceback.format_exc())
            error  = "args: %s\nkwargs: %s\n" % (args, kwargs)
            error += "Exception type: %s\nException value: %s\nTime: %s" \
                        % (sys.exc_type, sys.exc_value, web_time())
            error = error.replace("<", "").replace(">", "")
            return self.error(error)

    @expose
    def form(self, uinput=None, msg=None):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=uinput, msg=msg)
        return page

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
            elif  kwargs and kwargs.has_key('_id'):
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
            params   = {'query':json.dumps(query), 'idx':idx, 'limit':limit}
            path     = '/rest/request'
            headers  = {"Accept": "application/json"}
            data     = json.loads(
            urllib2_request('GET', url+path, params, headers=headers))
            if  type(data) is types.StringType:
                result = json.loads(data)
            else:
                result = data
            res = ""
            if  result['status'] == 'success':
                if  recordid: # we got id
                    for row in result['data']:
                        if  show == 'json':
                            jsoncode = {'jsoncode': json2html(row, "")}
                            res += self.templatepage('das_json', **jsoncode)
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
            self.daslogger.error(traceback.format_exc())
            error  = "args: %s\nkwargs: %s\n" % (args, kwargs)
            error += "Exception type: %s\nException value: %s\nTime: %s" \
                        % (sys.exc_type, sys.exc_value, web_time())
            error = error.replace("<", "").replace(">", "")
            return self.error(error)

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
        result  = json.loads(
        urllib2_request('GET', url+path, params, headers=headers))
        if  type(result) is types.StringType:
            data = json.loads(result)
        else:
            data = result
        if  data['status'] == 'success':
            return data['nresults']
        else:
            msg = "nresults returns status: %s" % str(data)
            self.daslogger.info(msg)
        self.send_request('POST', kwargs)
        return 0

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
        res     = json.loads(
        urllib2_request(method, url+path, params, headers=headers))
        return res

    def result(self, kwargs):
        """
        invoke DAS search call, parse results and return them to
        web methods
        """
        result  = self.send_request('GET', kwargs)
        if  type(result) is types.StringType:
            data = json.loads(result)
        else:
            data = result
        if  data['status'] == 'success':
            res    = data['data']
        elif data['status'] == 'not found':
            res  = []
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
        total   = self.nresults(kwargs)
        if  not total:
            ctime   = (time.time()-time0)
#            page    = self.templatepage('not_ready')
            page    = self.status(input=uinput)
            page    = self.page(form + page, ctime=ctime)
            return page

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
#                style = "blue" 
        style = "white"
        for row in rows:
            id    = row['_id']
            rec   = '<a href="/das/records/%s">%s</a>, ' % (id, id)
            page += '<div class="%s"><hr class="line" /><b>Record</b> %s<br />'\
                % (style, rec)
            gen   = self.convert2ui(row)
            for uikey, value in [k for k, g in groupby(gen)]:
                page += "<b>%s</b>: %s, " % (uikey, value)
            pad   = ""
            if  show == 'json':
                jsoncode = {'jsoncode': json2html(row, pad)}
                jsonhtml = self.templatepage('das_json', **jsoncode)
                jsondict = dict(data=jsonhtml, id=id)
                page += self.templatepage('das_row', **jsondict)
            else:
                code  = yaml.dump(row, width=100, indent=4, 
                                default_flow_style=False)
                data  = self.templatepage('das_code', code=code)
                datadict = {'data':data, 'id':id}
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
        print "\n\n#### yuijson", rows
        rowlist = []
        id = 0
        for row in rows:
            das = row['das']
            if  type(das) is types.DictType:
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
                    if  type(data) is types.ListType:
                        data = data[jdx]
                    if  type(data) is types.ListType:
                        data = data[idx]
                    # I need to extract from DAS object the values for UI keys
                    print "corresponding data", data
                    for item in self.dasmapping.presentation(key):
                        daskey = item['das']
                        uiname = item['ui']
                        if  not resdict.has_key(uiname):
                            resdict[uiname] = ""
                        print "daskey", daskey, uiname
                        # look at key attributes, which may be compound as well
                        # e.g. block.replica.se
                        if  type(data) is types.DictType:
                            result = dict(data)
                        elif type(data) is types.ListType:
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
                        print "resdict", resdict
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
        print "\n\njsondict", jsondict
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
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        img = '<img src="/dascontrollers/images/loading.gif" alt="loading"/>'
        req = """
        <script type="application/javascript">
        setTimeout('ajaxStatus()',3000)
        </script>"""

        def set_header():
            "Set HTTP header parameters"
            tstamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            cherrypy.response.headers['Expire'] = tstamp
            cherrypy.response.headers['Cache-control'] = 'no-cache'

        uinput  = getarg(kwargs, 'input', '')
        uinput  = urllib.unquote_plus(uinput)
        view    = getarg(kwargs, 'view', 'list')
        params  = {'query':uinput}
        path    = '/rest/status'
        url     = self.cachesrv
        headers = {'Accept': 'application/json'}
        result  = json.loads(
        urllib2_request('GET', url+path, params, headers=headers))
        if  type(result) is types.StringType:
            data  = json.loads(result)
        else:
            data  = result
        if  data['status'] == 'ok':
            page  = '<script type="application/javascript">reload()</script>'
        else:
            page  = img + ' ' + data['status'] + ', please wait...'
            page += req
            set_header()
        page = ajax_response(page)
        return page

