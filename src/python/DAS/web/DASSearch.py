#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface, based on WMCore/WebTools
"""

__revision__ = "$Id: DASSearch.py,v 1.16 2009/07/09 16:00:02 valya Exp $"
__version__ = "$Revision: 1.16 $"
__author__ = "Valentin Kuznetsov"

# system modules
import time
import types
import urllib
from cherrypy import expose
import cherrypy
try:
    # Python 2.6
    import json
    from json import JSONDecoder
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
    from simplejson import JSONDecoder

# WMCore/WebTools modules
from WMCore.WebTools.Page import TemplatedPage
from WMCore.WebTools.Page import exposedasjson, exposedasxml, exposetext
from WMCore.WebTools.Page import exposejson

# DAS modules
from DAS.core.das_core import DASCore
from DAS.utils.utils import getarg
from DAS.web.utils import urllib2_request

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
    print page
    return page

class DASSearch(TemplatedPage):
    """
    DAS web interface.
    """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        cdict          = self.config.dictionary_()
        self.cachesrv  = getarg(cdict, 'cache_server_url', 'http://localhost:8011')
        self.dasmgr = DASCore()
        self.pageviews = ['xml', 'list', 'table', 'plain', 'json', 'yuijson'] 
        self.cleantime = 60 # in seconds
        self.lastclean = time.time()
        self.decoder   = JSONDecoder()
        self.counter = 0 # TMP stuff, see request, TODO: remove

        # TMP: I define a few useful views, this should be done
        # elswhere (may be here, may be in external configuration,
        # may be in couchdb
#        query  = 'find dataset, count(file), sum(file.size)'
        query  = 'find dataset, dataset.createdate, dataset.createby, '
        query += 'sum(block.size), sum(file.numevents), count(file)'
        self.dasmgr.create_view('dataset', query)
        query  = 'find block.name, block.size, block.numfiles, '
        query += 'block.numevents, block.status, block.createby, '
        query += 'block.createdate, block.modby, block.moddate'
        self.dasmgr.create_view('block', query)
        query  = 'find site, sum(block.numevents), '
        query += 'sum(block.numfiles), sum(block.size)'
        self.dasmgr.create_view('site', query)
        query  = 'find datatype, dataset, run.number, run.numevents, '
        query += 'run.numlss, run.totlumi, run.store, run.starttime, '
        query += 'run.endtime, run.createby, run.createdate, run.modby, '
        query += 'run.moddate, count(file), sum(file.size), '
        query += 'sum(file.numevents)'
        self.dasmgr.create_view('run', query)

#    def clean_couch(self):
#        """
#        Clean couch DB
#        """
#        self.dasmgr.clean_cache('couch')

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
    def create_view(self, *args, **kwargs):
        """
        create DAS view.
        """
        msg   = ''
        if  kwargs.has_key('name') and kwargs.has_key('query'):
            name  = kwargs['name']
            query = kwargs['query']
            try:
                self.dasmgr.create_view(name, query)
                msg = "View '%s' has been created" % name
            except Exception, exc:
                msg = "Fail to create view '%s' with query '%s'" \
                % (name, query)
                msg += '</br>Reason: <span class="box_blue">' 
                msg += exc.message + '</span>'
                pass
        views = self.dasmgr.get_view()
        page  = self.templatepage('das_views', views=views, msg=msg)
        return self.page(page, response_div=False)

    @expose
    def views(self, *args, **kwargs):
        """
        represent DAS views.
        """
        views = self.dasmgr.get_view()
        page  = self.templatepage('das_views', views=views, msg='')
        return self.page(page, response_div=False)

    @expose
    def services(self, *args, **kwargs):
        """
        represent DAS services
        """
        keys = self.dasmgr.keys()
        page = self.templatepage('das_services', service_keys=keys)
        return self.page(page, response_div=False)

    @expose
    def index(self, *args, **kwargs):
        """
        represents DAS web interface. 
        It uses das_searchform template for
        input form and yui_table for output Table widget.
        """
        if  not args and not kwargs:
            page = self.form()
            return self.page(page)
        view = getarg(kwargs, 'view', 'table')
        if  args:
            return getattr(self, args[0][0])(args[1])
        if  view not in self.pageviews:
            raise Exception("Page view '%s' is not supported" % view)
        return getattr(self, '%sview' % view)(kwargs)

    @expose
    def form(self, uinput=None, msg=None):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=uinput, msg=msg)
        return page

    def result(self, kwargs):
        """
        invoke DAS search call, parse results and return them to
        web methods
        """
        url    = self.cachesrv
        uinput = getarg(kwargs, 'input', '')
        format = getarg(kwargs, 'format', '')
        idx    = getarg(kwargs, 'idx', 0)
        limit  = getarg(kwargs, 'limit', 10)
        skey   = getarg(kwargs, 'sort', '')
        sdir   = getarg(kwargs, 'dir', 'asc')
#        params = {'query':uinput, 'idx':idx, 'limit':limit}
        params = {'query':uinput, 'idx':idx, 'limit':limit, 
                  'skey':skey, 'order':sdir}
        path   = '/rest/json/GET'
        result = self.decoder.decode(urllib2_request(url+path, params))
        if  type(result) is types.StringType:
            data = json.loads(result)
        else:
            data = result
        titles = []
        res    = []
        total  = 0
        form   = self.form(uinput=uinput)
        if  data['status'] == 'success':
            res    = data['data']
            titles = res[0].keys()
            titles.sort()
            if  'id' in titles:
                titles.remove('id')
            titles = ['id'] + titles
            total  = data['nresults']
            form   = self.form(uinput=uinput)
        elif data['status'] == 'not found':
            # request data via POST
            path   = '/rest/json/POST'
            result = self.decoder.decode(urllib2_request(url+path, params))
            if  type(result) is types.StringType:
                data = json.loads(result)
            else:
                data = result
            # TODO: place AJAX message which will try to retrieve results again
            msg    = data['status']
            form   = self.form(uinput=uinput, msg=msg)
        return titles, res, total, form
        
    @exposedasxml
    def xmlview(self, kwargs):
        """
        provide DAS XML
        """
        titles, rows, total, form = self.result(kwargs)
        names = {'resultlist': rows}
        page  = self.templatepage('das_xml', **names)
        return page

    @exposedasjson
    def jsonview(self, kwargs):
        """
        provide DAS JSON
        """
        titles, rows, total, form = self.result(kwargs)
        return rows

    @expose
    def listview(self, kwargs):
        """
        provide DAS list view
        """
        time0   = time.time()
        titles, rows, total, form = self.result(kwargs)
        nrows   = len(rows)
        limit   = getarg(kwargs, 'limit', nrows)
        names   = {'titlelist':titles, 'nrows':total, 'limit':limit,
                   'resultlist': rows}
        page    = self.templatepage('das_list', **names)
        uinput  = getarg(kwargs, 'input', '')
        form    = self.form(uinput=uinput)
        ctime   = (time.time()-time0)
        ajaxreq = getarg(kwargs, 'ajax', 0)
        ajax    = self.templatepage('das_ajaxrequest', ajax=ajaxreq)
        return self.page(form + ajax + page, ctime=ctime)

    @exposetext
    def plainview(self, kwargs):
        """
        provide DAS plain view
        """
        titles, rows, total, form = self.result(kwargs)
        page = ""
        for item in rows:
            item  = str(item).replace('[','').replace(']','')
            page += "%s\n" % item.replace("'","")
        return page

    @exposejson
    def yuijson(self, **kwargs):
        """
        provide JSON in YUI compatible format to be used in DynamicData table
        widget, see
        http://developer.yahoo.com/yui/examples/datatable/dt_dynamicdata.html
        """
        titles, rows, total, form = self.result(kwargs)
        idx      = getarg(kwargs, 'idx', 0)
        limit    = getarg(kwargs, 'limit', 10)
        jsondict = {'recordsReturned': len(rows),
                   'totalRecords': total, 'startIndex':idx,
                   'sort':'true', 'dir':'asc',
                   'pageSize': limit,
                   'records': rows}
        return jsondict

    @expose
    def tableview(self, kwargs):
        """
        provide DAS table view
        """
        kwargs['format'] = 'html'
        time0 = time.time()
        titles, rows, total, form = self.result(kwargs)
        coldefs = ""
        for title in titles:
            coldefs += "{key:'%s',label:'%s',sortable:true,resizeable:true}," \
                        % (title, title)
        coldefs = "[%s]" % coldefs[:-1] # remove last comma
        coldefs = coldefs.replace("},{","},\n{")
        nrows   = len(rows)
        limit   = getarg(kwargs, 'limit', nrows)
        uinput  = getarg(kwargs, 'input', '')
        ajaxreq = getarg(kwargs, 'ajax', 0)
        if  not limit: # if no limit provided we use full range
            limit = nrows
        names   = {'titlelist':str(titles).replace("u'", "'"), 
                   'coldefs':coldefs, 'rowsperpage':limit,
                   'total':total, 'tag':'mytag', 'ajax':ajaxreq,
                   'input':uinput}
        page    = self.templatepage('das_table', **names)
        uinput  = getarg(kwargs, 'input', '')
        form    = self.form(uinput=uinput)
        ctime   = (time.time()-time0)
        ajax    = self.templatepage('das_ajaxrequest', ajax=ajaxreq)
        page    = self.page(form + ajax + page, ctime=ctime)
        return page

    @expose
    def default(self, *args, **kwargs):
        """
        default method
        """
        return self.index(args, kwargs)

    @expose
    def request(self, **kwargs):
        """
        Place request to obtain status about given query
        """
        cherrypy.response.headers['Content-Type'] = 'text/xml'
        img = """<img src="/dascontrollers/images/loading.gif" alt="loading" /> """
        req = """<script type="application/javascript">setTimeout('ajaxRequest()', 3000)</script>"""
#        page = req
#        page += "Got query, counter %s, %s" % (kwargs, self.counter)
        print "\n### request kwargs", kwargs, self.counter

        def send_post(idict):
            "Send POST request to server with provided parameters"
            params = {'query':idict['query']}
            url  = self.cachesrv
            path = '/rest/json/POST' 
            res  = self.decoder.decode(urllib2_request(url+path, params))
        def set_header():
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            cherrypy.response.headers['Expire'] = timestamp
            cherrypy.response.headers['Cache-control'] = 'no-cache'

        uinput = getarg(kwargs, 'input', '')
        uinput = urllib.unquote_plus(uinput)
        view   = getarg(kwargs, 'view', 'table')
        params = {'query':uinput, 'idx':0, 'limit':1}
        path   = '/rest/json/GET'
        url    = self.cachesrv
        result = self.decoder.decode(urllib2_request(url+path, params))
        if  type(result) is types.StringType:
            data  = json.loads(result)
        else:
            data  = result
        if  data['status'] == 'success':
            page  = """<script type="application/javascript">reload()</script>"""
        elif data['status'] == 'in raw cache':
            page  = img + 'data found in raw cache'
            page += req
            send_post(params)
            set_header()
        elif data['status'] == 'requested':
            page  = img + 'data has been requested'
            page += req
            set_header()
        elif data['status'] == 'waiting in queue':
            page  = img + 'your request is waiting in queue'
            page += req
            set_header()
        elif data['status'] == 'not found':
            page  = 'data not in DAS yet, please retry'
            send_post(params)
            set_header()
        else:
            page  = 'unknown status, %s' % urllib.urlencode(str(data))
#        
#        if  self.counter == 5:
#            page = "TEST DONE"
#        self.counter += 1
        page = ajax_response(page)
        print "\n### AJAX response",page
        return page

