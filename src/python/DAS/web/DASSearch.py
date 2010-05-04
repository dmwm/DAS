#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS web interface, based on WMCore/WebTools
"""

__revision__ = "$Id: DASSearch.py,v 1.8 2009/05/28 18:59:12 valya Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "Valentin Kuznetsov"

# system modules
import time
import types
import thread
from cherrypy import expose

# WMCore/WebTools modules
from WMCore.WebTools.Page import TemplatedPage
from WMCore.WebTools.Page import exposedasjson, exposedasxml, exposetext

# DAS modules
from DAS.core.das_core import DASCore
from DAS.utils.utils import getarg

import sys
if sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")


class DASSearch(TemplatedPage):
    """
    represents DAS web interface
    """
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
#        self.dasmgr = DASCore(mode='html', debug=1)
        self.dasmgr = DASCore()
        self.pageviews = ['xml', 'list', 'table', 'plain', 'json'] 
        self.cleantime = 60 # in seconds
        self.lastclean = time.time()

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

    def bottom(self):
        """
        Define footer for all DAS web pages
        """
        return self.templatepage('das_bottom')

    def page(self, content, ctime=None):
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
                                                timestamp=timestamp)
        return page

    @expose
    def faq(self, *args, **kwargs):
        """
        represent DAS FAQ.
        """
        page = self.templatepage('das_faq')
        return self.page(page)

    @expose
    def help(self, *args, **kwargs):
        """
        represent DAS help
        """
        page = self.templatepage('das_help')
        return self.page(page)

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
        return self.page(page)

    @expose
    def views(self, *args, **kwargs):
        """
        represent DAS views.
        """
        views = self.dasmgr.get_view()
        page  = self.templatepage('das_views', views=views, msg='')
        return self.page(page)

    @expose
    def services(self, *args, **kwargs):
        """
        represent DAS services
        """
        keys = self.dasmgr.keys()
        page = self.templatepage('das_services', service_keys=keys)
        return self.page(page)

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
    def form(self, uinput=None):
        """
        provide input DAS search form
        """
        page = self.templatepage('das_searchform', input=uinput)
        return page

    def result(self, kwargs):
        """
        invoke DAS search call, parse results and return them to
        web methods
        """
        # invoke cache cleaner if time since last clean exceed cleantime.
        if  (time.time() - self.lastclean) > self.cleantime:
            thread.start_new_thread(self.dasmgr.clean_cache, ('couch', ))
            self.lastclean = time.time()

        uinput  = getarg(kwargs, 'input', '')
        format  = getarg(kwargs, 'format', '')
        # NOTE: the current implementation of table UI, using YUI,
        # is done in JavaScript, which by itself doesn't recognize
        # a.c notations. So I replace a key in a dict from a.c form
        # to a_dot_c one. If any value is a dict itself, for HTML
        # format we take its string representation and replace 
        # curle brackets with appropriate HTML symbols.
        res     = []
        idx     = 0
        for item in self.dasmgr.result(uinput):
            item['id'] = idx
            if  format == 'html':
                for key, val in item.items():
                    if  type(val) is types.DictType:
                        newval = str(val).replace('{', '&#123;')
                        newval = newval.replace('}', '&#125;')
                        item[key] = newval.replace("'", '')
                    if  key.find('.') != -1:
                        item[key.replace('.', '_dot_')] = item[key]
                for key in item.keys():
                    if  key.find('.') != -1:
                        del(item[key])
                    
                res.append(item)
            else:
                res.append(item)
            idx += 1

#        res     = self.dasmgr.result(uinput)
        titles  = res[0].keys()
        titles.sort()
        titles.remove('id')
        titles  = ['id'] + titles
        form    = self.form(uinput=uinput)
        return titles, res, form

    @exposedasxml
    def xmlview(self, kwargs):
        """
        provide DAS XML
        """
        titles, rows, form = self.result(kwargs)
        names = {'resultlist': rows}
        page  = self.templatepage('das_xml', **names)
        return page

    @exposedasjson
    def jsonview(self, kwargs):
        """
        provide DAS JSON
        """
        titles, rows, form = self.result(kwargs)
        return rows

    @expose
    def listview(self, kwargs):
        """
        provide DAS list view
        """
        time0   = time.time()
        titles, rows, form = self.result(kwargs)
        ctime   = (time.time()-time0)
        nrows   = len(rows)
        limit   = getarg(kwargs, 'limit', nrows)
        names   = {'titlelist':titles, 'nrows':nrows, 'limit':limit,
                   'resultlist': rows, 'form':form}
        page    = self.templatepage('das_list', **names)
        return self.page(page, ctime=ctime)

    @exposetext
    def plainview(self, kwargs):
        """
        provide DAS plain view
        """
        titles, rows, form = self.result(kwargs)
        page = ""
        for item in rows:
            item  = str(item).replace('[','').replace(']','')
            page += "%s\n" % item.replace("'","")
        return page

    @expose
    def tableview(self, kwargs):
        """
        provide DAS table view
        """
        kwargs['format'] = 'html'
        time0 = time.time()
        titles, rows, form = self.result(kwargs)
        coldefs = ""
        for title in titles:
            coldefs += "{key:'%s',label:'%s',sortable:true,resizeable:true}," \
                        % (title, title)
        coldefs = "[%s]" % coldefs[:-1] # remove last comma
        coldefs = coldefs.replace("},{","},\n{")
        nrows   = len(rows)
        limit   = getarg(kwargs, 'limit', nrows)
        if  not limit: # if no limit provided we use full range
            limit = nrows
        results = {'records':rows, 'recordsReturned': nrows,
                   'totalRecords': nrows, 'startIndex':0,
                   'sort':'true', 'dir':'asc'}
        names   = {'titlelist':titles, 'results':results, 'form':form,
                   'coldefs':coldefs, 'nrows':nrows, 'rowsperpage':limit,
                   'tag':'mytag'}
        page    = self.templatepage('das_table', **names)
        ctime   = (time.time()-time0)
        return self.page(page, ctime)

    @expose
    def default(self, *args, **kwargs):
        """
        default method
        """
        return self.index(args, kwargs)
