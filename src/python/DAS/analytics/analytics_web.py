#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Analytics web server
"""
__author__ = "Gordon Ball"

import logging
import cherrypy
import collections
import time
import json
from cherrypy.process.plugins import PIDFile

from DAS.analytics.analytics_config import DASAnalyticsConfig
from DAS.web.das_webmanager import DASWebManager
from DAS.analytics.analytics_utils import WebHandler
from DAS.analytics.analytics_task import Task
import DAS

#This extensively borrows from DAS.web

def json_requested():
    return 'accept' in cherrypy.request.headers and \
           'json' in cherrypy.request.headers['accept']

DASAnalyticsConfig.add_option("web_port",
                              type=int,
                              default=8213,
      help="Cherrypy serve port.")
DASAnalyticsConfig.add_option("web_history",
                              type=int,
                              default=10000,
      help="Length limit of the web server deques")
DASAnalyticsConfig.add_option("web_base",
                              type=basestring,
                              default='/analytics',
      help="Base path for analytics web")

class DASAnalyticsWebManager(DASWebManager):
    """
    Use the DASWebManager class as the root of our interfaces,
    replacing the default header and footer.
    """
    def __init__(self, config, root):
        self.config = config
        self.root = root
        DASWebManager.__init__(self, config)
        self.base = config.web_base
    
    def templatepage(self, tmpl, **kwargs):
        """
        Intercept template requests, and return a raw dictionary if json.
        """
        if json_requested():
            kwargs['template'] = tmpl
            return json.dumps(kwargs, default=str)
        else:
            return super(DASAnalyticsWebManager, self).templatepage(tmpl, **kwargs)
        
    def page(self, content):
        """
        Intercept page requests, and return no headers if json.
        """
        if json_requested():
            return content
        else:
            return super(DASAnalyticsWebManager, self).page(content)
    
    @cherrypy.expose
    def index(self, *args, **kwargs):
        "Show a main page with a list of other pages."
        return self.page(self.templatepage("analytics_main",
                                           base=self.base))
    
    def error(self, error, **kwargs):
        page = self.templatepage("analytics_error",
                                 error=error,
                                 request=cherrypy.request.request_line,
                                 params=cherrypy.request.params,
                                 headers=cherrypy.request.headers,
                                 extra=kwargs)
        return self.page(page)
    
    def top(self):
        """
        Provide masthead for all web pages
        """
        return self.templatepage('analytics_header', base=self.base)

    @cherrypy.expose
    def doc(self):
        """Return templated (essentially static) documentation page"""
        return self.page(self.templatepage("analytics_doc",
                                           base=self.base))

    def bottom(self):
        """
        Provide footer for all web pages
        """
        return self.templatepage('analytics_bottom', version=DAS.version)
    
    @cherrypy.expose    
    def schedule(self, *path, **attrs):
        "Show the currently scheduled and running tasks"
        task_schedule = self.root.controller.scheduler.get_scheduled()
        task_schedule = sorted(task_schedule, key=lambda x: x['at'])
        next = None
        now = time.time()
        if 'next' in attrs:
            next = int(attrs['next'])
            before = now + next
            task_schedule = filter(lambda x: x['at'] < before, 
                                   task_schedule)
        
        task_running = self.root.controller.scheduler.get_running()
        
        task_running = sorted(task_running, key=lambda x: x['started'])
        page = self.templatepage("analytics_schedule",
                                 schedule=task_schedule,
                                 running=task_running,
                                 base=self.base,
                                 now=now)
        return self.page(page)
    
    @cherrypy.expose
    def logger(self, *path, **attrs):
        "Show the current log."
        log_entries = [r for r in self.root.log_data]
        match = '.'.join(path)
        child_match = match + '.'
        children = json.loads(attrs.get('children', 'true'))       
        if match and children:
            
            
            log_entries = filter(lambda x: x.name.startswith(match), log_entries)
            child_names = [r.name.split('.') for r in log_entries]
            
        elif match:
            child_names = [r.name.split('.') for r in log_entries 
                           if r.name.startswith(child_match)]
            log_entries = filter(lambda x: x.name == match)
        else:
            child_names = [r.name.split('.') for r in log_entries]
        
        
        filters = set([n[len(path)] for n in child_names 
                       if len(n) > len(path) 
                       and all([nn == pp 
                                for nn, pp in zip(n[:len(path)], path)])])
        
        now = time.time()
        last = None
        if 'last' in attrs:
            last = int(attrs['last'])
            log_entries = filter(lambda x: now - x.created < last, log_entries)
        
        level = None
        if 'level' in attrs:
            level = int(attrs['level'])
            log_entries = filter(lambda x: x.level >= level, log_entries)
        
        total = len(log_entries)
        
        offset = 0
        if 'offset' in attrs:
            offset = int(attrs['offset'])
            log_entries = log_entries[offset:]
        
        limit = None
        if 'limit' in attrs:
            limit = int(attrs['limit'])
            if limit > 0:
                log_entries = log_entries[:limit]
        else:
            if len(log_entries) > 100:
                limit = 100
                log_entries = log_entries[:limit]
        
        
        page = self.templatepage('analytics_log',
                                 entries=log_entries,
                                 filters=filters,
                                 textpath='.'.join(path),
                                 path=self.base + '/logger/' + '/'.join(path),
                                 base=self.base,
                                 offset=offset,
                                 limit=limit,
                                 last=last,
                                 total=total,
                                 level=level)
        return self.page(page)
    
    @cherrypy.expose
    def task(self, *path, **attrs):
        """
        Show detailed information for a single path.
        """
        if not 'id' in attrs:
            return self.error("Argument 'id' required")
        master_id = attrs['id']
        task = self.root.controller.scheduler.get_task(master_id)
                
        if task:
            page = self.templatepage('analytics_task',
                                     base=self.base,
                                     id=master_id,
                                     **task)
        else:
            return self.error("Requested ID does not exist", 
                              id=master_id)
        return self.page(page)
        
    
    @cherrypy.expose
    def control(self, *path, **attrs):
        """
        Show a page with some control options.
        """
        
        if 'key' in attrs and 'value' in attrs:
            try:
                value = json.loads(attrs['value'])
            except:
                return self.error("JSON decode of value failed",
                                  value=attrs['value'])
            result = self.config.set_option(attrs['key'], value)
            if not result == True:
                return self.error("Setting of option failed",
                                  response=result)
                
        
        page = self.templatepage('analytics_control',
                                 base=self.base,
                                 config=self.config.get_dict())
        return self.page(page)
        
    
    @cherrypy.expose
    def results(self, *path, **attrs):
        """
        Show a possibly-filtered list of task results.
        """
        results = [r for r in self.root.result_data]
        master = None
        if 'master' in attrs:
            master = attrs['master']
            results = filter(lambda x: x['master_id'] == master, results)
        parent = None
        if 'parent' in attrs:
            parent = attrs['parent']
            results = filter(lambda x: x['parent'] == parent, results)
        error = False
        if 'error' in attrs:
            error = True
            results = filter(lambda x: not x['success'], results)
        success = False
        if 'success' in attrs:
            success = True
            results = filter(lambda x: x['success'], results)
        last = None
        if 'last' in attrs:
            last = int(attrs['last'])
            since = time.time() - last
            results = filter(lambda x: x['finish_time'] > since, results)
            
        total = len(results)
        
        offset = 0
        if 'offset' in attrs:
            offset = int(attrs['offset'])
            results = results[offset:]
        
        limit = None
        if 'limit' in attrs:
            limit = int(attrs['limit'])
            if limit > 0:
                results = results[:limit]
        else:
            if len(results) > 100:
                limit = 100
                results = results[:limit]
        
        page = self.templatepage('analytics_results',
                                 results=results,
                                 base=self.base,
                                 total=total,
                                 offset=offset,
                                 limit=limit,
                                 master=master,
                                 parent=parent,
                                 error=error,
                                 success=success,
                                 last=last)
        return self.page(page)
        
    
    @cherrypy.expose
    def result(self, *path, **attrs):
        """
        Show in detail a single result. It is intended
        that a result can provide a special key ("render")
        which names a special template/class to display it
        (eg requesting graphs, etc)
        """
        if not 'id' in attrs:
            return self.error("Argument 'id' required")
        task_id = attrs['id']
        result = None
        for r in self.root.result_data:
            if r['task_id'] == task_id:
                result = r
                break
        if not result:
            return self.error("Requested ID does not exist", 
                              id=task_id)
        
        page = self.templatepage('analytics_result',
                                 base=self.base,
                                 result=result)
        return self.page(page)
        
    
    @cherrypy.expose
    def remove_task(self, *path, **attrs):
        """
        Remove a task from the schedule. TODO: Auth.
        """
        if not 'id' in attrs:
            return self.error("Argument 'id' required")
        master_id = attrs['id']
        result = self.root.controller.scheduler.remove_task(master_id)
        if result:
            return self.schedule()
        else:
            return self.error("Unable to remove task. It probably didn't exist.",
                              id=master_id)
    
    @cherrypy.expose
    def reschedule_task(self, *path, **attrs):
        """
        Reschedule a task to a new time. TODO: Auth
        """
        if not ('id' in attrs and 'at' in attrs):
            return self.error("Arguments 'id' and 'at' required")
        master_id = attrs['id']
        when = float(attrs['at'])
        result = self.root.controller.scheduler.reschedule_task(master_id, 
                                                                when)
        if result:
            return self.schedule()
        else:
            return self.error("Unable to reschedule task. It probably didn't exist.",
                              id=master_id,
                              at=when)
        
    @cherrypy.expose
    def add_task(self, *path, **attrs):
        """
        Add a new task, for an existing class.
        TODO: Auth
        TODO: accept a single dictionary containing all arguments
        """
        if not ('name' in attrs 
                and 'classname' in attrs 
                and 'interval' in attrs):
            return self.error("Arguments 'name', 'classname' and 'interval' required.")
        
        try:
            kwargs = json.loads(attrs.get('kwargs', "{}"))
            for k, v in kwargs.items():
                del kwargs[k]
                #kwargs must be string, not unicode
                kwargs[k.encode('ascii')] = v
            task = Task(name=attrs['name'],
                        classname=attrs['classname'],
                        interval=float(attrs['interval']),
                        kwargs=kwargs,
                        only_once=json.loads(attrs.get('only_once', 'false')),
                        max_runs=json.loads(attrs.get('max_runs', 'null')),
                        only_before=json.loads(attrs.get('only_before', 'null')))
        except:
            return self.error("There was an error decoding the arguments.")
        try:
            self.root.controller.scheduler.add_task(task,
                                when=json.loads(attrs.get('when', 'null')),
                                offset=json.loads(attrs.get('offset', 'null')))
            
            return self.task(id=task.master_id)
        except:
            return self.error("There was an error adding the task.")
    

class DASAnalyticsWeb:
    """
    DAS Analytics web class to be served under CherryPy server.
    """
    def __init__(self, config, controller):
        self.config = config
        self.logger = logging.getLogger('DASAnalytics.Web')
        self.controller = controller
        
        self.log_data = collections.deque(maxlen=config.web_history)
        self.result_data = collections.deque(maxlen=config.web_history)
        self.pid = config.get('pid', '/tmp/das_analytics.pid')

    def start(self):
        """
        Set up the cherrypy server.
        """
        self.controller.logger.addHandler(WebHandler(self.log_data))
        self.controller.scheduler.add_callback(self.result_data.appendleft)
        
        cherrypy.config["engine.autoreload_on"] = False
        cherrypy.config["server.socket_port"] = self.config.web_port
        
        cherrypy.tree.mount(DASAnalyticsWebManager(self.config, self),
                            self.config.web_base)
        
        pid = PIDFile(cherrypy.engine, self.pid)
        pid.subscribe()

        cherrypy.engine.start()
        cherrypy.engine.block()
