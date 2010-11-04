import logging
import cherrypy
import collections
import time
import json
import atexit

from DAS.analytics.analytics_config import DASAnalyticsConfig
from DAS.web.das_webmanager import DASWebManager
from DAS.analytics.analytics_utils import WebHandler
from DAS.analytics.analytics_task import Task
import DAS

#This extensively borrows from DAS.web

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
        
    @cherrypy.expose
    def index(self, *args, **kwargs):
        "Show a main page with a list of other pages."
        return self.page(self.templatepage("analytics_main",
                                           base=self.base))
    
    def top(self):
        """
        Provide masthead for all web pages
        """
        return self.templatepage('analytics_header',
                                 base=self.base,
                                 yui=self.yuidir)

    def bottom(self):
        """
        Provide footer for all web pages
        """
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        return self.templatepage('analytics_bottom',
                timestamp=timestamp, version=DAS.version)
    
    @cherrypy.expose    
    def schedule(self, *path, **attrs):
        "Show the currently scheduled and running tasks"
        task_schedule = self.root.controller.scheduler.get_scheduled()
        task_schedule = sorted(task_schedule, key=lambda x: x['at'])
        next = None
        if 'next' in attrs:
            next = int(attrs['next'])
            before = time.time() + next
            task_schedule = filter(lambda x: x['at'] < before, 
                                   task_schedule)
        
        task_running = self.root.controller.scheduler.get_running()
        
        task_running = sorted(task_running, key=lambda x: x['started'])
            
        page = self.templatepage("analytics_schedule",
                                 schedule=task_schedule,
                                 running=task_running,
                                 base=self.base)
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
    def shutdown(self, *path, **attrs):
        """
        Shut down the main loop and kill the cherrypy server.
        Should probably require some auth, if kept at all
        """
        self.root.controller.stop()
    
    @cherrypy.expose
    def task(self, *path, **attrs):
        """
        Show detailed information for a single path.
        """
        if not 'id' in attrs:
            return self.page("No id specified. Cannot display.")
        master_id = attrs['id']
        task = self.root.controller.scheduler.get_task(master_id)
        if not task:
            return self.page("Specified id does not exist. Cannot display")
        page = self.templatepage('analytics_task',
                                 base=self.base,
                                 **task)
        return self.page(page)
        
    
    @cherrypy.expose
    def control(self, *path, **attrs):
        """
        Show a page with some control options.
        """
        page = self.templatepage('analytics_control',
                                 base=self.base,
                                 config=self.config._options)
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
            return self.page("No id specified. Cannot display.")
        task_id = attrs['id']
        result = None
        for r in self.root.result_data:
            if r['task_id'] == task_id:
                result = r
                break
        if not result:
            return self.page("Specified id not found. Cannot display.")
        
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
            return self.page("No id specified. Cannot remove.")
        master_id = attrs['id']
        result = self.root.controller.scheduler.remove_task(master_id)
        if result:
            return self.page("Task removed.")
        else:
            return self.page("Task removal failed. Probably didn't exist.")
    
    @cherrypy.expose
    def reschedule_task(self, *path, **attrs):
        """
        Reschedule a task to a new time. TODO: Auth
        """
        if not ('id' in attrs and 'at' in attrs):
            return self.page("Either (id,at) not specified. Cannot reschedule.")
        master_id = attrs['id']
        when = int(attrs['at'])
        result = self.root.controller.scheduler.reschedule_task(master_id, 
                                                                when)
        if result:
            return self.page("Task rescheduled.")
        else:
            return self.page("Task rescheduling failed. Probably didn't exist.")
        
    @cherrypy.expose
    def add_task(self, *path, **attrs):
        """
        Add a new task, for an existing class.
        TODO: Auth
        """
        if not ('name' in attrs 
                and 'classname' in attrs 
                and 'interval' in attrs):
            return self.page("Require at least (name,classname,interval).")
        
        try:
            kwargs = json.loads(attrs.get('kwargs', "{}"))
            for k, v in kwargs.items():
                del kwargs[k]
                #kwargs must be string, not unicode
                kwargs[k.encode('ascii')] = v
            task = Task(name=attrs['name'],
                        classname=attrs['classname'],
                        interval=int(attrs['interval']),
                        kwargs=kwargs,
                        only_once=json.loads(attrs.get('only_once', 'false')),
                        max_runs=json.loads(attrs.get('max_runs', 'null')),
                        only_before=json.loads(attrs.get('only_before', 'null')))
                        
            self.root.controller.scheduler.add_task(task,
                                when=json.loads(attrs.get('when', 'null')),
                                offset=json.loads(attrs.get('offset', 'null')))
            
            return self.page("Created new task with master_id=%s" % \
                             task.master_id)
        except:
            return self.page("There was an error creating your task.")
    

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

    def start(self):
        """
        Set up the cherrypy server.
        """
        self.controller.logger.addHandler(WebHandler(self.log_data))
        self.controller.scheduler.add_callback(self.result_data.append)
        
        cherrypy.config["engine.autoreload_on"] = False
        cherrypy.config["server.socket_port"] = self.config.web_port
        
        cherrypy.tree.mount(DASAnalyticsWebManager(self.config, self),
                            self.config.web_base)
        
        cherrypy.engine.start()
        cherrypy.engine.block()