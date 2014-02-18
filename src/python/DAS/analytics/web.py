#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0613,W0511,W0703
"""
DAS Analytics web server
"""
__author__ = "Gordon Ball"

import cherrypy
import time
import json
import logging

from DAS.analytics.config import DASAnalyticsConfig
from DAS.web.das_webmanager import DASWebManager
from DAS.analytics.task import Task
from DAS.analytics.utils import parse_time, TASK_INFO
from DAS.analytics.utils import REPORT_CLASSES, REPORT_GROUPS
import DAS


#This extensively borrows from DAS.web

def require(*required):
    "require wrapper"
    def wrap(func):
        "wrapper function"
        def inner(self, *args, **kwargs):
            "inner function"
            for arg in required:
                if not arg in kwargs:
                    return self.error("Required argument '%s' not given!" % arg)
            return func(self, *args, **kwargs)
        return inner
    return wrap

def json_requested():
    "Check JSON in cherrypy request headers"
    return 'accept' in cherrypy.request.headers and \
           'json' in cherrypy.request.headers['accept']

DASAnalyticsConfig.add_option("web_port", type=int, default=8213,
    help="Cherrypy serve port.")
DASAnalyticsConfig.add_option("web_base", type=basestring,
    default='/analytics',
    help="Base path for analytics web")
DASAnalyticsConfig.add_option("plotfairy_base", type=basestring,
    default='/plotfairy',
    help="Base path to a plotfairy instance to make graphs.")

class AnalyticsWeb(DASWebManager):
    """
    Use the DASWebManager class as the root of our interfaces,
    replacing the default header and footer.
    """
    def __init__(self, config, scheduler, results):
        self.config = config
        self._scheduler = scheduler
        self._results = results
        self._reports = {} # report instances, created on demand
        DASWebManager.__init__(self, config)
        self.base = config.web_base
        self.plotfairy = config.plotfairy_base

    def templatepage(self, tmpl, **kwargs):
        """
        Intercept template requests, and return a raw dictionary if json.
        """
        kwargs['base'] = self.base
        kwargs['plotfairy'] = self.plotfairy
        if json_requested():
            kwargs['template'] = tmpl
            return json.dumps(kwargs, default=str)
        else:
            return super(AnalyticsWeb, self).templatepage(tmpl, **kwargs)

    def page(self, content, _ctime=None, _response=False):
        """
        Intercept page requests, and return no headers if json.
        """
        if json_requested():
            return content
        else:
            return super(AnalyticsWeb, self).page(content)

    @cherrypy.expose
    def index(self, *args, **kwargs):
        """
        Show a main page with a list of other pages.
        TODO: A summary of number of defined tasks, running tasks,
        recently finished tasks and errors.
        """
        return self.page(self.templatepage("analytics_main"))

    def error(self, error, **kwargs):
        "Error page"
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
        return self.templatepage('analytics_header')

    @cherrypy.expose
    def doc(self):
        """Return templated (essentially static) documentation page"""
        return self.page(self.templatepage("analytics_doc"))

    def bottom(self, _div=""):
        """
        Provide footer for all web pages
        """
        return self.templatepage('analytics_bottom', version=DAS.version)

    @cherrypy.expose
    def schedule(self, *path, **attrs):
        """
        Show the currently scheduled and running tasks.
        TODO:

            - Move away from using master_id/uuid to friendly naming scheme.
            - Highlight jobs being retried.
            - Better reschedule interface.
        """
        task_schedule = self._scheduler.get_scheduled()
        task_schedule = sorted(task_schedule, key=lambda x: x['at'])
        next_attr = None
        now = time.time()
        if 'next' in attrs:
            next_attr = int(attrs['next'])
            before = now + next_attr
            task_schedule = filter(lambda x: x['at'] < before,
                                   task_schedule)

        task_running = self._scheduler.get_running()

        task_running = sorted(task_running, key=lambda x: x['started'])
        page = self.templatepage("analytics_schedule",
                                 schedule=task_schedule,
                                 running=task_running)
        return self.page(page)

    @cherrypy.expose
    def logger(self, *path, **attrs):
        """
        Show the current log.
        TODO: Auto link masterid/uuids in messages.
        TODO: Separate DAS and other message sources.
        TODO: Highlight by log level.
        TODO: Select by log level.
        """

        last = int(attrs.get('last', 0))
        level = int(attrs.get('level', 0))
        offset = int(attrs.get('offset', 0))
        limit = int(attrs.get('limit', 100))

        match = '.'.join(path)
        children = json.loads(attrs.get('children', 'true'))

        kwargs = {'log': match, 'children': children}
        if last:
            kwargs['after'] = time.time() - last
        if level:
            kwargs['lvl'] = level
        if offset:
            kwargs['skip'] = offset
        if limit:
            kwargs['limit'] = limit
        log_entries = self._results.get_logs(**kwargs)

        filters = [n.replace(match+'.', '') \
                for n in self._results.get_log_names(match+'.')]
        total = len(log_entries)

        page = self.templatepage('analytics_log',
                                 entries=log_entries,
                                 filters=filters,
                                 textpath='.'.join(path),
                                 path=self.base + '/logger/' + '/'.join(path),
                                 offset=offset,
                                 limit=limit,
                                 last=last,
                                 total=total,
                                 level=level)
        return self.page(page)

    @cherrypy.expose
    @require("id")
    def task(self, *path, **attrs):
        """
        Show detailed information for a single task.
        TODO: Refer by friendly names.
        """
        master_id = attrs['id']
        task = self._scheduler.get_task(master_id)

        if task:
            recent_results = self._results.get_results(master_id=master_id,
                                                       only='result',
                                                       limit=5)
            childen = self._scheduler.get_children(task)

            page = self.templatepage('analytics_task',
                                     id=master_id,#
                                     results=recent_results,
                                     children=childen,
                                     task=task)
        else:
            return self.error("Requested ID does not exist",
                              id=master_id)
        return self.page(page)


    @cherrypy.expose
    def control(self, *path, **attrs):
        """
        Show a page with some control options.
        TODO: Config subscription so changes take effect.
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
                                 config=self.config.get_dict(),
                                 tasks=TASK_INFO)
        return self.page(page)


    @cherrypy.expose
    def results(self, *path, **attrs):
        """
        Show a possibly-filtered list of task results.
        TODO: Highlight recent, retried or failed tasks.
        TODO: Select by time, parentage, status, class, etc
        """
        master = attrs.get('master', None)
        parent = attrs.get('parent', None)
        error = 'error' in attrs
        success = 'success' in attrs
        last = int(attrs.get('last', 0))
        offset = int(attrs.get('offset', 0))
        limit = int(attrs.get('limit', 100))
        classname = attrs.get('classname', None)
        name = attrs.get('name', None)

        query = {'only': 'result'}
        if master:
            query['master_id'] = master
        if parent:
            query['parent'] = parent
        if error:
            query['success'] = False
        if success:
            query['success'] = True
        if last:
            query['after'] = time.time() - last
        if offset:
            query['skip'] = offset
        if limit:
            query['limit'] = limit
        if classname:
            query['classname'] = classname
        if name:
            query['name'] = name

        results = self._results.get_results(**query)
        total = len(results)

        page = self.templatepage('analytics_results',
                                 results=results,
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
    @require("id", "index")
    def result(self, *path, **attrs):
        """
        Show in detail a single result.
        """
        master_id = attrs['id']
        index = int(attrs['index'])
        result = self._results.get_results(master_id=master_id,
                                           index=index,
                                           only='result', limit=1)
        if result:
            result = result[0]
        else:
            return self.error("Requested ID:index does not exist",
                              master_id=master_id,
                              index=index)

        logs = self._results.get_results(master_id=master_id,
                                         index=index,
                                         only='reslog')
        page = self.templatepage('analytics_result',
                                 logs=logs,
                                 result=result)
        return self.page(page)


    @cherrypy.expose
    @require("id")
    def remove_task(self, *path, **attrs):
        """
        Remove a task from the schedule. TODO: Auth.
        """
        master_id = attrs['id']
        result = self._scheduler.remove_task(master_id)
        if result:
            return self.schedule()
        else:
            return self.error(\
                "Unable to remove task. It probably didn't exist.",
                              id=master_id)

    @cherrypy.expose
    @require("id", "at")
    def reschedule_task(self, *path, **attrs):
        """
        Reschedule a task to a new time. TODO: Auth
        """
        master_id = attrs['id']
        when = parse_time(attrs['at'])
        if when > 0: #an integer or floating point
            when = float(when)
            if when > time.time():
                result = self._scheduler.reschedule_task(master_id, when)
                if result:
                    return self.schedule()
                else:
                    return self.error("Unable to reschedule task.",
                                      id=master_id,
                                      at=when)
            else:
                return self.error(\
                        "Attempting to reschedule a task into the past.",
                                  id=master_id,
                                  at=when)
        else:
            return self.error("Could not parse time format.",
                              id=master_id,
                              at=when)

    @cherrypy.expose
    @require("name", "classname", "interval")
    def add_task(self, *path, **attrs):
        """
        Add a new task, for an existing class.
        TODO: Auth
        TODO: accept a single dictionary containing all arguments
        """
        try:
            kwargs = json.loads(attrs.get('kwargs', "{}"))
            for key, val in kwargs.items():
                del kwargs[key]
                #kwargs must be string, not unicode
                kwargs[key.encode('ascii')] = val
            for key, val in attrs.items():
                if key.startswith('kwarg_'):
                    #if loading the string evaluates to JSON != string,
                    #use that type otherwise, treat it as a string
                    #(avoiding the need to explicitly quote strings)
                    try:
                        if not isinstance(json.loads(val), basestring):
                            kwargs[key[6:].encode('ascii')] = json.loads(val)
                        else:
                            kwargs[key[6:].encode('ascii')] = val
                    except:
                        kwargs[key[6:].encode('ascii')] = val
            task = Task(name=attrs['name'],
                    classname=attrs['classname'],
                    interval=float(attrs['interval']),
                    kwargs=kwargs,
                    max_runs=json.loads(attrs.get('max_runs', 'null')),
                    only_before=json.loads(attrs.get('only_before', 'null')))
        except Exception as exp:
            return self.error(\
                "There was an error decoding the arguments.", details=exp[0])
        try:
            self._scheduler.add_task(task,
                     when=json.loads(attrs.get('when', 'null')),
                     offset=json.loads(attrs.get('offset', 'null')))

            return self.task(id=task.master_id)
        except Exception as exp:
            return self.error(\
                "There was an error adding the task.", details=exp[0])

    @cherrypy.expose
    def reports(self, *path, **attrs):
        """
        Provide the list of available reports.
        """
        print REPORT_GROUPS
        page = self.templatepage("analytics_reports",
                                 groups=REPORT_GROUPS)
        return self.page(page)

    @cherrypy.expose
    @require("report")
    def report(self, *path, **attrs):
        """
        Access an individual report, handing off rendering to
        the implementation class.

        It is intended that this is entirely stateless, but I suppose
        some state/ajax could be hacked in if really necessary.
        """
        report = attrs['report']
        if report in REPORT_CLASSES:
            #instantiate if not yet existing
            if not report in self._reports:
                try:
                    self._reports[report] = \
                        REPORT_CLASSES[report](config=self.config,
                           scheduler=self._scheduler,
                           results=self._results,
                           logger=logging.getLogger(\
                                "DASAnalytics.Report.%s" % report))
                except Exception as exp:
                    return self.error(\
                        "There was an error instantiating report %s: %s"\
                        % (report, exp[0]))
            try:
                template, kwargs = self._reports[report](**attrs)
                return self.page(self.templatepage(template, **kwargs))
            except Exception as exp:
                return self.error(\
                    "There was an error generating report %s: %s"\
                        % (report, exp), args=attrs)
        else:
            return self.error(
                "Requested report (%s) does not exist." % (report))
