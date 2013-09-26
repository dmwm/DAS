#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0703
"""
General utilities for DAS analytics.
"""

import multiprocessing
import threading
import logging
import time
import re
import os.path
import glob
import sys
from types import ClassType, TypeType
from DAS.utils.das_config import das_readconfig
from DAS.utils.utils import deepcopy
from DAS.core.das_analytics_db import DASAnalytics
from DAS.core.das_mongocache import decode_mongo_query

def get_mongo_query(query):
    "Get DAS query in MongoDB format and remove DAS look-up keys from it"
    mongoquery = decode_mongo_query(deepcopy(query))
    if  isinstance(mongoquery, dict) and 'spec' in mongoquery:
        for key in mongoquery['spec'].keys():
            if  key.find('das') != -1:
                # remove DAS keys, e.g. das.primary_key
                del mongoquery['spec'][key]
    return mongoquery

#adapted from http://stackoverflow.com/
#questions/641420/how-should-i-log-while-using-multiprocessing-in-python
class MultiprocessingLoggerClient(object):
    """
    _logger_ object that will be passed to the client.
    This fakes the interface of a logging object (mostly)
    but passes back messages (which must be pickleable)
    to the listener, which then injects them into the
    primary logger in the parent process.
    """
    def __init__(self, name, queue, **kwargs):
        self.name = name
        self.queue = queue
        self.extra = deepcopy(kwargs)

    def dispatch(self, lvl, msg, *args):
        """
        Create the record (just a dictionary, will
        be turned into a LogRecord server-side) and
        insert it into the queue.
        """
        attrs = {'lvl': lvl, 'args': tuple(args),
                 'msg': msg, 'name': self.name,
                 'extra': self.extra}
        try:
            self.queue.put_nowait(attrs)
        except:
            pass

    def info(self, msg, *args):
        "info logger"
        self.dispatch(logging.INFO, msg, *args)

    def debug(self, msg, *args):
        "debug logger"
        self.dispatch(logging.DEBUG, msg, *args)

    def warning(self, msg, *args):
        "warning logger"
        self.dispatch(logging.WARNING, msg, *args)

    def error(self, msg, *args):
        "error logger"
        self.dispatch(logging.ERROR, msg, *args)

    def critical(self, msg, *args):
        "critical logger"
        self.dispatch(logging.CRITICAL, msg, *args)

class MultiprocessingLoggerListener(object):
    """
    This is the second part of the multiprocessing logger
    chain. Messages are received by a thread here and
    injected into the named logger.
    """
    def __init__(self):
        self.queue = multiprocessing.Queue(-1)

        thread = threading.Thread(target=self.receive)
        thread.daemon = True
        thread.start()

    def receive(self):
        """
        Run by a dedicated thread, to receive log messages and inject
        them into the main logger.
        """
        while True:
            try:
                attrs = self.queue.get()
                logger = logging.getLogger(attrs['name'])
                logger.log(attrs['lvl'], attrs['msg'],
                           *attrs['args'], extra=attrs['extra'])
            except EOFError:
                break
            except:
                pass

    def getLogger(self, name, **kwargs):
        """
        Return a new client logger, with given name.
        """
        return MultiprocessingLoggerClient(name, self.queue, **kwargs)

MULTILOGGING_LISTENER = None
def multilogging():
    """
    This needs to be called first in the main process. Whichever
    calls it will be where messages are collected and injected.
    This has to be global because multiprocessing.Queue objects
    cannot be passed between processes, only inherited through
    shared memory.
    """
    global MULTILOGGING_LISTENER
    if not MULTILOGGING_LISTENER:
        MULTILOGGING_LISTENER = MultiprocessingLoggerListener()
    return MULTILOGGING_LISTENER


def parse_time(raw):
    """
    Consider time formats:
    number - interpret as GMT unix time
    +number - seconds from now
    +number[hH] - hours from now
    +number[mM] - minutes from now
    HH:MM[:SS] - absolute (localtime)
    HH:MM[:SS][utc gmt z] - absolute (gmt)
    """
    raw = raw.lower()
    match_unix = re.match(r'(\d+(?:\.\d*)?)', raw)
    match_offset = re.match(r'\+(\d+(?:\.\d*)?)([smhd]?)', raw)
    match_local = re.match(r'(\d{2}:\d{2}(?::\d{2})?)([utcgmz]*)', raw)

    if match_local:
        split = match_local.group(1).split(':')
        if len(split) == 3:
            hours, mins, secs = map(float, split)
        else:
            hours, mins, secs = map(float, split) + [0]
        if match_local.group(2):
            local = time.gmtime()
        else:
            local = time.localtime()
        target = time.time()
        target -= local.tm_hour * 3600
        target -= local.tm_min * 60
        target -= local.tm_sec
        target += hours * 3600
        target += mins * 60
        target += secs
        if target < time.time():
            target += 86400
        return target
    elif match_offset:
        value = float(match_offset.group(1))
        mult = match_offset.group(2)
        if mult == 'd':
            return time.time() + value*86400
        elif mult == 'h':
            return time.time() + value*3600
        elif mult == 'm':
            return time.time() + value*60
        else:
            return time.time() + value
    elif match_unix:
        return float(match_unix.group(1))
    else:
        return 0

def format_offset(offset):
    "Helper function to put sec in human readable format"
    if abs(offset) < 5:
        return "%d milliseconds" % int(offset * 1000)
    elif abs(offset) < 121:
        return "%d seconds" % int(offset)
    elif abs(offset) < 121*60:
        return "%d minutes" % int(offset / 60)
    elif abs(offset) < 49*3600:
        return "%d hours" % int(offset / 3600)
    else:
        return "%d days" % int(offset / 86400)

def get_analytics_interface():
    """
    Factory function to get a standalone interface to DASAnalytics without
    loading the rest of DAS, that logs to our global logger.
    """
    global DAS_CONFIG
    config = deepcopy(DAS_CONFIG)
    config['logger'] = logging.getLogger("DASAnalytics.AnalyticsDB")
    return DASAnalytics(config)


def nested_to_baobab(nested):
    """
    Convert a once-nested int dictionary into
    a baobab data structure for plotfairy
    """
    total = 0
    result = []
    for key, value in nested.items():
        res2 = []
        total2 = 0
        for kkk, vvv in value.items():
            res2.append({'label':kkk, 'value':vvv})
            total2 += vvv
        result.append({'label':key, 'value':total2, 'children':res2})
        total += total2
    return {'label': 'root', 'value': total, 'children': result}

class Report(object):
    """
    Base class for reports, to avoid the need to duplicate __init__
    Not mandatory, just for convience
    """
    def __init__(self, config, scheduler, results, logger):
        self.config = config
        self.scheduler = scheduler
        self.results = results
        self.logger = logger
    def __call__(self, **kwargs):
        raise NotImplementedError

def get_classes_by_path(path):
    """
    Import a package name, find the directory (if possible),
    import the contained python files and return a dictionary of
    class names to class objects. This is essentially similar to
    writing __all__ in __init__.py for the package, but the
    tasks/ and reports/ directories are intended to be added to
    in a fairly ad-hoc way, so it should be unnecessary to edit
    the __all__ each time one is added.
    """
    result = {}
    directory = None
    python_files = []
    try:
        # note we don't take the return value
        # which is DAS rather than DAS.analytics
        __import__(path)
        directory = os.path.dirname(sys.modules[path].__file__)
    except ImportError as err:
        print "Error trying to import package %s: %s" % (path, err[0])

    if directory:
        try:
            python_files = glob.glob(os.path.join(directory, "*.py"))
        except Exception as exp:
            print "Error trying to list package directory %s (%s): %s" \
                % (directory, path, exp[0])

    for pyfile in python_files:
        try:
            if os.path.isfile(pyfile):
                #get the python import name for this file
                pypath = path + '.' + \
                        os.path.splitext(os.path.basename(pyfile))[0]
                __import__(pypath)
                module = sys.modules[pypath]
                for name, obj in module.__dict__.items():
                    if not name.startswith('_'): #not hidden
                        if type(obj) in (TypeType, ClassType):
                            if not \
                            (hasattr(obj, "hide") and getattr(obj, "hide")):
                                if obj.__module__ == pypath:
                                    result[name] = obj


        except Exception as exp:
            msg = "Error trying to import file %s (%s): %s" \
                % (pypath, pyfile, exp[0])
            print msg
    return result

def get_globals():
    "Define globals dictionaries"
    task_classes = get_classes_by_path("DAS.analytics.tasks")
    report_classes = get_classes_by_path("DAS.analytics.reports")
    report_groups = {}
    for name, obj in report_classes.items():
        group = getattr(obj, "report_group") \
                    if hasattr(obj, "report_group") else "General"
        title = getattr(obj, "report_title") \
                    if hasattr(obj, "report_title") else name
        info = getattr(obj, "report_info") \
                    if hasattr(obj, "report_info") \
                    else (obj.__doc__ if obj.__doc__ else "")
        report_groups[group] = \
            report_groups.get(group, []) + [(name, title, info)]
    task_info = {}
    for name, obj in task_classes.items():
        title = getattr(obj, "task_title") \
                    if hasattr(obj, "task_title") else name
        info = getattr(obj, "task_info") \
                    if hasattr(obj, "task_info") \
                    else (obj.__doc__ if obj.__doc__ else "")
        options = getattr(obj, "task_options") \
                    if hasattr(obj, "task_options") else None
        task_info[name] = (title, info, options)
    return task_classes, report_classes, report_groups, task_info

TASK_CLASSES, REPORT_CLASSES, REPORT_GROUPS, TASK_INFO = get_globals()
DAS_CONFIG = das_readconfig()
