"""
DAS analytics results module
"""

# system modules
import re
import thread
import logging

# DAS modules
from pymongo import DESCENDING
from pymongo.errors import InvalidName
from bson.errors import InvalidDocument, InvalidStringData
from DAS.utils.das_db import db_connection, is_db_alive, db_monitor
from DAS.analytics.config import DASAnalyticsConfig
from DAS.utils.thread import start_new_thread

DASAnalyticsConfig.add_option("db_uri", type=basestring,
      default="mongodb://localhost:27017/",
      help="MongoDB URI")
DASAnalyticsConfig.add_option("db_name", type=basestring,
      default="analytics_results",
      help="Name of MongoDB database")
DASAnalyticsConfig.add_option("db_coll", type=basestring,
      default="db", help="Name of MongoDB collection")
DASAnalyticsConfig.add_option("db_size", type=int,
      default=64*1024*1024,
      help="Maximum size of analytics internal DB")

class ResultManager(logging.Handler):
    """
    Class that receives and provides access to task results and
    logging information, and sorts these for web access.
    
    These are stored in a mongodb capped collection. 
    """
    
    def __init__(self, config):
        logging.Handler.__init__(self, logging.NOTSET)
        #flag to terminate the clean-up thread
        self.logger = logging.getLogger("DASAnalytics.ResultManager")
        self.dburi  = config.db_uri
        self.dbname = config.db_name        
        self.dbcoll = config.db_coll
        self.dbsize = config.db_size
        
        self.col = None #collection

        # Monitoring thread which performs MongoDB connection
        self.init()
        thname = 'analytics_results'
        start_new_thread(thname, db_monitor, (self.dburi, self.init, 5))

    def init(self):
        "Initialize connection to MongoDB"
        conn = db_connection(self.dburi)
        if  conn:
            database = conn[self.dbname]
            if  self.dbcoll not in database.collection_names():
                database.create_collection(self.dbcoll, \
                            capped=True, size=self.dbsize)
            self.col = database[self.dbcoll]                
        if  not is_db_alive(self.dburi):
            self.col = None
            return
        
    def emit(self, record):
        "Logging Handler method to receive messages"
        if not hasattr(record, 'message'):
            record.message = record.msg % record.args
        if hasattr(record, 'task_master'):
            self.receive_task_log(record)
        else:
            self.receive_log(record)
     
    def receive_task_result(self, result):
        """
        Receive a result dictionary from a finished task, 
        and store it in the DB.
        """
        result['type'] = 'result'
        try:
            if  self.col:
                self.col.insert(result, check_keys=True)
        except (InvalidName, InvalidDocument, InvalidStringData):
            print "\n### Fail to insert", result 
            #we tried to insert a dict with dotted names (or similar),
            #insert a minimal dict and an explanatory note
            result = {'master_id': result['master_id'],
                      'name': result['name'],
                      'classname': result['classname'],
                      'success': result['success'],
                      'start_time': result['start_time'],
                      'finish_time': result['finish_time'],
                      'index': result['index'],
                      'parent': result['parent'],
                      'type': 'result',
                      'mongo_error': 
                        'Result contained invalid Mongo names, dropped.'}
            if  self.col:
                self.col.insert(result)

    def receive_task_log(self, record):
        """Receive a task-associated log message, and store it."""
        result = {'msg': record.message,
                  'lvl': record.levelno,
                  'log': record.name,
                  'type': 'reslog',
                  'time': record.created,
                  'name': record.task_name,
                  'classname': record.task_class,
                  'index': record.task_index,
                  'master_id': record.task_master,
                  'parent': record.task_parent}
        if  self.col:
            self.col.insert(result)

    def receive_log(self, record):
        """Receive a general log message, and store it."""
        result = {'msg': record.message,
                  'lvl': record.levelno,
                  'log': record.name,
                  'type': 'log',
                  'time': record.created}
        if  self.col:
            self.col.insert(result)

    def get_logs(self, log=None, lvl=None, before=None, after=None, 
                 children=False, limit=0, skip=0):
        "Get some general logs from the DB"
        query = {'type': 'log'}
        if log:
            if children:
                try:
                    query['log'] = re.compile("^" + log.replace('.', r'\.'))
                except:
                    pass
            else:
                query['log'] = log
        if lvl:
            query['lvl'] = {'$gte': lvl}
        if before and after:
            query['time'] = {'$gte': after, '$lte': before}
        elif before:
            query['time'] = {'$lte': before}
        elif after:
            query['time'] = {'gte': after}
        if  self.col:
            result = list(self.col.find(query, limit=limit, skip=skip,
                                      sort=[('time', DESCENDING)]))
        else:
            result = []
        return result
        
    def get_log_names(self, match=None):
        "Get the set of logger names known"
        query = {'type':'log'}
        if match:
            query['log'] = re.compile('^'+match.replace('.', r'\.'))
        if  self.col:
            result = self.col.find(query, ['log'])
        return list(set(item['log'] for item in result))
    
    def get_results(self, master_id=None, index=None, name=None, success=None,
                    classname=None, before=None, after=None, parent=None,
                    limit=0, skip=0, only=None):
        "Get some results and/or associated log entries"
        query = {}
        if index:
            query['index'] = index
        if master_id:
            query['master_id'] = master_id
        if name:
            query['name'] = name
        if not success == None:
            query['success'] = success
        if classname:
            query['classname'] = classname
        if before:
            query['start_time'] = {'$lte': before}
        if after:
            query['finish_time'] = {'$gte': after}
        if parent:
            query['parent'] = parent
        if only:
            query['type'] = only
        if  self.col:
            result = list(self.col.find(query, limit=limit, skip=skip,
                                      sort=[('finish_time', DESCENDING)]))
        else:
            result = []
        return result
