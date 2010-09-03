"Query submission classes for queryspammer"

import random
import multiprocessing
import urllib
import urllib2
import time

try:
    from DAS.core.das_core import DASCore
    HAVE_DAS = True
except ImportError:
    HAVE_DAS = False

LOG = multiprocessing.get_logger()

class Submitter(multiprocessing.Process):
    "Baseclass for query submission classes"
    def __init__(self, producer, **kwargs):
        self.producer = producer
        self.mode = kwargs.get('mode', 'continuous')
        self.delay = kwargs.get('delay', 1)
        self.max_calls = kwargs.get('max_calls', -1)
        self.max_time = kwargs.get('max_time', -1)
        multiprocessing.Process.__init__(self)
    def run(self):
        calls = 0
        start_time = time.time()
        while True:
                          
            query = self.producer()
            query_start = time.time()
            try:
                result = self.submit(query)
            except Exception, e:
                result = str(e)            
            query_end = time.time()

            LOG.info('call=%d query="%s" latency=%.3f result="%s"' % (calls,
                                                                      query,
                                                                      query_end-query_start,
                                                                      result))

            calls += 1
            
            if not self.max_calls == -1 and calls > self.max_calls:
                LOG.info('Reached maximum calls (%s), aborting.' % self.max_calls)
                return
            if not self.max_time == -1 and query_end - start_time > self.max_time:
                LOG.info('Reached maximum time (%s), aborting.' % self.max_time)
                return

            if self.mode == 'continuous':
                pass
            elif self.mode == 'delay':
                time.sleep(self.delay)
            elif self.mode == 'random':
                time.sleep(random.random() * self.delay)
    def submit(self, query):
        return None
            
class StdOutSubmitter(Submitter):
    "Submitter that prints the query to stdout"
    def submit(self, query):
        print query
        return True
            
class FileSubmitter(Submitter):
    "Submitter that writes the query to a file"
    def __init__(self, producer, **kwargs):
        self.filename = kwargs.get('filename', 'query.out')
        self.file = open(self.filename, 'w')
        LOG.info('Writing to %s' % self.filename)
        Submitter.__init__(self, producer, **kwargs)
    def submit(self, query):
        self.file.write(query+'\n')
        return True

class HTTPSubmitter(Submitter):
    "Submitter that makes a GET or POST request"
    def __init__(self, producer, **kwargs):
        self.baseurl = kwargs.get('baseurl', 'http://localhost/')
        self.args = kwargs.get('args', {})
        self.queryarg = kwargs.get('queryarg', 'query')
        self.method = kwargs.get('method', 'GET')
        self.getmode = kwargs.get('getmode', 'query')
        self.headers = kwargs.get('headers', {})
        self.timeout = kwargs.get('timeout', None)
        Submitter.__init__(self, producer, **kwargs)
    def submit(self, query):
        if self.mode == 'GET':
            if self.getmode == 'query':
                self.args[self.queryarg] = query
                url = self.baseurl +\
                 '' if self.baseurl[-1] == '?' else '?' +\
                  urllib.urlencode(self.args)
                req = urllib2.Request(url=url, headers=self.headers)
                result = urllib2.urlopen(req, timeout=self.timeout)
                return len(result.read())
            elif self.getmode == 'positional':
                url = self.baseurl + urllib.quote_plus(query)
                req = urllib2.Request(url=url, headers=self.headers)
                result = urllib2.urlopen(req, timeout=self.timeout)
                return 'READ %d' % len(result.read())
        elif self.mode == 'POST':
            self.args[self.queryarg] = query
            req = urllib2.Request(url=url, 
                                  data=urllib.urlencode(self.args), 
                                  headers=self.headers)
            result = urllib2.urlopen(req, timeout=self.timeout)
            return 'READ %d' % len(result.read())
        return None

class DASSubmitter(Submitter):
    "Submitter using DASCore"
    def __init__(self, producer, **kwargs):
        assert HAVE_DAS
        self.idx = kwargs.get('idx', 0)
        self.limit = kwargs.get('limit', 0)
        self.skey = kwargs.get('skey', None)
        self.sorder = kwargs.get('sorder', 'asc')
        Submitter.__init__(self, producer, **kwargs)
        self.DAS = DASCore()
    def submit(self, query):
        result = self.DAS.result(query, 
                                 self.idx, 
                                 self.limit, 
                                 self.skey, 
                                 self.sorder)
        return 'READ %d' % len(result)

def list_submitters():
    "List all available submitter classes"
    return [k 
            for k,v in globals().items() 
            if type(v)==type(type) 
            and issubclass(v,Submitter) 
            and not v==Submitter]
