import multiprocessing
import threading
import logging
import time
import collections

#adapted from http://stackoverflow.com/questions/641420/how-should-i-log-while-using-multiprocessing-in-python
class MultiprocessingLoggerClient:
    """
    _logger_ object that will be passed to the client.
    This fakes the interface of a logging object (mostly)
    but passes back messages (which must be pickleable)
    to the listener, which then injects them into the
    primary logger in the parent process.
    """
    def __init__(self, name, queue):
        self.name = name
        self.queue = queue
        
    def dispatch(self, lvl, msg, *args):
        """
        Create the record (just a dictionary, will
        be turned into a LogRecord server-side) and
        insert it into the queue.
        """
        attrs = {'lvl': lvl, 'args': tuple(args),
                 'name': self.name, 'msg': msg}
        try:
            self.queue.put_nowait(attrs)
        except:
            pass
        
    def info(self, msg, *args):
        self.dispatch(logging.INFO, msg, *args)
    
    def debug(self, msg, *args):
        self.dispatch(logging.DEBUG, msg, *args)
        
    def warning(self, msg, *args):
        self.dispatch(logging.WARNING, msg, *args)
        
    def error(self, msg, *args):
        self.dispatch(logging.ERROR, msg, *args)
        
    def critical(self, msg, *args):
        self.dispatch(logging.CRITICAL, msg, *args)

class MultiprocessingLoggerListener:
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
                logger.log(attrs['lvl'], attrs['msg'], *attrs['args'])
            except EOFError:
                break
            except:
                pass
    
    def getLogger(self, name):
        """
        Return a new client logger, with given name.
        """
        return MultiprocessingLoggerClient(name, self.queue)
            
MULTILOGGING_LISTENER=None
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

class WebHandler(logging.Handler):
    "LogHandler to redirect records to the web deque."
    def __init__(self, store):
        self.store = store
        logging.Handler.__init__(self, 1)
    def emit(self, record):
        self.store.append(record)
        
class RunningJobHandler(logging.Handler):
    """
    This listens to log messages, looking for references to
    the currently running task_ids. These are then stored in
    a cache until the job finishes, at which point they are
    popped off the cache and stored with the job result.
    
    DEPRECATED. For reasons unclear to me, adding this to
    the logging chain results in all the logs getting eaten.
    """
    def __init__(self):
        self.watchlist = set()
        self.cache = collections.defaultdict(list)
        logging.Handler.__init__(self, 1)
    
    def add_watch(self, taskid):
        "Add a taskid to watch for"
        self.watchlist.add(taskid)
    
    def emit(self, record):
        "Handle a log record"
        for taskid in self.watchlist:
            if taskid in record.name:
                self.cache[taskid] += [{'msg': record.msg,
                                        'args':record.args,
                                        'lvl': record.lvl,
                                        'name':record.name,
                                        'time':time.time()}]
    def get(self, taskid):
        "Get the logs for a given taskid and remove it from the watchlist"
        self.watchlist.remove(taskid)
        return self.cache.pop(taskid, [])
        