import multiprocessing
import logging
import cherrypy
import collections

from DAS.analytics.analytics_config import DASAnalyticsConfig
from DAS.analytics.analytics_utils import elem

DASAnalyticsConfig.add_option("port",
                              type=int,
                              default=1080,
      help="Cherrypy listen port.")
DASAnalyticsConfig.add_option("web_history",
                              type=int,
                              default=1000,
      help="How many log and task results to keep in the webserver.")
class DASAnalyticsWeb(multiprocessing.Process):
    """
    DAS Analytics web class to be served under CherryPy server.
    """
    def __init__(self, config, pipe):
        self.config = config
        self.pipe = pipe
        self.logger = logging.getLogger('DASAnalytics.Web')
        multiprocessing.Process.__init__(self, name="DASAnalyticsWeb")
        self.daemon = True
        self.log_data = collections.deque(maxlen=config.web_history)
        self.result_data = collections.deque(maxlen=config.web_history)
        self.task_data = []
        self.info_data = {}

    def run(self):
        """
        accumulate data in this loop by pipe from the controller
        then allow cherrypy to serve it
        """
        cherrypy.server.socket_port = self.config.port
        cherrypy.tree.mount(self)
        cherrypy.server.quickstart()
        cherrypy.engine.start()

        while True:
            if self.pipe.poll():
                data = self.pipe.recv()
                if isinstance(data, tuple) and len(data)==2:
                    datatype = data[0]
                    content = data[1]
                    if datatype == 'log':
                        self.log_data.appendleft(content)
                    elif datatype == 'result':
                        self.result_data.appendleft(content)
                    elif datatype == 'tasks':
                        self.task_data = content
                    elif datatype == 'info':
                        self.info_data = content

    def dispatch(self, msgtype, payload=None):
        """Dispatch method"""
        self.pipe.send((msgtype, payload))

    @cherrypy.expose    
    def log(self, *path, **attrs):
        """Log method"""
        filters = []
        if len(path) == 1:
            filters.append(lambda x: path[0] in x.name)
        records = None
        maximum = attrs.get('limit', None)
        maximum = int(maximum) if maximum else None
        if filters:
            records = [record.getMessage() 
                       for record in self.log_data 
                       if all([f(record) for f in filters])]
        else:
            records = [record.getMessage()
                       for record in self.log_data]
        if maximum:
            records = records[:maximum]
        return elem('html', elem('body', elem(\
                'ul','\n'.join([elem('li', elem('pre', r)) for r in records]))))
            
    @cherrypy.expose
    def tasks(self, *path, **attrs):
        """Show tasks"""
        return elem('html', elem('body', elem(\
        'ul','\n'.join([elem('li', elem('pre', str(t))) for t in self.task_data]))))
    
    @cherrypy.expose
    def results(self, *path, **attrs):
        """Show results"""
        return elem('html', elem('body', elem(\
        'ul','\n'.join([elem('li', elem('pre', str(r))) for r in self.result_data]))))
    
    @cherrypy.expose
    def info(self, *path, **attrs):
        """Show info"""
        return elem('html', elem('body', elem('pre', str(self.info_data))))
    
    @cherrypy.expose
    def cmd(self, *path, **attrs):
        """Perform command by asking dispatch method"""
        if len(path) >= 1:
            cmd = path[0]
            self.dispatch(cmd, {'args':path[1:], 'kwargs':attrs})
            
