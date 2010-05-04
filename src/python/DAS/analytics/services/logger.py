import DAS.analytics.analytics_service
import sys
import time

class Logger(DAS.analytics.analytics_service.DASServiceBase):
    defaults = {
                'interval':1,
                'output':'/tmp/das_analytics_log',
                'subscriptions':[('all','*','log')],
                'flush_frequency':5
                }
    levels = {
              0: 'INFO',
              1: 'ERROR',
              2: 'CRITICAL'
              }
    def configure(self,config):
        self.output = open(config['output'],'w')
        self.flush_frequency = config['flush_frequency']
        self.flush_count = 0
        
        #self.output = sys.stdout
    
    def pipe_log(self,msg):
        print 'pipe_log'
        src = msg.get('src','')
        level = msg.get('level',0)
        t = msg.get('time',0)
        text = msg.get('text','')
        self.output.write('[%s] %s: %s: %s\n'%(time.strftime('%d-%m-%Y %H:%M:%S',time.gmtime(t)),src,self.levels.get(level,'OTHER'),text))
        self.flush_count += 1
        if self.flush_count%self.flush_frequency:
            self.output.flush()
        
    def finalise(self):
        self.output.close()
        
    
        
    