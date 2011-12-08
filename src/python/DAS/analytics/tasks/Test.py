"""
DAS Analytics task test class
"""

import random
import time
from DAS.utils.logger import PrintManager

class Test(object):
    """
    This is a test task that prints a message (supplied in kwargs) to stdout.
    It will randomly sometimes raise exceptions (5%), spawn subtasks (4%) or
    disable resubmission of itself (1%).
    """
    task_title = "Test task"
    task_options = [{"name":"message", "type":"string", 
                     "default":"hello world", "help":"Message to print"}]
    
    def __init__(self, **kwargs):
        self.logger = PrintManager('Test', kwargs.get('verbose', 0))
        self.name = kwargs['name']
        self.message = kwargs['message']
        self.index = kwargs['index']
    
    def __call__(self):
        self.logger.info('%s from index=%s' % (self.message, self.index))
        
        result = {}
        
        effect = random.random()
        if effect > 0.99:
            self.logger.info('..disabling resubmission')
            result['resubmit'] = False
        elif effect > 0.95:
            task = {'name':'spawn-of-%s' % self.index,
                    'classname':'Test',
                    'interval': random.randint(1,30),
                    'kwargs':{'message':'spawn-of-%s' % self.message}}
            effect2 = random.random()
            if effect2 > 0.50:
                task['only_once'] =  True
                self.logger.info('..spawning run-once task')
            elif effect2 > 0.25:
                task['max_runs'] = random.randint(1, 5)
                self.logger.info(\
                '..spawning task to run %s times' % task['max_runs'])
            else:
                task['only_before'] = time.time() + random.randint(1, 120)
                self.logger.info(\
                '..spawning task to run until %s' % task['only_before'])
            result['new_tasks'] = [task]
        elif effect > 0.90:
            self.logger.error('..raising an exception')
            raise
        
        return result
