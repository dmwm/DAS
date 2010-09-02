import random
import time

class Test(object):
    """
    This is a test task that prints a message (supplied in kwargs) to stdout.
    It will randomly sometimes raise exceptions (5%), spawn subtasks (4%) or
    disable resubmission of itself (1%).
    """
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.name = kwargs['name']
        self.taskid = kwargs['taskid']
        self.message = kwargs['message']
    
    def __call__(self):
        print '%s from taskid=%s'%(self.message,self.taskid)
        
        result = {}
        
        effect = random.random()
        if effect > 0.99:
            print '..disabling resubmission'
            result['resubmit']=False
        elif effect > 0.95:
            task = {'name':'spawn-of-%s'%self.taskid,
                    'classname':'test',
                    'interval': random.randint(1,30),
                    'kwargs':{'message':'spawn-of-%s'%self.message}}
            effect2 = random.random()
            if effect2 > 0.50:
                task['only_once'] =  True
                print '..spawning run-once task'
            elif effect2 > 0.25:
                task['max_runs'] = random.randint(1,5)
                print '..spawning task to run %s times' % task['max_runs']
            else:
                task['only_before'] = time.time() + random.randint(1,120)
                print '..spawning task to run until %s' % task['only_before']
            result['new_tasks'] = [task]
        elif effect > 0.90:
            print '..raising an exception'
            raise
        
        return result