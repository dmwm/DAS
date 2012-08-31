#!/usr/bin/env python

"""
DAS dataset populator
"""

# system modules
import sys
import time
import thread

# DAS modules
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection
from DAS.core.das_core import DASCore
from DAS.core.das_query import DASQuery
from DAS.utils.task_manager import TaskManager

def datasets(inst='cms_dbs_prod_global'):
    "Provide list of datasets"
    dasconfig = das_readconfig()
    conn = db_connection(dasconfig['mongodb']['dburi'])
    coll = conn['dbs'][inst]
    for row in coll.find():
        yield row['dataset']

class Maintainer(object):
    "Maintainer keeps alive data records in DAS cache"
    def __init__(self, config):
        self.sleep = config.get('sleep', 5)
        nworkers = int(config.get('nworkers', 4))
        name = config.get('name', 'das_populator')
        dasconfig = das_readconfig()
        debug = False
        self.dascore = DASCore(config=dasconfig, nores=True, debug=debug)
        self.taskmgr = TaskManager(nworkers=nworkers, name=name)
        self.conn = db_connection(dasconfig['mongodb']['dburi'])

    def check_records(self):
        "Check and return list of DAS records which require update"
        for row in self.conn['das']['merge'].find():
            if  not row.has_key('qhash'):
                continue
            spec = {'qhash': row['qhash'], 'das.system':'das'}
            for rec in self.conn['das']['cache'].find(spec):
                if  rec.has_key('query'):
                    expire = rec['das']['expire']
                    if  expire < time.time() or \
                        abs(expire-time.time()) < self.sleep:
                        yield DASQuery(rec['query']), expire

    def update(self):
        """
        Update DAS cache:

            - get list of expired or near expire DAS records
            - store them into onhold set
            - loop over onhold set and invoke expired queries
            - sleep and repeat.
        """
        add_to_analytics = False
        onhold = {}
        while True:
            jobs = []
            for query, expire in self.check_records():
                if  not onhold.has_key(query):
                    onhold[query] = expire
            for query, expire in onhold.items():
                if  expire < time.time():
                    print "update %s at %s" % (query, time.time())
                    jobs.append(self.taskmgr.spawn(\
                            self.dascore.call, query, add_to_analytics))
                    del onhold[query]
            self.taskmgr.joinall(jobs)
            time.sleep(self.sleep)

class Populator(object):
    """
    This class populates DAS cache with data.
    The run method accepts list of DAS queries.
    """
    def __init__(self, config):
        nworkers = int(config.get('nworkers', 4))
        name = config.get('name', 'das_populator')
        dasconfig = das_readconfig()
        debug = False
        self.dascore = DASCore(config=dasconfig, nores=True, debug=debug)
        self.taskmgr = TaskManager(nworkers=nworkers, name=name)

    def run(self, queries):
        "Run taskmanger with given queries"
        jobs = []
        add_to_analytics = False
        for query in queries:
            jobs.append(self.taskmgr.spawn(\
                    self.dascore.call, DASQuery(query), add_to_analytics))
        self.taskmgr.joinall(jobs)

def main():
    "Populator daemon"
    mgr = Populator({'nworkers':100})
    inst = 'cms_dbs_prod_global'
    queries = []
    for dataset in datasets():
        if  dataset and dataset != '*':
            queries.append('dataset=%s instance=%s' % (dataset, inst))
    for q in queries[:5]:
        print q
    print "Fetch %s datasets from %s DBS collection" % (len(queries), inst)
    # Populator thread which feeds DAS cache with data
    time0 = time.time()
    print "start populator job", time0
#    thread.start_new_thread(mgr.run, (queries,))
    mgr.run(queries[:100])
    print "stop populator job, elapsed time", time.time()-time0
    sys.exit(0)
    print "start maintainer job", time.time()
    # Maintainer thread which update records in DAS cache
    maintainer = Maintainer({})
    maintainer.update()
#    thread.start_new_thread(maintainer.update, ())
#
# main
#
if __name__ == '__main__':
    main()
