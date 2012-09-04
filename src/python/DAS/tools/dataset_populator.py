#!/usr/bin/env python
#pylint: disable-msg=R0903
"""
DAS dataset populator. It splits into two parts:
    - Populator class which feeds DAS with dataset info
    - Maintainer class which keeps dataset info in DAS cache up-to-date
      ----------------- IMPORTANT -----------------
      - to keep queries in DAS cache it needs to be initialized
        with query_pattern. For example, to keep dataset queries we
        provide {'das.system':'dbs', 'das.primary_key':'dataset.name'}
        pattern. It implies that we need to maintain pattern indexes
        in order to efficiently look-up patterns in MongoDB cache.
"""

# system modules
import sys
import time
import thread
from optparse import OptionParser

# DAS modules
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection
from DAS.core.das_core import DASCore
from DAS.core.das_query import DASQuery
from DAS.utils.task_manager import TaskManager

if sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

class DASOptionParser:
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store",
            type="int", default=0, dest="verbose", help="verbose output")
        self.parser.add_option("--inst", action="store", type="string",
            default="cms_dbs_prod_global", dest="instance",
            help="specify DBS instance, default cms_dbs_prod_global")
        self.parser.add_option("--mworkers", action="store", type="int",
            default=10, dest="mworkers",
            help="specify number of maintainer workers to run")
        self.parser.add_option("--pworkers", action="store", type="int",
            default=10, dest="pworkers",
            help="specify number of populator workers to run")
        self.parser.add_option("--sleep", action="store", type="int",
            default=5, dest="interval",
            help="specify sleep interval for maintainer worker")

    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

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
        self.sleep   = config.get('sleep', 5)
        pattern      = {'das.system':'dbs', 'das.primary_key': 'dataset.name'}
        self.pattern = config.get('query_pattern', pattern)
        nworkers     = int(config.get('nworkers', 10))
        name         = config.get('name', 'dataset_keeper')
        dasconfig    = das_readconfig()
        debug        = False
        self.dascore = DASCore(config=dasconfig, nores=True, debug=debug)
        self.taskmgr = TaskManager(nworkers=nworkers, name=name)
        self.conn    = db_connection(dasconfig['mongodb']['dburi'])

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
        nworkers     = int(config.get('nworkers', 10))
        name         = config.get('name', 'dataset_populator')
        dasconfig    = das_readconfig()
        debug        = False
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

def daemon(config):
    "Populator daemon"
    instance = config.get('instance')
    interval = config.get('interval')
    mworkers = config.get('mworkers')
    pworkers = config.get('pworkers')
    # Run maintainer thread which updates records in DAS cache
#    print "start maintainer job", time.time()
#    config = {'sleep': interval, 'nworkers':mworkers}
#    maintainer = Maintainer(config)
#    maintainer.update()
#    thread.start_new_thread(maintainer.update, ())

    mgr = Populator({'nworkers':pworkers})
    queries = []
    for dataset in datasets():
        if  dataset and dataset != '*':
            queries.append('dataset=%s instance=%s' % (dataset, instance))
    print "Fetch %s datasets from %s DBS collection" % (len(queries), instance)
    # Populator thread which feeds DAS cache with data
    time0 = time.time()
    print "start populator job", time0
#    thread.start_new_thread(mgr.run, (queries,))
    mgr.run(queries[:100])
    print "stop populator job, elapsed time", time.time()-time0
#
# main
#
def main():
    "Main function"
    mgr = DASOptionParser()
    opts, _ = mgr.get_opt()
    config  = dict(instance=opts.instance,
            interval=opts.interval,
            mworkers=opts.mworkers,
            pworkers=opts.pworkers,)
    daemon(config)

if __name__ == '__main__':
    main()
