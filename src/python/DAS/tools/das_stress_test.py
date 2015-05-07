#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=R0903,R0912,R0913,R0914,R0915,W0702
"""
File       : stress_test.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: DAS stress test tool
We retrieve list of all DBS datasets and run
N-processes which retrieve some information
about those datasets, e.g. file dataset=X
"""
from __future__ import print_function

# system modules
import os
import sys
import json
import time
import random
try:
    import psutil
    PSUTIL = True
except:
    PSUTIL = False
from   optparse import OptionParser
from   multiprocessing import Process, Queue

# DAS modules
from DAS.tools.das_client import get_data
from DAS.core.das_query import DASQuery

DBS_GLOBAL='prod/global'

class TestOptionParser(object):
    "Test option parser"
    def __init__(self):
        cmd = 'das_stress_test'
        usage  = 'Usage: %s [OPTIONS]' % cmd
        usage += '\nTest and monitor DAS performance. It uses the seed query'
        usage += '\nto obtain list of results and select keys which are used'
        usage += '\nrandomly to construct DAS queries. Seed query should be'
        usage += '\nused a dataset pattern, e.g. dataset=/A*/*/*. The results'
        usage += '\nvalues will be used in tests with random select keys'
        usage += '\n\nExamples:\n'
        usage += '\n    # run 10 tests and monitor system usage'
        usage += '\n    %s --ntests=10 --monitor' % cmd
        usage += '\n    # run 10 tests using different seed query'
        usage += '\n    %s --ntests=10 --seed="dataset=/ZMM*/*/*"' % cmd
        usage += '\n    # run 10 tests using given select keys at random'
        usage += '\n    %s --ntests=10 --keys="file,block"' % cmd
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("-v", "--verbose", action="store",
            type="int", default=0, dest="debug",
            help="verbose output")
        self.parser.add_option("--host", action="store", type="string",
            default="http://localhost:8212", dest="host",
            help="specify DAS URL to test, e.g. https://dastest.cern.ch")
        self.parser.add_option("--ntests", action="store", type="int",
            default=0, dest="ntests",
            help="specify max number of clients")
        self.parser.add_option("--limit", action="store", type="int",
            default=0, dest="limit",
            help="specify number of rows to retrieve within a test, default all")
        self.parser.add_option("--file", action="store", type="string",
            default=None, dest="qfile",
            help="read queries from given file")
        self.parser.add_option("--seed", action="store", type="string",
            default="dataset=/A*/*/*", dest="query",
            help="seed query, e.g. dataset=/A*/*/*")
        msg = "list of look-up keys to be used in tests for seed query values"
        self.parser.add_option("--keys", action="store", type="string",
            default="dataset,block,file,summary,file_lumi,file_run_lumi",
            dest="lkeys", help=msg)
        msg  = 'specify X509 key file name'
        ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
        self.parser.add_option("--key", action="store", type="string",
            default=ckey, dest="ckey", help=msg)
        cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
        msg  = 'specify X509 certificate file name'
        self.parser.add_option("--cert", action="store", type="string",
            default=cert, dest="cert", help=msg)
        msg  = 'turn on monitoring, depends on psutil module'
        self.parser.add_option("--monitor", action="store_true",
            default=False, dest="mon", help=msg)
        msg  = 'monitor system or pid, pid=0 provides mem/cpu monitoring'
        msg += ', pid=-1 provides network monitoring, pid=PID will monitor'
        msg += ' given PID'
        self.parser.add_option("--monitor-pid", action="store", type="int",
            default=0, dest="mpid", help=msg)
        msg  = 'output file name for monitor information, default monitor.csv'
        self.parser.add_option("--monitor-out", action="store", type="string",
            default="monitor.csv", dest="mout", help=msg)
    def get_opt(self):
        """Returns parse list of options"""
        return self.parser.parse_args()

def timestamp(time0=None):
    "Standard timestamp"
    ttime = time.localtime(time0) if time0 else time.localtime()
    tstamp = time.strftime('%d/%b/%Y:%H:%M:%S', ttime)
    return tstamp

def etime(time0):
    "Return formatted output for elapsed time"
    return '%0.2f' % (time.time()-time0)

def run(out, host, query, idx, limit, debug, thr, ckey, cert):
    """
    Worker function which performs query look-up in DAS and print
    results to stdout. It should be spawned as separate process
    to test DAS server.
    """
    time0  = time.time()
    data   = get_data(host, query, idx, limit, debug, thr, ckey, cert)
    if  isinstance(data, dict):
        jsondict = data
    else:
        jsondict = json.loads(data)
    status = jsondict.get('status', None)
    reason = jsondict.get('reason', None)
    nres   = jsondict.get('nresults', None)
    tstm   = jsondict.get('timestamp', 0)
    data   = jsondict.get('data')
    if  data and isinstance(data, list) and len(data):
        qhash  = data[0].get('qhash')
    else:
        qhash = DASQuery(query + ' instance=%s' % DBS_GLOBAL).qhash
    msg    = 'status: %s client: %s server: %s nresults: %s query: %s qhash: %s' \
            % (status, etime(time0), etime(tstm), nres, query, qhash)
    if  reason:
        msg += ' reason: %s' % reason
    out.put((nres, status, qhash))
    print(msg)
    if  debug:
        if  nres > 0:
            if  len(data):
                print(data[0])
            else:
                print("### NO DATA:", jsondict)

def main():
    "Main function"
    mgr      = TestOptionParser()
    opts, _  = mgr.get_opt()

    ntests   = opts.ntests
    if  not ntests:
        print("Please specify number of tests to run, see options via --help")
        sys.exit(0)
    host     = opts.host
    ckey     = opts.ckey
    cert     = opts.cert
    thr      = 600
    debug    = opts.debug
    query    = opts.query
    qfile    = opts.qfile
    limit    = opts.limit
    lkeys    = [k.strip().replace('_', ',') for k in opts.lkeys.split(',')]
    uinput   = query.replace('dataset=', '')
    if  query.find('dataset=') == -1:
        print('Improper query="%s", please provide dataset query' \
                % query)
        sys.exit(1)
    # check/start monitoring
    monitor_proc = None
    if  opts.mon:
        if  PSUTIL:
            monitor_proc  = Process(target=monitor, args=(opts.mpid, opts.mout))
            monitor_proc.daemon = True
            monitor_proc.start()
        else:
            print("Unable to load psutil package, turn off monitoring")
    # setup initial parameters
    time0    = time.time()
    if  qfile: # read queries from query file
        status = 'qfile'
    else:
        idx      = 0
        limit    = 0 # fetch all datasets
        data     = get_data(host, query, idx, limit, debug, thr, ckey, cert)
        if  isinstance(data, dict):
            jsondict = data
        else:
            jsondict = json.loads(data)
        status   = jsondict.get('status', None)
    pool     = {}
    out      = Queue()
    if  status == 'ok':
        nres = jsondict.get('nresults', None)
        sec = "(all reported times are in seconds)"
        print("Seed query results: status %s, nrecords %s, time %s %s" \
                % (status, nres, etime(time0), sec))
        if  nres:
            idx   = 0
            limit = opts.limit # control how many records to get
            datasets = [r['dataset'][0]['name'] for r in jsondict['data'] \
                    if  r['dataset'][0]['name'] != uinput]
            if  ntests > len(datasets):
                msg  = 'Number of tests exceed number of found datasets. '
                msg += 'Please use another value for ntests'
                msg += 'ndatasets=%s, ntests=%s' % (len(datasets), ntests)
                print('\nERROR:', msg)
                sys.exit(1)
            datasets.sort()
            if  debug:
                print("Found %s datasets" % len(datasets))
                print("First %s datasets:" % ntests)
                for dataset in datasets[:ntests]:
                    print(dataset)
            for dataset in datasets[:ntests]:
                jdx   = random.randint(0, len(lkeys)-1)
                skey  = lkeys[jdx] # get random select key
                query = '%s dataset=%s' % (skey, dataset)
                idx   = 0 # always start from first record
                args  = (out, host, query, idx, limit, debug, thr, ckey, cert)
                proc  = Process(target=run, args=args)
                proc.start()
                pool[proc.name] = proc
    elif status == 'qfile':
        flist = [f.replace('\n', '') for f in open(qfile, 'r').readlines()]
        for query in random.sample(flist, ntests):
            idx   = 0 # always start from first record
            args  = (out, host, query, idx, limit, debug, thr, ckey, cert)
            proc  = Process(target=run, args=args)
            proc.start()
            pool[proc.name] = proc
    else:
        print('DAS cli fails status=%s, query=%s' % (status, query))
        print(jsondict)
    # wait for all processes to finish their tasks
    while 1:
        for pname in pool.keys():
            if  not pool[pname].is_alive():
                del pool[pname]
        if  len(pool.keys()) == 0:
            break
        time.sleep(1)
    if  opts.mon and PSUTIL:
        monitor_proc.terminate()
    # retrieve results
    tot_ok   = 0
    tot_fail = []
    tot_zero = []
    while True:
        try:
            res, status, qhash = out.get_nowait()
            if  status == 'ok':
                if  res:
                    tot_ok += 1
                else:
                    tot_zero.append(qhash)
            else:
                tot_fail.append(qhash)
        except:
            break
    print("+++ SUMMARY:")
    print("# queries  :", ntests)
    print("status ok  :", tot_ok)
    print("status fail:", len(tot_fail), tot_fail)
    print("nresults 0 :", len(tot_zero), tot_zero)

# part for system monitoring
def safe_value(value):
    "Return formatted float value"
    try:
        return '%s' % value
    except:
        return None

def float_value(value):
    "Return formatted float value"
    return '%0.2f' % value

def percent_value(value):
    "Return formatted percent value"
    return '%3.1f%%' % value

def get_object_data(name, obj):
    "Retrive service data"
    data = {}
    if  hasattr(obj, '__dict__'):
        for att in obj.__dict__.keys():
            item = '%s.%s' % (name, att)
            data[item] = getattr(obj, att)
    else:
        record = False
        for attr in ['total', 'used', 'free', 'percent', 'active', 'inactive']:
            if  hasattr(obj, attr):
                data['%s.%s' % (name, attr)] = getattr(obj, attr)
                record = True
        if  not record:
            data[name] = str(obj)
    return data

def get_srv_data(srv):
    "Retrive service data"
    data = {}
    for key, val in srv.items():
        data.update(get_object_data(key, val))
    return data

def monitor_process(pid, activity, sleep=5):
    "Monitor given process"
    proc = psutil.Process(pid)
    while True:
        data = {'pid': pid, 'status': proc.status,
                'timestamp': timestamp(), 'time':time.time()}
        mem_info = proc.get_memory_info()
        cpu_info = proc.get_cpu_times()
        for field in activity:
            field = field.strip()
            if field == 'cpu':
                data[field] = percent_value(proc.get_cpu_percent(interval=1.0))
            elif field == 'mem':
                data[field] = percent_value(proc.get_memory_percent())
            elif field == 'user':
                data[field] = float_value(cpu_info.user)
            elif field == 'system':
                data[field] = float_value(cpu_info.system)
            elif field == 'rss':
                data[field] = mem_info.rss
            elif field == 'vms':
                data[field] = mem_info.vms
            elif field == 'threads':
                data[field] = safe_value(proc.get_num_threads())
            elif field == 'connections':
                con_dict = {}
                for con in proc.get_connections():
                    con_dict.setdefault(con.status, []).append(con)
                for key, val in con_dict.items():
                    item = "%s.%s" % (field, key)
                    data[item] = len(val)
            elif field == 'io':
                try:
                    data[field] = safe_value(proc.get_io_counters())
                except:
                    data[field] = None
            elif field == 'files':
                data[field] = len([f for f in proc.get_open_files()])
        yield data
        time.sleep(sleep)

def monitor_system(sleep=5):
    "Monitor system activity"
    while True:
        data = {'timestamp':timestamp(), 'time':time.time()}
        data['cpu'] = psutil.cpu_percent(interval=1)
        data.update(get_object_data('vmem', psutil.virtual_memory()))
        data.update(get_object_data('swap', psutil.swap_memory()))
        yield data
        time.sleep(sleep)

def monitor_network(sleep=5):
    "Monitor network activity"
    while True:
        data = {'timestamp': timestamp(), 'time':time.time()}
        net  = psutil.network_io_counters(pernic=True)
        data.update(get_srv_data(net))
        yield data
        time.sleep(sleep)

def output(gen):
    "Yield results from given generator"
    columns  = ''
    for data in gen:
        if  not columns:
            columns = data.keys()
            columns.sort()
            yield ', '.join(columns)
        values = [str(data.get(c, 0)) for c in columns]
        yield ', '.join(values)

def monitor(pid, out, sleep=1):
    """
    Monitor function which monitor activity of given pid and write its output
    to given file name or stdout. User may supply pid=0 to monitor entire
    system or pid=-1 to monitor network interface.
    """
    act = "cpu,mem,threads,user,system,rss,vms,connections,io,files"
    activity = act.split(',')
    if  pid:
        gen = monitor_process(pid, activity, sleep)
    elif pid == 0:
        gen = monitor_system(sleep)
    else:
        gen = monitor_network(sleep)
    if  out != "stdout":
        with open(out, 'w') as stream:
            for row in output(gen):
                stream.write(row)
                stream.write('\n')
                stream.flush()
    else:
        for row in output(gen):
            print(row)

###### end of system monitoring part
if __name__ == '__main__':
    main()
