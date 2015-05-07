#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
DAS statistics tool based on results of DAS logdb, see
DAS/core/das_mongocache.py
"""

import datetime
from optparse import OptionParser
from pymongo import MongoClient
try:
#    import numpy as np
    from pylab import plt
    PLOT_ALLOWED = True
except:
    PLOT_ALLOWED = False

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("--host", action="store", type="string",\
                                          default="localhost", dest="host",\
             help="specify hostname")
        self.parser.add_option("--port", action="store", type="int",\
                                          default=8230, dest="port",\
             help="specify port")
        self.parser.add_option("--date", action="store", type="string",\
                                          default=None, dest="date",\
             help="specify date or date range, default is today")
        self.parser.add_option("--plot", action="store_true",\
                                          default=False, dest="plot",\
             help="create plots")
    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

class LogdbManager(object):
    """LogdbManager"""
    def __init__(self, dbhost, dbport, logdb, logcol):
        super(LogdbManager, self).__init__()
        conn   = MongoClient(dbhost, dbport)
        self.coll = conn[logdb][logcol]
        
    def dates(self):
        "Return registered dates from logdb"
        spec = {'type':{'$in': ['cache', 'web']}}
        min_date = 10**9
        max_date = 0
        dates = set()
        for row in self.coll.find(spec):
            if  row['date'] > max_date:
                max_date = row['date']
            if  row['date'] <= min_date:
                min_date = row['date']
            dates.add(row['date'])
        return min_date, max_date

    def stats(self, coll, date):
        """
        Collect/dump statistics from given collection and date. The date
        can be in a form of int (yyyymmdd) or mongo dict, e.g.
        {'$lt':value}
        """
        spec   = {'type':coll, 'date':date}
        if  coll in ['cache', 'merge']:
            insert = 0
            delete = 0
            for row in self.coll.find(spec):
                insert += row.get('insert', 0)
                delete += row.get('delete', 0)
            res = {'insert': insert, 'delete': delete}
        elif coll == 'web':
            path_info = {}
            queries = set()
            addrs   = set()
            for row in self.coll.find(spec):
                path = row['path']
                if  path in path_info:
                    pdict = path_info[path]
                    addrs = pdict['ip']
                    queries = pdict['queries']
                    queries.add(row['args']['input'])
                    addrs.add(row['ip'])
                else:
                    queries = set()
                    addrs   = set()
                    queries.add(row['args']['input'])
                    addrs.add(row['ip'])
                    path_info[path] = {'ip': addrs, 'queries': queries}
            for key, val in path_info.items():
                path_info[key] = \
                {'ip':len(val['ip']), 'queries':len(val['queries'])}
            res = {'queries': len(queries)}
            res.update(path_info)
        res.update({'type': coll})
        return res

def dateobj(some_date):
    "Return dateime date object for given YYYYMMDD number"
    str_date = str(some_date)
    year  = int(str_date[:4])
    month = int(str_date[4:6])
    day   = int(str_date[6:8])
    return datetime.date(year, month, day)

def collector(dbhost, dbport, logdb, logcol, date):
    "Generator which collect results"
    mgr = LogdbManager(dbhost, dbport, logdb, logcol)
    if  date:
        for coll in ['web', 'cache', 'merge']:
            res = mgr.stats(coll, date)
            res.update({'date':date})
            yield res
    else:
        min_date, max_date = mgr.dates()
        dates = dateobj(max_date) - dateobj(min_date)
        for delta in xrange(0, dates.days+1):
            datestr = dateobj(min_date) + datetime.timedelta(days=delta)
            date = int(str(datestr).replace('-', ''))
            for coll in ['web', 'cache', 'merge']:
                res = mgr.stats(coll, date)
                res.update({'date':date})
                yield res

def print_row(row, header=False):
    "Print row of stat table (either header or data)"
    frmt = "%8s %8s %8s %8s %8s %6s %6s %6s %6s"
    if  header:
        header = frmt \
        % ('date', 'cache+', 'cache-', 'merge+', 'merge-', \
                'cliip', 'cliq', 'webip', 'webq')
        print header
        print '-'*len(header)
    else:
        print frmt \
        % (row['date'], row['cachein'], row['cacheout'],
           row['mergein'], row['mergeout'], row['cliip'], row['cliq'],
           row['webip'], row['webq'])

def empty_row():
    "Return empty row for stat table"
    data = {'cachein':0, 'cacheout':0, 'mergein':0, 'mergeout':0,
            'webq':0, 'webip':0, 'cliq':0, 'cliip':0}
    return data

def filled_row(row):
    "Check if row is filled with all values"
    for val in row.values():
        if  not val:
            return False
    return True

def stats(dbhost, dbport, lobdb, lobcol, date, plot=None):
    "Main function which collects DAS statistics"
    results = collector(dbhost, dbport, lobdb, lobcol, date)
    row = empty_row()
    if  not plot:
        print_row(row, header=True)
    counter = 0
    rows = []
    for rec in results:
        date = rec['date']
        row.update({'date':date})
        coll = rec['type']
        insert  = rec.get('insert', '')
        delete  = rec.get('delete', '')
        if  coll == 'cache':
            row.update({'cachein':insert, 'cacheout':delete})
        elif  coll == 'merge':
            row.update({'mergein':insert, 'mergeout':delete})
        clicall = rec.get('/cache', '')
        if  clicall:
            cliip = clicall['ip']
            cliq  = clicall['queries']
            row.update({'cliip':cliip, 'cliq':cliq})
        webcall = rec.get('/request', '')
        if  webcall:
            webip = webcall['ip']
            webq  = webcall['queries']
            row.update({'webip':webip, 'webq':webq})
        counter += 1
        if  counter == 3: # we got info about web/cache/merge
            if  plot:
                rows.append(row)
            else:
                print_row(row)
            row = empty_row()
            counter = 0
    if  plot:
        plot_stat(rows, 'cache')
        plot_stat(rows, 'merge')
        plot_stat(rows, 'cli')
        plot_stat(rows, 'web')

def plot_stat(rows, cache):
    "Use matplotlib to plot DAS statistics"
    if  not PLOT_ALLOWED:
        raise Exception('Matplotlib is not available on the system')
    if  cache in ['cache', 'merge']: # cachein, cacheout, mergein, mergeout
        name_in  = '%sin' % cache
        name_out = '%sout' % cache
    else: # webip, webq, cliip, cliq
        name_in  = '%sip' % cache
        name_out = '%sq' % cache
    def format_date(date):
        "Format given date"
        val = str(date)
        return '%s-%s-%s' % (val[:4], val[4:6], val[6:8])
    date_range = [r['date'] for r in rows]
    formated_dates = [format_date(str(r['date'])) for r in rows]
    req_in  = [r[name_in] for r in rows]
    req_out = [r[name_out] for r in rows]

    plt.plot(date_range, req_in , 'ro-',\
             date_range, req_out, 'gv-',)
    plt.grid(True)
    plt.axis([min(date_range), max(date_range), \
                0, max([max(req_in), max(req_out)])])
    plt.xticks(date_range, tuple(formated_dates), rotation=17)
#    plt.xlabel('dates [%s, %s]' % (date_range[0], date_range[-1]))
    plt.ylabel('DAS %s behavior' % cache)
    plt.savefig('das_%s.pdf' % cache, format='pdf', transparent=True)
    plt.close()

def main():
    "Main function"
    opts, _ = DASOptionParser().get_opt() 
    dbhost = opts.host
    dbport = opts.port
    logdb  = 'logging'
    logcol = 'db'
    date   = opts.date
    plot   = opts.plot
    stats(dbhost, dbport, logdb, logcol, date, plot)

if __name__ == '__main__':
    main()
