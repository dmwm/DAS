#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunRegistry service
"""
from __future__ import print_function
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import os
import time
import calendar
import datetime
import xmlrpclib
import urllib
import DAS.utils.jsonwrapper as json
from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator, adjust_value, convert_datetime
from   DAS.utils.utils import convert2date, print_exc, convert2ranges

def rr_date(timestamp):
    """
    Convert given timestamp into RR date format, YYYY-MM-DD
    """
    if  isinstance(timestamp, int) or isinstance(timestamp, long):
        return time.strftime("%Y-%m-%d", time.gmtime(timestamp))
    return timestamp

def duration(ctime, etime):
    """
    Calculate run duration.
    """
    if  not ctime or not etime:
        return 'N/A'
    dformat = "%a %d-%m-%y %H:%M:%S" # Sun 15-05-11 17:25:00
    csec = time.strptime(ctime.split('.')[0], dformat)
    esec = time.strptime(etime.split('.')[0], dformat)
    tot  = calendar.timegm(csec) - calendar.timegm(esec)
    return str(datetime.timedelta(seconds=abs(tot)))

def convert_time(ctime):
    "Convert RR timestamp into sec since epoch"
    if  not ctime:
        return ctime
    dformat = "%a %d-%m-%y %H:%M:%S" # Sun 15-05-11 17:25:00
    sec = time.strptime(ctime.split('.')[0], dformat)
    return long(calendar.timegm(sec))

def collect_lumis(records):
    "Helper function to collect lumi records for same runs"
    rec = {}
    for row in records:
        if  not rec:
            rec.update(row)
            continue
        if  row['lumi']['run_number'] == rec['lumi']['run_number']:
            lumis = rec['lumi']
            lumis['number'] += row['lumi']['number']
            rec['lumi'] = lumis
        else:
            rec['lumi']['number'] = convert2ranges(rec['lumi']['number'])
            yield rec
            rec = {}
    if  rec:
        rec['lumi']['number'] = convert2ranges(rec['lumi']['number'])
        yield rec

def run_duration(records):
    """
    Custom parser to produce run duration out of RR records and
    convert time stamps into DAS notations.
    """
    times = ['start_time', 'end_time', 'creation_time', 'modification_time']
    for row in records:
        if  'run' not in row:
            continue
        run = row['run']
        if  isinstance(run, dict):
            for key in times:
                if  key in run:
                    run[key] = convert_time(run[key])
            if  run['creation_time'] and run['end_time']:
                tot = abs(run['creation_time']-run['end_time'])
                run['duration'] = str(datetime.timedelta(seconds=tot))
            elif run['start_time'] and run['end_time']:
                tot = abs(run['start_time']-run['end_time'])
                run['duration'] = str(datetime.timedelta(seconds=tot))
            else:
                run['duration'] = None
        yield row

def worker_helper(url, query, table='runsummary'):
    """
    Query RunRegistry service, see documentation at
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/DqmRrApi
    url=http://runregistry.web.cern.ch/runregistry/
    """
    workspace = 'GLOBAL'
    template = 'json'
    if  table == 'runsummary':
        columns = ['number', 'startTime', 'stopTime', 'triggers',
                   'runClassName', 'runStopReason', 'bfield', 'gtKey',
                   'l1Menu', 'hltKeyDescription', 'lhcFill', 'lhcEnergy',
                   'runCreated', 'modified', 'lsCount', 'lsRanges']
    elif table == 'runlumis':
        columns = ['sectionFrom', 'sectionTo', 'runNumber']
    sdata = json.dumps({'filter':query})
    path = 'api/GLOBAL/%s/%s/%s/none/data' \
                % (table, template, urllib.quote(','.join(columns)))
    callurl = os.path.join(url, path)
    result = urllib.urlopen(callurl, sdata)
    record = json.load(result)
    result.close()
    notations = {'lsRanges':'lumi_section_ranges',
            'number':'run_number', 'runCreated':'create_time',
            'runNumber': 'run_number',
            'stopTime': 'end_time', 'startTime': 'start_time',
            'lsCount': 'lumi_sections', 'runStopReason': 'stop_reason',
            'hltKeyDescription': 'hltkey', 'gtKey': 'gtkey',
            'lhcEnergy': 'beam_e', 'l1Menu': 'l1key',
            'modified': 'modify_time', 'runClassName': 'group_name'}
    for rec in record:
        for key, val in rec.items():
            if  key in notations:
                rec[notations[key]] = val
                del rec[key]
        if  table == 'runsummary':
            yield dict(run=rec)
        elif table == 'runlumis':
            if  'sectionTo' in rec and 'sectionFrom' in rec:
                rec['number'] = [i for i in \
                        range(rec.pop('sectionFrom'), rec.pop('sectionTo')+1)]
            yield dict(lumi=rec)

def rr_worker(url, query, table='runsummary'):
    "Call RunRegistry APIs"
    for key, val in query.items():
        if  key == 'runNumber' and table == 'runsummary':
            query['number'] = val
            del query['runNumber']
        if  key == 'runStartTime':
            query['startTime'] = val
            del query['runStartTime']
    for row in worker_helper(url, query, table):
        yield row

class RunRegistryService(DASAbstractService):
    """
    Helper class to provide RunRegistry service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'runregistry', config)
        self.headers = {'Accept': 'text/json;application/json'}
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def apicall(self, dasquery, url, api, args, dformat, expire):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        _query  = ''
        _table  = 'runsummary'
        if  api == 'rr_xmlrpc_lumis':
            _table = 'runlumis'
        for key, val in dasquery.mongo_query['spec'].iteritems():
            if  key == 'run.run_number':
                if  isinstance(val, int):
                    _query = {'runNumber': '%s' % val}
                elif isinstance(val, dict):
                    minrun = 0
                    maxrun = 0
                    for kkk, vvv in val.iteritems():
                        if  kkk == '$in':
                            runs = ' or '.join([str(r) for r in vvv])
                            _query = {'runNumber': runs}
                        elif kkk == '$lte':
                            maxrun = vvv
                        elif kkk == '$gte':
                            minrun = vvv
                    if  minrun and maxrun:
                        _query = {'runNumber': '>= %s and < %s' % (minrun, maxrun)}
            elif key == 'date':
                if  isinstance(val, dict):
                    if  '$in' in val:
                        value = val['$in']
                    elif '$lte' in val and '$gte' in val:
                        value = (val['$gte'], val['$lte'])
                    else:
                        msg = 'Unable to get the value from %s=%s' \
                                % (key, val) 
                        raise Exception(msg)
                    try:
                        date1 = convert_datetime(value[0])
                        date2 = convert_datetime(value[-1])
                    except:
                        msg = 'Unable to convert to datetime format, %s' \
                            % value
                        raise Exception(msg)
                elif  isinstance(val, str) or isinstance(val, unicode):
                    date1, date2 = convert2date(val)
                    date1 = rr_date(date1)
                    date2 = rr_date(date2)
                else:
                    date1 = convert_datetime(val)
                    date2 = convert_datetime(val + 24*60*60)
                run_time = '>= %s and < %s' % (date1, date2)
                _query = {'runStartTime': run_time}
            else:
                msg  = 'RunRegistryService::api\n\n'
                msg += "--- %s reject API %s, parameters don't match, args=%s" \
                        % (self.name, api, args)
                self.logger.info(msg)
                return
        if  not _query:
            msg = 'Unable to match input parameters with input query'
            raise Exception(msg)
        if  'run' in args and isinstance(args['run'], dict):
            args['run'] = str(args['run'])
        msg = "DASAbstractService:RunRegistry, query=%s" % _query
        self.logger.info(msg)
        time0   = time.time()
        rawrows = rr_worker(url, _query, _table)
        genrows = self.translator(api, rawrows)
        if  _table == 'runsummary':
            dasrows = run_duration(genrows)
        else:
            dasrows = collect_lumis(genrows)
        ctime   = time.time() - time0
        try:
            self.write_to_cache(\
                dasquery, expire, url, api, args, dasrows, ctime)
        except Exception as exc:
            print_exc(exc)

def test():
    "Test function"
#    query = "{runNumber} >= 135230 and {runNumber} <= 135230"
#    query = {'runStartTime': '>= 2010-10-18 and < 2010-10-22'}
#    query = {'runNumber': '>= 137081 and < 137088'}
#    query = {'runNumber': '>= 147623 and <= 147623'}
#    query = {'runNumber': '147623'}
#    query = {'runNumber': '165103'}
#    ver = 2
    url = 'http://localhost:8081/cms-service-runregistry-api/xmlrpc'
    query = {'startTime': '>= 2010-10-18 and < 2010-10-22'}
    query = {'number': '165103'}
    query = {'number': '>= 165103 and <= 165110'}
    url = 'http://localhost:8081/runregistry/'
    for row in rr_worker(url, query):
        print(row, type(row))

if __name__ == '__main__':
    test()
