#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunRegistry service
"""
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
from   DAS.utils.utils import convert2date, print_exc

def rr_date(timestamp):
    """
    Convert given timestamp into RR date format, YYYY-MM-DD
    """
    if  isinstance(timestamp, int) or isinstance(timestamp, long):
        return time.strftime("%Y-%m-%d", time.gmtime(timestamp))
    return timestamp

def duration(ctime, etime, api_ver=2):
    """
    Calculate run duration.
    """
    if  not ctime or not etime:
        return 'N/A'
    if  api_ver == 2:
        dformat = "%Y-%m-%dT%H:%M:%S" # 2010-10-09T17:39:51.0
    else:
        dformat = "%a %d-%m-%y %H:%M:%S" # Sun 15-05-11 17:25:00
    csec = time.strptime(ctime.split('.')[0], dformat)
    esec = time.strptime(etime.split('.')[0], dformat)
    tot  = calendar.timegm(csec) - calendar.timegm(esec)
    return str(datetime.timedelta(seconds=abs(tot)))

def run_duration(records, api_ver=2):
    """
    Custom parser to produce run duration out of RR records
    """
    for row in records:
        if  not row.has_key('run'):
            continue
        run = row['run']
        if  isinstance(run, dict):
            if  run.has_key('create_time') and run.has_key('end_time'):
                ctime = run['create_time']
                etime = run['end_time']
                run['duration'] = duration(ctime, etime, api_ver)
        yield row

def worker_v2(url, query):
    """
    Query RunRegistry service, see documentation at
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/DqmRrApi
    url=http://pccmsdqm04.cern.ch/runregistry/xmlrpc
    """
    server    = xmlrpclib.ServerProxy(url)
    namespace = 'GLOBAL'
    if  isinstance(query, str) or isinstance(query, unicode):
        try:
            data = server.RunLumiSectionRangeTable.exportJson(namespace, query)
        except Exception as _err:
            data = "{}" # empty response
        for row in json.loads(data):
            yield row
    elif isinstance(query, dict):
        iformat  = 'tsv_runs' # other formats are xml_all, csv_runs
        try:
            data = server.RunDatasetTable.export(namespace, iformat, query)
        except Exception as _err:
            data = "" # empty response
        titles = []
        for line in data.split('\n'):
            if  not line:
                continue
            if  not titles:
                for title in line.split('\t')[:-1]:
                    title = title.lower()
                    if  title != 'run_number':
                        title = title.replace('run_', '')
                    titles.append(title)
                continue
            val = line.split('\t')[:-1]
            if  len(val) != len(titles):
                continue
            record = {}
            for idx in range(0, len(titles)):
                key = titles[idx]
                record[key] = adjust_value(val[idx])
            yield dict(run=record)

def worker_v3(url, query):
    """
    Query RunRegistry service, see documentation at
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/DqmRrApi
    url=http://runregistry.web.cern.ch/runregistry/
    """
    workspace = 'GLOBAL'
    table = 'runsummary'
    template = 'json'
    columns = ['number', 'startTime', 'stopTime', 'triggers',
               'runClassName', 'runStopReason', 'bfield',
               'l1Menu', 'hltkey', 'lhcFill', 'lhcEnergy',
               'runCreated', 'modified', 'lsCount', 'lsRanges']
    sdata = json.dumps({'filter':query})
    path = 'api/GLOBAL/%s/%s/%s/none/data' \
                % (table, template, urllib.quote(','.join(columns)))
    callurl = os.path.join(url, path)
    result = urllib.urlopen(callurl, sdata)
    record = json.load(result)
    notations = {'lsRanges':'lumi_section_ranges',
            'number':'run_number', 'runCreated':'create_time',
            'stopTime': 'end_time', 'startTime': 'start_time',
            'lsCount': 'lumi_sections', 'runStopReason': 'stop_reason',
            'lhcEnergy': 'beam_e', 'l1Menu': 'l1key',
            'modified': 'modify_time', 'runClassName': 'group_name'}
    for rec in record:
        for key, val in rec.items():
            if  notations.has_key(key):
                rec[notations[key]] = val
                del rec[key]
        yield dict(run=rec)

def worker(url, query, api_ver):
    "Call RunRegistry APIs"
    if  api_ver == 2:
        for row in worker_v2(url, query):
            yield row
    elif api_ver == 3:
        for key, val in query.items():
            if  key == 'runNumber':
                query['number'] = val
                del query['runNumber']
            if  key == 'runStartTime':
                query['startTime'] = val
                del query['runStartTime']
        for row in worker_v3(url, query):
            yield row
    else:
        msg = 'Unsupported RunRegistry API, version=%s' % api_ver
        raise Exception(msg)

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
        _query  = ""
        for key, val in dasquery.mongo_query['spec'].iteritems():
            if  key == 'run.run_number':
                if  isinstance(val, int):
                    _query = {'runNumber': '%s' % val}
                elif isinstance(val, dict):
                    minrun = 0
                    maxrun = 0
                    for kkk, vvv in val.iteritems():
                        if  kkk == '$in':
                            if len(vvv) == 2:
                                minrun, maxrun = vvv
                            else: # in[1, 2, 3]
                                msg = "runregistry can not deal with 'in'"
                                self.logger.info(msg)
                                continue
                        elif kkk == '$lte':
                            maxrun = vvv
                        elif kkk == '$gte':
                            minrun = vvv
                    _query = {'runNumber': '>= %s and < %s' % (minrun, maxrun)}
            elif key == 'date':
                if  isinstance(val, dict):
                    if  val.has_key('$in'):
                        value = val['$in']
                    elif val.has_key('$lte') and val.has_key('$gte'):
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
        if  args.has_key('run') and isinstance(args['run'], dict):
            args['run'] = str(args['run'])
        msg = "DASAbstractService:RunRegistry, query=%s" % _query
        self.logger.info(msg)
        time0   = time.time()
        api_ver = 3 # API version for RunRegistry, v2 is xmlrpc, v3 is REST
        rawrows = worker(url, _query, api_ver)
        genrows = self.translator(api, rawrows)
        dasrows = self.set_misses(dasquery, api, run_duration(genrows, api_ver))
        ctime   = time.time() - time0
        try:
            self.write_to_cache(\
                dasquery, expire, url, api, args, dasrows, ctime)
        except Exception as exc:
            print_exc(exc)

def test():
    "Test function"
    query = "{runNumber} >= 135230 and {runNumber} <= 135230"
    query = {'runStartTime': '>= 2010-10-18 and < 2010-10-22'}
    query = {'runNumber': '>= 137081 and < 137088'}
    query = {'runNumber': '>= 147623 and <= 147623'}
    query = {'runNumber': '147623'}
    query = {'runNumber': '165103'}
    ver = 2
    url = 'http://localhost:8081/cms-service-runregistry-api/xmlrpc'
    query = {'startTime': '>= 2010-10-18 and < 2010-10-22'}
    query = {'number': '165103'}
    query = {'number': '>= 165103 and <= 165110'}
    url = 'http://localhost:8081/runregistry/'
    ver = 3
    for row in worker(url, query, ver):
        print row, type(row)

if __name__ == '__main__':
    test()
