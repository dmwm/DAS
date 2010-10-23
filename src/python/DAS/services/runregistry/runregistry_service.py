#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunRegistry service
"""
__revision__ = "$Id"
__version__ = "$Revision"
__author__ = "Valentin Kuznetsov"

import time
import xmlrpclib
import DAS.utils.jsonwrapper as json
from   DAS.services.abstract_service import DASAbstractService
from   DAS.utils.utils import map_validator, adjust_value

def convert_datetime(sec):
    """
    Convert seconds since epoch or YYYYMMDD to date format used in RunRegistry
    """
    value = str(sec)
    if  value == 8: # we got YYYYMMDD
        return "%s-%s-%s" % (value[:4], value[5:6], value[7:8])
    return time.strftime("%Y-%m-%d", time.gmtime(sec))
    
def worker(url, query):
    """
    Query RunRegistry service, see documentation at
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/DqmRrApi
    url=http://pccmsdqm04.cern.ch/runregistry/xmlrpc
    """
    server    = xmlrpclib.ServerProxy(url)
    namespace = 'GLOBAL'
    if  isinstance(query, str):
        data = server.RunLumiSectionRangeTable.exportJson(namespace, query)
        for row in json.loads(data):
            yield row
    elif isinstance(query, dict):

        # example of using xml_all
#        format = 'xml_all'
#        data = server.RunDatasetTable.export(namespace, format, query)
#        handle, tname = tempfile.mkstemp()
#        fds = open(tname, 'w')
#        fds.write(data)
#        fds.close()
#        with open(tname) as source:
#            gen = xml_parser(source, prim_key='RUN')
#            for row in gen:
#                yield row
#        os.remove(tname)

        format = 'tsv_runs' # other formats are xml_all, csv_runs
        data   = server.RunDatasetTable.export(namespace, format, query)
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

class RunRegistryService(DASAbstractService):
    """
    Helper class to provide RunRegistry service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'runregistry', config)
        self.headers = {'Accept': 'text/json;application/json'}
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def api(self, query):
        """
        A service worker. It parses input query, invoke service API 
        and return results in a list with provided row.
        """
        api     = self.map.keys()[0] # we have only one key
        url     = self.map[api]['url']
        expire  = self.map[api]['expire']
        args    = dict(self.map[api]['params'])
        _query  = ""
        for key, val in query['spec'].items():
            if  key == 'run.run_number':
                if  isinstance(val, int):
                    _query += "{runNumber} >= %s and {runNumber} <= %s" \
                                % (val, val)
                elif isinstance(val, dict):
                    minrun = 0
                    maxrun = 0
                    for kkk, vvv in val.items():
                        if  kkk == '$in':
                            minrun, maxrun = vvv
                        elif kkk == '$lte':
                            minrun = vvv
                        elif kkk == '$gte':
                            maxrun = vvv
                    _query += "{runNumber} >= %s and {runNumber} <= %s" \
                            % (minrun, maxrun)
            elif key == 'date':
                if  val.has_key('$in'):
                    value = val['$in']
                    date1 = convert_datetime(value[0])
                    date2 = convert_datetime(value[-1])
                    run_time = '>= %s and < %s' % (date1, date2)
                    _query = {'runStartTime': run_time}
        if  not _query:
            msg = 'Unable to match input parameters with input query'
            raise Exception(msg)
        msg = "DASAbstractService:RunRegistry, query=%s" % _query
        self.logger.info(msg)
        time0   = time.time()
        rawrows = worker(url, _query)
        genrows = self.translator(api, rawrows)
        ctime   = time.time() - time0
        self.write_to_cache(query, expire, url, api, args, genrows, ctime)
#
if __name__ == '__main__':
    QUERY = "{runNumber} >= 135230 and {runNumber} <= 135230"
    QUERY = {'runStartTime': '>= 2010-10-18 and < 2010-10-22'}
    URL = 'http://localhost:8080/runregistry/xmlrpc'
    for row in worker(URL, QUERY):
        print row, type(row)
