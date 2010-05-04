#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS Query Language parser.
"""

__revision__ = "$Id: das_parser.py,v 1.4 2010/03/19 17:25:49 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import re
import time
import types
import urllib
from DAS.utils.utils import adjust_value
from DAS.core.das_ql import das_filters, das_aggregators
from DAS.core.das_ql import das_operators, MONGO_MAP, URL_MAP
from DAS.utils.regex import last_time_pattern, date_yyyymmdd_pattern
from DAS.utils.regex import key_attrib_pattern

def convert2date(value):
    """
    Convert input value to date range format expected by DAS.
    """
    msg = "Unsupported syntax for value of last operator"
    pat = last_time_pattern
    if  not pat.match(value):
        raise Exception(msg)
    oper = ' = '
    if  value.find('h') != -1:
        hour = int(value.split('h')[0])
        if  hour > 24:
            raise Exception('Wrong hour %s' % value)
        date1 = time.time() - hour*60*60
        date2 = time.time()
    elif value.find('m') != -1:
        minute = int(value.split('m')[0])
        if  minute > 60:
            raise Exception('Wrong minutes %s' % value)
        date1 = time.time() - minute*60
        date2 = time.time()
    else:
        raise Exception('Unsupported value for last operator')
    value = [long(date1), long(date2)]
    return value

def das_dateformat(value):
    """Check if provided value in expected DAS date format."""
    pat = date_yyyymmdd_pattern
    if  pat.match(value): # we accept YYYYMMDD
        ddd = datetime.date(int(value[0:4]), # YYYY
                            int(value[4:6]), # MM
                            int(value[6:8])) # DD
        return time.mktime(ddd.timetuple())
    else:
        msg = 'Unacceptable date format'
        raise Exception(msg)

def add_spaces(query):
    """Add spaces around DAS operators in input DAS query"""
    enc_query = urllib.quote(query)
    for oper in ['!=', '<=', '>=', '<', '>', '=']:
        enc_oper = URL_MAP[oper]
        space    = URL_MAP[' ']
        enc_query = enc_query.replace(enc_oper, '%s%s%s' % (space, enc_oper, space)) 
    splitted_query = urllib.unquote(enc_query)
    while True:
        splitted_query = splitted_query.replace('  ', ' ')
        if  splitted_query.find('  ') == -1:
            break
    splitted_query = splitted_query.replace('! =', '!=')
    splitted_query = splitted_query.replace('< =', '<=')
    splitted_query = splitted_query.replace('> =', '>=')
    return splitted_query

def find_das_operator(query, pos=0):
    """
    Find a das operator in input query based on provided position.
    """
    das_oper = None
    idx = 999999999
    for oper in ['%s ' % o for o in das_operators()]:
        jdx = query.find(oper, pos)
        if  jdx != -1:
            if  jdx < idx:
                das_oper = oper
                idx = jdx
    if  not das_oper:
        idx = -1
    return das_oper, idx

def find_das_word(words, query, pos=0):
    """
    Find a das word in input query based on provided position.
    """
    word = None
    idx  = 99999999999
    for das_word in ['%s ' % w for w in words]:
        jdx = query.find(das_word, pos)
        if  jdx != -1:
            if  jdx < idx:
                word = das_word
                idx = jdx
    if  not word:
        idx = -1
    return word, idx

def get_aggregator(input):
    """
    Convert input into aggregator dict, e.g. sum(block.name)
    info ('sum','block.name')
    """
    count = 0
    for item in input.split(","):
        if  count:
            msg  = "Current implementation does not support multiple"
            msg += " aggregator functions. Please use only"
            msg += " one at a time."
            raise Exception(msg)
        if  item.count("(") != item.count(")"):
            msg = "Not equal number of open/closed brackets %s" % item
            raise Exception(msg)
        left_split = item.split("(")
        func = left_split[0].strip()
        if  func not in das_aggregators():
            msg = 'Unknown aggregator function %s' % func
            raise Exception(msg)
        expr = left_split[-1].split(")")[0]
        if  len(left_split) > 2 or expr.find(",") != -1 or expr.find(" ") != -1:
            msg = 'Multiple arguments found in %s' % item
            raise Exception(msg)
        yield (func, expr)
        count += 1

def get_filters(query):
    """
    Extract from input DAS query set of filters.
    """
    mapreduce   = []
    aggregators = []
    filters     = []
    pat         = key_attrib_pattern
    if  query.find("|") != -1:
        split_results = query.split("|")
        query = split_results[0]
        for item in split_results[1:]:
            func = item.split("(")[0].strip()
            found_filter = 0
            for filter in ['%s ' % f for f in das_filters()]:
                if  item.find(filter) == -1:
                    continue
                for elem in item.replace(filter, '').split(','):
                    dasfilter = elem.strip()
                    if  not dasfilter:
                        continue
                    if  not pat.match(dasfilter):
                        msg = 'Incorrect filter: %s' % dasfilter
                        raise Exception(msg)
                    if  dasfilter not in filters:
                        filters.append(dasfilter)
                        found_filter = 1
            if func in das_aggregators():
                aggregators = [agg for agg in get_aggregator(item)]
            elif not found_filter:
                mapreduce.append(item)
    return filters, aggregators, mapreduce

def parser(query, daskeys, operators):
    """
    Parser input DAS query and convert it into MongoDB one
    """
    filters, aggregators, mapreduce = get_filters(query)
    query  = add_spaces(query)
    query  = query.split("|")[0]
    length = len(query)
    fields = []
    spec   = {}
    pos    = 0
    value  = None
    will_break = 0
    count =0 
    while True:
        das_word, idx = find_das_word(daskeys, query, pos)
        if  idx == -1:
            will_break = 1
        else:
            pos = idx + len(das_word)
            if  pos+1 < length:
                if  query[pos+1] == ',':
                    pos += 1
        oper, jdx = find_das_operator(query, pos)
        cond      = oper and jdx != -1
        word, kdx = find_das_word(daskeys, query, pos)
        if  kdx  != -1:
            cond  = cond and jdx < kdx
        if  cond:
            pos = jdx + len(oper)
            word, idx = find_das_word(daskeys, query, pos)
            if  idx == -1:
                value = query[pos:]
            else:
                value = query[pos:idx]
            value = adjust_value(value.strip())
            if  das_word:
                das_word = das_word.strip()
            if  oper:
                oper = oper.strip()
            if  type(value) is types.StringType and value[0] == '[' and\
                value[-1] == ']':
                value = value.replace('[','').replace(']','')
                value = [adjust_value(i) for i in value.split(',')]
            if  oper == '=':
                mongo_value = value
            elif oper == 'last':
                mongo_value = convert2date(value)
            elif das_word == 'date' and oper != 'last':
                if  type(value) is types.StringType:
                    value = [das_dateformat(value), time.time()]
                elif type(value) is types.ListType:
                    try:
                        value1 = das_dateformat(value[0])
                        value2 = das_dateformat(value[1])
                        value  = [value1, value2]
                    except:
                        msg = "Unable to parse %s" % value
                        raise Exception(msg)
            else:
                mongo_value = {MONGO_MAP[oper]:value}
            spec[das_word] = mongo_value
        else:
            if  das_word and das_word not in fields:
                fields.append(das_word.strip())
        if  will_break:
            break
        count += 1
        if  count == 10:
            break
    if  not fields:
        fields = None
    mongo_query = dict(fields=fields, spec=spec)
    if  mapreduce:
        mongo_query['mapreduce'] = mapreduce
    if  filters:
        mongo_query['filters'] = filters
    if  aggregators:
        mongo_query['aggregators'] = aggregators
    return mongo_query

def decompose(query):
    """Extract selection keys and conditions from input query"""
    skeys = query.get('fields', [])
    cond  = query.get('spec', {})
    return skeys, cond

class QLManager(object):
    """
    DAS QL manager.
    """
    def __init__(self, config):
        if  not config['dasmapping']:
            msg = "No mapping found in provided config=%s" % config
            raise Exception(msg)
        if  not config['dasanalytics']:
            msg = "No analytics found in provided config=%s" % config
            raise Exception(msg)
        if  not config['dasmapping'].check_maps():
            msg = "No DAS maps found in MappingDB"
            raise Exception(msg)
        self.map         = config['dasmapping']
        self.analytics   = config['dasanalytics']
        self.daskeysmap  = self.map.daskeys()
        self.operators   = das_operators()
        self.daskeys     = ['system', 'date'] # two reserved words
        for val in self.daskeysmap.values():
            for item in val:
                self.daskeys.append(item)

    def parse(self, query, add_to_analytics=True):
        """
        Parse input query and return query in MongoDB form.
        Optionally parsed query can be written into analytics DB.
        """
        if  query and type(query) is types.StringType and \
            query[0] == "{" and query[-1] == "}":
            mongo_query = json.loads(query)
            if  mongo_query.keys() != ['fields', 'spec']:
                raise Exception("Invalid MongoDB query %s" % query)
            if  add_to_analytics:
                self.analytics.add_query(query, mongo_query)
            return mongo_query
        mongo_query = parser(query, self.daskeys, self.operators)
        # convert DAS input keys into DAS selection keys
        for key in mongo_query['spec'].keys():
            for system in self.map.list_systems():
                mapkey = self.map.find_mapkey(system, key)
                if  mapkey and mapkey != key and \
                    mongo_query['spec'].has_key(key):
                    mongo_query['spec'][mapkey] = mongo_query['spec'][key]
                    del mongo_query['spec'][key]
                    continue
        if  add_to_analytics:
            self.analytics.add_query(query, mongo_query)
        return mongo_query

    def services(self, query):
        """Find out DAS services to use for provided query"""
        skeys, cond = decompose(query)
        if  not skeys:
            skeys = []
        if  type(skeys) is types.StringType:
            skeys = [skeys]
        slist = []
        # look-up services from Mapping DB
        for key in skeys + [i for i in cond.keys()]:
            for service, keys in self.daskeysmap.items():
                daskeys = self.map.find_daskey(service, key)
                if  set(keys) & set(daskeys) and service not in slist:
                    slist.append(service)
        return slist

    def service_apis_map(self, query):
        """
        Find out which APIs correspond to provided query.
        Return a map of found services and their apis.
        """
        skeys, cond = decompose(query)
        if  not skeys:
            skeys = []
        if  type(skeys) is types.StringType:
            skeys = [skeys]
        adict = {}
        mapkeys = [key for key in cond.keys()]
        services = [srv for srv in self.map.list_systems()]
        for srv in services:
            alist = self.map.list_apis(srv)
            for api in alist:
                daskeys = self.map.api_info(api)['daskeys']
                maps = [r['map'] for r in daskeys]
                if  set(mapkeys) & set(maps) == set(mapkeys): 
                    if  adict.has_key(srv):
                        new_list = adict[srv] + [api]
                        adict[srv] = list( set(new_list) )
                    else:
                        adict[srv] = [api]
        return adict

    def params(self, query):
        """
        Return dictionary of parameters to be used in DAS Core:
        selection keys, conditions and services.
        """
        skeys, cond = decompose(query)
        services = []
        for srv in self.services(query):
            if  srv not in services:
                services.append(srv)
        return dict(selkeys=skeys, conditions=cond, services=services)

