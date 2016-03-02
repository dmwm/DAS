#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das_ql_parser.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: DAS Query Parser module. It tasks to parse input DAS queryes
             and yield tokens in a form of DASPLY module.
             This module meant to replace DASPLY and be thread-safe.
"""

# system modules
from __future__ import print_function
from   optparse import OptionParser
import json

# das modules
from DAS.core.das_ql import das_filters, das_operators, das_mapreduces
from DAS.core.das_ql import das_aggregators, das_special_keys
from DAS.utils.regex import date_yyyymmdd_pattern, int_number_pattern, float_number_pattern
from DAS.utils.utils import das_dateformat

class DASOptionParser(object):
    "Option parser class"
    def __init__(self):
        "User based option parser"
        usage  = "Usage: %prog [options]\n"
        usage += "For more help please visit https://cmsweb.cern.ch/das/faq"
        self.parser = OptionParser(usage=usage)
        self.parser.add_option("--query", action="store", \
            dest="query", default="", help="DAS query")
        self.parser.add_option("--verbose", action="store", \
            dest="verbose", default=0, help="verbosity level")
        self.parser.add_option("--timeit", action="store", \
            dest="timeit", default=0, help="timeit with provided number of iterations")

def adjust_value(val):
    "Adjust value to DAS patterns"
    if  date_yyyymmdd_pattern.match(val):
        return das_dateformat(val)
    elif  int_number_pattern.match(val):
        return int(val)
    return val

def relax(query, operators):
    "Add spaces around special symbols"
    qlen = len(query)
    for oper in operators + ['(', ')', '>', '<', '!', '[', ']', ',', '=']:
        if  oper in ['in', 'between', 'last']:
            continue
        query = query.replace(oper, ' %s ' % oper)
    query = ' '.join(q.strip() for q in query.split())
    qlen = len(query)
    idx = 0
    out = ''
    while idx < qlen:
        val = query[idx]
        nval = query[idx+1] if idx+1 < qlen else None
        nnval = query[idx+2] if idx+2 < qlen else None
        if  nnval and nval and val in ['<', '>', '!'] and nnval == '=':
            out += '%s%s ' % (val, nnval)
            idx += 3
        else:
            out += val
            idx += 1
    out = ' '.join(o.strip() for o in out.split())
    return out

def parse_quotes(query, quote):
    """
    Parse quotes boundaries in query and return un-quoted value and new index
    """
    idx = None
    jdx = None
    for iii in range(0, len(query)):
        item = query[iii]
        if  item.startswith(quote):
            if  not idx:
                idx = iii
        if  item.endswith(quote):
            if  not jdx:
                jdx = iii
                break
    if  idx==None or jdx==None:
        error(query, idx, 'Fail to extract value from quotes')
    val = ' '.join(query[idx:jdx+1]).replace(quote, '')
    if  not val:
        error(query, idx, 'Fail to extract value from quotes')
    return val, jdx+1

def parse_curle_brackets(query):
    """
    Parse curle bracket boundaries in query and return array and new index
    Return [('array', begin, end), shift]
    """
    idx = query.index('(')
    jdx = query.index(')')
    val = query[idx+1:jdx]
    if  len(val) != 1:
        error(query, idx, 'Fail to extract value from curle brackets')
    return val[0], jdx+1

def parse_array(query, oper, daskey):
    """
    Parse array boundaries in query and return array and new index
    Return [('array', begin, end), shift]
    """
    idx = query.index('[')
    jdx = query.index(']')
    try:
        subq = ' '.join(query[idx:jdx+1])
        if  daskey == 'dataset':
            subq = subq.replace('[', '["').replace(']', '"]').replace(',', '","')
            subq = ''.join([s.strip() for s in subq.split()])
        val = json.loads(subq)
    except Exception as exc:
        error(query, idx, 'Fail to extract value from square brackets, '+str(exc))
    if  oper == 'in':
        out = val
    elif oper == 'between':
        out = [min(val), max(val)]
    else:
        error(query, idx, 'Fail to extract value from square brackets')
    if  daskey == 'date':
        for item in val:
            if  not date_yyyymmdd_pattern.match(str(item)):
                error(query, idx, 'Wrong date value, use YYYYMMDD format')
    return out, jdx+1

def error(query, idx, msg='Parsing failure'):
    "Form error message and raise appropriate exception message"
    out = ' '.join(query)
    where = ''
    for jdx in range(0, idx):
        where += '-'*(len(query[jdx])+1)
    where += '^'
    msg += '\n' + out + '\n' + where
    raise Exception(msg)

def spec_entry(key, oper, val):
    "Convert key oper val triplet into MongoDB spec entry"
    spec = {}
    if  oper == '=' or oper == 'last':
        spec[key] = val
        if  key == 'date' and date_yyyymmdd_pattern.match(val):
            spec[key] = das_dateformat(val)
    elif oper == 'in' and isinstance(val, list):
        spec[key] = {'$in': val}
        if  key == 'date':
            out = [das_dateformat(d) for d in val]
            spec[key] = {'$in': out}
    elif oper == 'between' and isinstance(val, list):
        spec[key] = {'$gte': min(val), '$lte': max(val)}
        if  key == 'date':
            spec[key] = {'$gte': das_dateformat(str(min(val))),
                         '$lte': das_dateformat(str(max(val)))}
    else:
        Exception('Not implemented spec entry')
    return spec

def daskeyvalue_check(query, value, daskeys):
    "Check that value to start with DAS key"
    val = value.split('.')[0].split('=')[0].split('<')[0].split('>')[0].strip()
    if  val not in daskeys:
        iii = 0
        for iii in range(0, len(query)):
            if  query[iii].startswith(val):
                break
        error(query, iii, 'Filter value does not start with DAS key')

class DASQueryParser(object):
    """docstring for DASQueryParser"""
    def __init__(self, daskeys, dassystems,
                 operators=None, specials=None, filters=None,
                 aggregators=None, mapreduces=None, verbose=0):
        super(DASQueryParser, self).__init__()
        self.daskeys = daskeys + ['queries', 'records']
        self.verbose = verbose
        self.dassystems = dassystems
        # test if we have been given a list of desired operators/filters
        # /aggregators, if not get the lists from das_ql
        self.operators = operators if operators else das_operators()
        self.filters = filters if filters else das_filters()
        self.aggregators = aggregators if aggregators else das_aggregators()
        self.mapreduces = mapreduces if mapreduces else das_mapreduces()
        self.specials = specials if specials else das_special_keys()
        if  self.verbose:
            print("operators", self.operators)
            print('filters', self.filters)
            print('mapreduces', self.mapreduces)
            print('specials', self.specials)
        self.daskeys += self.specials

    def parse(self, query):
        "Parse input query"
        spec = {}
        filters = {}
        aggregators = []
        fields = []
        keys = []
        pipe = []
        relaxed_query = relax(query, self.operators).split()
        if  self.verbose:
            print("\n### input query=%s, relaxed=%s" % (query, relaxed_query))
        tot = len(relaxed_query)
        idx = 0
        while idx < tot:
            item = relaxed_query[idx]
            if  self.verbose > 1:
                print("parse item", item)
            if  item == '|':
                step = self.parse_pipe(relaxed_query[idx:], filters, aggregators)
                idx += step
            if  item == ',':
                idx += 1
                continue
            next_elem = relaxed_query[idx+1] if idx+1 < tot else None
            next_next_elem = relaxed_query[idx+2] if idx+2 < tot else None
            if  self.verbose > 1:
                print("### parse items", item, next_elem, next_next_elem)
            if  next_elem and (next_elem == ',' or next_elem in self.daskeys):
                if  item in self.daskeys:
                    fields.append(item)
                idx += 1
                continue
            elif next_elem in self.operators:
                oper = next_elem
                if  item not in self.daskeys+self.specials:
                    error(relaxed_query, idx, 'Wrong DAS key')
                if  next_next_elem.startswith('['):
                    val, step = parse_array(relaxed_query[idx:], next_elem, item)
                    spec.update(spec_entry(item, next_elem, val))
                    idx += step
                elif next_elem in ['in', 'beetween'] and \
                     not next_next_elem.startswith('['):
                    msg = '"%s" operator ' % next_elem
                    msg += 'should be followed by square bracket value'
                    error(relaxed_query, idx, msg)
                elif next_next_elem.startswith('"'):
                    val, step = parse_quotes(relaxed_query[idx:], '"')
                    spec.update(spec_entry(item, next_elem, val))
                    idx += step
                elif next_next_elem.startswith("'"):
                    val, step = parse_quotes(relaxed_query[idx:], "'")
                    spec.update(spec_entry(item, next_elem, val))
                    idx += step
                else:
                    if  float_number_pattern.match(next_next_elem):
                        next_next_elem = float(next_next_elem)
                    elif int_number_pattern.match(next_next_elem) and \
                        not date_yyyymmdd_pattern.match(next_next_elem):
                        next_next_elem = int(next_next_elem)
                    elif next_next_elem in self.daskeys:
                        msg = 'daskey operator daskey structure is not allowed'
                        error(relaxed_query, idx, msg)
                    spec.update(spec_entry(item, next_elem, next_next_elem))
                    idx += 3
                continue
            elif item == '|':
                step = self.parse_pipe(relaxed_query[idx:], filters, aggregators)
                idx += step
            elif not next_elem and not next_next_elem:
                if  item in self.daskeys:
                    fields.append(item)
                    idx += 1
                else:
                    error(relaxed_query, idx, 'Not a DAS key')
            else:
                error(relaxed_query, idx)
        out = {}
        for word in ['instance', 'system']:
            if  word in spec:
                out[word] = spec.pop(word)
        if  not fields:
            fields = [k for k in spec.keys() if k in self.daskeys]
            if  len(fields) > 1:
                fields = None # ambiguous spec, we don't know which field to look-up
        if  fields and not spec:
            error(relaxed_query, 0, 'No conditition specified')
        out['fields'] = fields
        out['spec'] = spec
        # perform cross-check of filter values
        for key, item in filters.items():
            if  key not in ['grep', 'sort']:
                continue
            for val in item:
                daskeyvalue_check(query, val, self.daskeys)
        # perform cross-check of aggregator values
        for _, val in aggregators:
            daskeyvalue_check(query, val, self.daskeys)
        if  filters:
            out['filters'] = filters
        if  aggregators:
            out['aggregators'] = aggregators

        if  self.verbose:
            print("MongoDB query: %s" % out)
        return out

    def parse_pipe(self, query, filters, aggregators):
        "Parse DAS query pipes"
        if  self.verbose > 1:
            print("parse_pipe", query)
        if  not len(query):
            return 1 # advance index
        cfilter = None
        tot = len(query)
        idx = query.index('|') + 1
        while idx < tot:
            item = query[idx].strip()
            if  self.verbose > 1:
                print("parse_pipe item", item, idx)
            if  item == '|': # new pipe
                break
            elif  item == 'grep':
                cfilter = item
                filters[item] = []
                idx += 1
                continue
            elif item == 'unique':
                cfilter = item
                filters[item] = 1
                idx += 1
                continue
            elif item == 'sort':
                cfilter = item
                filters[item] = []
                idx += 1
                continue
            elif item in self.aggregators:
                cfilter = item
                val, step = parse_curle_brackets(query[idx:])
                if  val:
                    aggregators.append((item, val))
                idx += step
            elif item == ',':
                idx += 1
                continue
            elif cfilter == 'grep':
                filters['grep'].append(item)
            elif cfilter == 'sort':
                filters['sort'].append(item)
            else:
                error(query, idx, 'Fail to parse filters')
            idx += 1
        if  self.verbose > 1:
            print("filters", filters)
            print("aggregators", aggregators)
        for key, val in filters.items():
            if  key == 'grep':
                if  len(val) > 2:
                    new_val = []
                    iii = 0
                    while iii < len(val):
                        vvv = val[iii]
                        nval = val[iii+1] if iii+1 < len(val) else None
                        nnval = val[iii+2] if iii+2 < len(val) else None
                        if  nnval and nval in ['<', '>', '=', '<=', '>=', '!=']:
                            new_val.append('%s%s%s' % (vvv, nval, nnval))
                            iii += 3
                        else:
                            new_val.append(vvv)
                            iii += 1
                    val = new_val
                else:
                    val = ','.join(val).split(',')
                filters[key] = val
        return idx

def test_parser(query, verbose=0):
    "Test function"
    daskeys = ['file', 'dataset', 'site', 'run', 'lumi', 'block']
    dassystems = ['dbs', 'phedex']
    mgr = DASQueryParser(daskeys, dassystems, verbose=verbose)
    obj = mgr.parse(query)
    return obj

def main():
    "Main function"
    optmgr = DASOptionParser()
    opts = optmgr.parser.parse_args()
    query = opts.query
    verbose = opts.verbose
    print(test_parser(query, verbose))
    if  opts.timeit:
        import timeit
        niter = int(opts.timeit)
        args = 'from __main__ import test_parser\nquery="%s"\nverbose=%s' % (query, verbose)
        TIME = timeit.timeit("test_parser(query, verbose)", setup=args, number=niter)
        print("Benchmark: %e sec/query (%s iterations)" % (TIME/niter, niter))

if __name__ == '__main__':
    main()
