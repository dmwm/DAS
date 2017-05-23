#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0201,W0703,W0702,R0914,R0912,R0904,R0915,W0107,C0321
"""
File: cms_representation.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: CMS DAS records representation
"""
from __future__ import print_function

import sys
# python 3
if  sys.version.startswith('3.'):
    import urllib.parse as urllib
else:
    import urllib
# system modules
import time
import json
from itertools import groupby

# mongodb modules
from bson.objectid import ObjectId

# DAS modules
from DAS.core.das_ql import das_aggregators, das_filters, das_record_keys
from DAS.core.das_query import DASQuery
from DAS.utils.ddict import DotDict
from DAS.utils.utils import print_exc, getarg, size_format, access, convert2ranges
from DAS.utils.utils import identical_data_records, dastimestamp, sort_rows
from DAS.web.utils import das_json, quote, gen_color, not_to_link
from DAS.web.tools import exposetext
from DAS.web.das_representation import DASRepresentation
from DAS.utils.regex import int_number_pattern

def make_args(key, val, inst):
    """
    Helper function to make appropriate url parameters
    """
    return urllib.urlencode({'input':'%s=%s' % (key, val), 'instance':inst})

def make_links(links, value, inst):
    """
    Make new link for provided query links and passed value.
    """
    if  isinstance(value, list):
        values = value
    else:
        values = [value]
    for link in links:
        for val in values:
            if  'query' in link:
                dasquery = link['query'] % val
                uinput = urllib.quote(dasquery)
                url = '/das/request?input=%s&instance=%s&idx=0&limit=10' \
                            % (uinput, inst)
                if  link['name']:
                    key = link['name']
                else:
                    key = val
                url = """<a href="%s">%s</a>""" % (quote(url), key)
                yield url
            elif 'link' in link:
                if  link['name']:
                    key = link['name']
                else:
                    key = val
                url = link['link'] % val
                url = """<a href="%s">%s</a>""" % (quote(url), key)
                yield url

def repr_val(val):
    "Represent given value in appropriate form"
    if  isinstance(val, dict) or isinstance(val, list):
        val = '<pre>%s</pre>' % json.dumps(val, indent=2)
    else:
        val = str(val)
    return val

def repr_values(row, flt):
    "Represent values of given row/filter in appropriate form"
    values = [r for r in DotDict(row).get_values(flt)]
    if  not len(values):
        val = ''
    elif len(values) == 1:
        val = repr_val(values[0])
    else:
        if  isinstance(values[0], dict) or isinstance(values[0], list):
            val = repr_val(values)
        else:
            val = ', '.join(set([str(v) for v in values]))
    if  flt.lower() == 'run.run_number':
        if  isinstance(val, basestring):
            val = int(val.split('.')[0])
    return val

def add_filter_values(row, filters):
    """Add filter values for a given row"""
    page = ''
    if filters:
        for flt in filters:
            if  flt.find('<') == -1 and flt.find('>') == -1:
                val = repr_values(row, flt)
                page += "<br />Filter <em>%s:</em> %s" % (flt, val)
    page += '<br/>'
    return page

def tooltip_helper(title):
    "Tooltip helper for block/dataset/file definitions"
    page  = ''
    if  title == 'Dataset presence':
        tooltip = title + ' is a total number of files\
 at the site divided by total number of files in a dataset'
    elif title == 'Block presence':
        tooltip = title + ' is a total number of blocks at the site\
 divided by total number of blocks in a dataset'
    elif title == 'File-replica presence':
        tooltip = title + ' is a total number of files at the site\
 divided by total number of files in all block at this site'
    elif title == 'Block completion':
        tooltip = title + \
        ' is a total number of blocks fully transferred to the site\
 divided by total number of blocks at this site'
    elif title == 'Config urls':
        tooltip = title + \
        ' represents either config file(s) used to produced this dataset\
 (input-config) or config file(s) used to produce other datasets using dataset\
 in question (output-config)'
    else:
        title = ''
    if  title:
        page = \
 """<span class="tooltip">%s<span class="classic">%s</span></span>""" \
        % (title, tooltip)
    return page

def adjust_values(func, gen, links, pkey):
    """
    Helper function to adjust values in UI.
    It groups values for identical key, make links for provided mapped function,
    represent "Number of" keys as integers and represents size values in GB format.
    The mapped function is the one from das_mapping_db which convert
    UI key into triplet of das key, das access key and link, see 
    das_mapping_db:daskey_from_presentation
    """
    rdict = {}
    uidict = {}
    for uikey, value, uilink, uidesc, uiexamples in [k for k, _g in groupby(gen)]:
        val = quote(value)
        if  uikey in rdict:
            existing_val = rdict[uikey]
            if  not isinstance(existing_val, list):
                existing_val = [existing_val]
            if  val not in existing_val:
                rdict[uikey] = existing_val + [val]
        else:
            rdict[uikey] = val
        uidict[uikey] = (uilink, uidesc, uiexamples)
    page = ""
    to_show = []
    green = 'style="color:green"'
    red = 'style="color:red"'
    for key, val in rdict.items():
        uilink, _uidesc, _uiexamples = uidict[key]
        if  uilink and val:
            if  not isinstance(val, list):
                val = [val]
            values = []
            for elem in val:
                for ilink in uilink:
                    dasquery = ilink['query'] % elem
                    val = '<a href="/das/request?input=%s">%s</a>' \
                            % (dasquery, elem)
                    values.append(val)
            to_show.append((key, ', '.join(values)))
            continue
        lookup = func(key)
        if  key.lower() == 'reason' or key.lower() == 'qhash' or key.lower() == 'hints':
            continue
        if  key.lower() == 'error':
            key = '<span %s>WARNING</span>' % red
            val = json.dumps(val) + ', click on show link to get more info<br/>'
        if  lookup:
            if  key.find('Member') != -1 and val:
                link = '/das/request?input=user%3D'
                if  isinstance(val, list):
                    val = ['<a href="%s%s">%s</a>' \
                    % (link, quote(v), quote(v)) for v in val]
                elif isinstance(val, basestring):
                    val = '<a href="%s%s">%s</a>' \
                        % (link, quote(val), quote(val))
            if  key.find('Config urls') != -1 and val:
                if  isinstance(val, dict):
                    urls = []
                    for rtype, rurls in val.items():
                        for vdx in range(len(rurls)):
                            urls.append('<a href="%s">%s-config-%d</a>' % (rurls[vdx], rtype, vdx+1))
                    value = ', '.join(urls)
                else:
                    value = '<a href="%s">config</a>' % val
                key = tooltip_helper(key)
            elif  isinstance(val, list):
                value = ', '.join([str(v) for v in val])
                try:
                    length = len(set(val))
                except TypeError: # happens when val list contains a dict
                    length = len(val)
                if  length > 1 and \
                    (key.lower().find('number') != -1 or \
                        key.lower().find('size') != -1):
                    if  key not in ['Luminosity number', 'Run number']:
                        value = '<span %s>%s</span>' % (red, value)
            elif  key.lower().find('size') != -1 and val:
                value = size_format(val)
            elif  key.find('Number of ') != -1 and val and int(val) != 0:
                value = int(val)
            elif  key.find('Run number') != -1 and val:
                value = int(val)
            elif  key.find('Lumi') != -1 and val:
                value = int(val)
            elif  key.find('Site type') != -1 and val:
                if  isinstance(val, basestring) and val.lower() == 'disk':
                    value = val
                else:
                    value = '<span %s><b>TAPE</b></span> <b>no user access</b>' % red
            elif  key.find('Tag') != -1 and val:
                if  isinstance(val, basestring) and val.lower() == 'unknown':
                    value = '<span %s>%s</span>' % (red, val)
                else:
                    value = val
            elif  key.find('Creation time') != -1 and val:
                try:
                    value = time.strftime('%d/%b/%Y %H:%M:%S GMT', \
                                time.gmtime(val))
                except:
                    value = val
            else:
                value = val
            if  isinstance(value, list) and isinstance(value[0], str):
                value = ', '.join(value)
            if  key == 'Open' or key == 'Custodial':
                if  value == 'n':
                    value = '<span %s>%s</span>' % (green, value)
                else:
                    value = '<span %s>%s</span>' % (red, value)
            if  key.lower().find('presence') != -1 or \
                key.lower().find('completion') != -1:
                if  not value:
                    continue
                else:
                    if  value == '100.00%':
                        value = '<span %s>100%%</span>' % green
                    else:
                        value = '<span %s>%s</span>' % (red, value)
                key = tooltip_helper(key)
            to_show.append((key, value))
        else:
            if  key == 'result' and isinstance(val, dict) and \
                'value' in val: # result of aggregation function
                if  'key' in rdict and rdict['key'].find('.size') != -1:
                    val = size_format(val['value'])
                elif isinstance(val['value'], float):
                    val = '%.2f' % val['value']
                else:
                    val = val['value']
            to_show.append((key, val))
    if  to_show:
        to_show = list(set(to_show))
        page += '<br />'
        tdict = {}
        for key, val in to_show:
            tdict[key] = val
        result_keys = ['function', 'result', 'key']
        if  set(tdict.keys()) & set(result_keys) == set(result_keys):
            page += '%s(%s)=%s' \
                % (tdict['function'], tdict['key'], tdict['result'])
        elif sorted(tdict.keys()) == sorted(['Luminosity number', 'Run number']):
            page += 'Run number: %s, Luminosity ranges: %s' \
                    % (tdict['Run number'], convert2ranges(rdict['Luminosity number']))
        elif sorted(tdict.keys()) == sorted(['Events', 'Luminosity number', 'Run number']):
            page += 'Run number: %s, Luminosity ranges: %s' \
                    % (tdict['Run number'], convert2ranges(rdict['Luminosity number']))
            page += lumi_evts(rdict)
        elif sorted(tdict.keys()) == sorted(['Luminosity number']):
            page += 'Luminosity ranges: %s' \
                    % (convert2ranges(rdict['Luminosity number']))
        elif sorted(tdict.keys()) == sorted(['Events', 'Luminosity number']):
            page += 'Luminosity ranges: %s' \
                    % (convert2ranges(rdict['Luminosity number']))
            page += lumi_evts(rdict)
        else:
            rlist = ["%s: %s" \
                % (k[0].capitalize()+k[1:], v) for k, v in to_show]
            rlist.sort()
            page += ', '.join(rlist)
            page  = page.replace('<br/>,', '<br/>')
    if  links:
        page += '<br />' + ', '.join(links)
    return page

def lumi_evts(rdict):
    "Helper function to show lumi-events pairs suitable for web UI"
    run = rdict['Run number']
    lumis = rdict['Luminosity number']
    events = rdict['Events']
    pdict = dict(zip(lumis, events))
    pkeys = [str(k) for k in pdict.keys()]
    tag = 'id_%s_%s' % (run, ''.join(pkeys))
    link = 'link_%s_%s' % (run, ''.join(pkeys))
    hout = '<div class="hide" id="%s" name="%s">' % (tag, tag)
    tot_evts = 0
    for idx, lumi in enumerate(sorted(pdict.keys())):
        evts = pdict[lumi]
        if  evts != 'NA' and evts and int_number_pattern.match(str(evts)):
            tot_evts += int(evts)
        hout += 'Lumi: %s, Events %s<br/>' % (lumi, evts)
    hout += "</div>"
    out = """&nbsp;<em>lumis/events pairs</em>\
    <a href="javascript:ToggleTag('%s', '%s')" id="%s">show</a>""" \
            % (tag, link, link)
    if  tot_evts:
        out += '&nbsp; Total events=%s' % tot_evts
    out += hout
    return out

class CMSRepresentation(DASRepresentation):
    """
    CMSRepresentation is an abstract class which represents DAS records
    """
    def __init__(self, config, dasmgr):
        DASRepresentation.__init__(self, config)
        self.dasmgr     = dasmgr
        self.dasmapping = self.dasmgr.mapping
        if  'dbs' in config:
            self.dbs_global = self.dasmapping.dbs_global_instance()
        else:
            self.dbs_global = None
        self.colors = {'das':gen_color('das')}
        for system in self.dasmgr.systems:
            self.colors[system] = gen_color(system)

    def sort_dict(self, titles, pkey):
        """Return dict of daskey/mapkey for given list of titles"""
        tdict = {}
        for uikey in titles:
            pdict = self.dasmapping.daskey_from_presentation(uikey)
            if  pdict and pkey in pdict:
                mapkey = pdict[pkey]['mapkey']
            else:
                mapkey = uikey
            tdict[uikey] = mapkey
        return tdict

    def get_few_rows(self, dasquery, nrows=5):
        """
        Invoke DAS workflow and get few rows from the cache.
        """
        for row in self.dasmgr.get_from_cache(dasquery, idx=0, limit=nrows):
            yield row

    def fltpage(self, dasquery):
        """Prepare filter snippet for a given query"""
        page = ''
        fields = []
        for row in self.get_few_rows(dasquery):
            fields += self.get_result_fieldlist(row)
        fields = list(set(fields))
        if fields:
            #fields += ['das.conflict']
            dflt = das_filters() + das_aggregators()
            dflt.remove('unique')
            page = self.templatepage('das_filters',
                                     filters=dflt, das_keys=fields)
        return page

    def get_result_fieldlist(self, row):
        rowkeys = []
        if  row and 'das' in row  and 'primary_key' in row['das']:
            pkey = row['das']['primary_key']
            if  pkey and (isinstance(pkey, str) or isinstance(pkey, unicode)):
                try:
                    mkey = pkey.split('.')[0]
                    if  mkey not in row:
                        return []
                    if  isinstance(row[mkey], list):
                        # take first five or less entries from the list to cover
                        # possible aggregated records and extract row keys
                        ndict   = DotDict({mkey: row[mkey][:10]})
                        rowkeys = list(ndict.get_keys(mkey))
                    else:
                        rowkeys = list(DotDict(row).get_keys(mkey))
                    rowkeys.sort()
                    rowkeys += ['das.conflict']
                except Exception as exc:
                    # TODO: pkey.split fail only if called on non-string
                    msg = "Fail to pkey.split('.') for pkey=%s" % pkey
                    print(msg)
                    print_exc(exc)
        return rowkeys

    def convert2ui(self, idict, not2show=None):
        """
        Convert input row (dict) into UI presentation
        """
        for key in idict.keys():
            if  key == 'das' or key.find('_id') != -1:
                continue
            val = idict[key]
            if  isinstance(val, list):
                for elem in val:
                    if  'error' in elem:
                        yield 'error', elem['error'], None, None, None
            else:
                if  'error' in val:
                    yield 'error', val['error'], None, None, None
            for item in self.dasmapping.presentation(key):
                try:
                    daskey = item['das']
                    link = item.get('link', None)
                    desc = item.get('description', None)
                    exam = item.get('examples', None)
                    if  not2show and not2show == daskey:
                        continue
                    uikey  = item['ui']
                    for value in access(idict, daskey):
                        if  value:
                            yield uikey, value, link, desc, exam
                except:
                    link = None
                    desc = None
                    exam = None
                    yield key, idict[key], link, desc, exam

    def systems(self, slist):
        """Colorize provided sub-systems"""
        page = ""
        if  not self.colors:
            return page
        pads = "padding:2px"
        # at web UI we don't need to list all systems, we'll only
        # take their unique set
        for system in list(set(slist)):
            bkg, col = self.colors[system]
            page += '<span style="background-color:%s;color:%s;%s">%s</span>' \
                % (bkg, col, pads, system)
        return page

    def filter_bar(self, dasquery):
        "Construct filter bar UI element and returned for given input"
        if  dasquery.filters:
            # if we have filter/aggregator get one row from the given query
            try:
                if  dasquery.mongo_query:
                    fltpage = self.fltpage(dasquery)
            except Exception as exc:
                fltpage = 'N/A, please check DAS record for errors'
                msg = 'Fail to apply filter to query=%s' % dasquery.query
                print(msg)
                print_exc(exc)
        else:
            fltpage = ''
        return fltpage

    def processing_time(self, dasquery):
        "Return processing time of DAS query"
        return self.dasmgr.processing_time(dasquery)

    def listview(self, head, data):
        """
        Represent data in list view.
        """
        kwargs   = head.get('args')
        uinput   = kwargs.get('input', '')
        total    = head.get('nresults', 0)
        apilist  = head.get('apilist')
        dasquery = head.get('dasquery', None)
        if  not dasquery:
            inst     = head.get('instance', self.dbs_global)
            dasquery = DASQuery(uinput, instance=inst)
        inst     = dasquery.instance
        filters  = dasquery.filters
        aggrtrs  = dasquery.aggregators
        pager    = self.pagination(head)
        main     = pager
        style    = 'white'
        rowkeys  = []
        fltpage  = self.filter_bar(dasquery)
        page     = ''
        old      = None
        dup      = False
        status   = head.get('status', None)
        if  status == 'fail':
            reason = head.get('reason', '')
            if  reason:
                page += '<br/><span class="box_red">%s</span>' % reason
        for row in data:
            if  not row:
                continue
            if  not dup and old and identical_data_records(old, row):
                dup = True
            error = row.get('error', None)
            try:
                mongo_id = row['_id']
            except Exception as exc:
                msg  = 'Exception: %s\n' % str(exc)
                msg += 'Fail to process row\n%s' % str(row)
                raise Exception(msg)
            page += '<div class="%s"><hr class="line" />' % style
            links = []
            pkey  = None
            pval  = None
            lkey  = None
            if  'das' in row and 'primary_key' in row['das']:
                pkey = row['das']['primary_key']
                if  pkey and not rowkeys and not fltpage:
                    fltpage = self.fltpage(dasquery)
                try:
                    lkey = pkey.split('.')[0]
                    if  pkey == 'summary':
                        pval = row[pkey]
                    else:
                        pval = [i for i in DotDict(row).get_values(pkey)]
                        if  isinstance(pval, list):
                            if  pval and not isinstance(pval[0], list):
                                pval = list(set(pval))
                        else:
                            pval = list(set(pval))
                        if  len(pval) == 1:
                            pval = pval[0]
                        if  pkey == 'run.run_number' or pkey == 'lumi.number':
                            if  isinstance(pval, basestring):
                                pval = int(pval)
                except Exception as exc:
                    msg  = "Fail to extract pval for pkey='%s', lkey='%s'" \
                            % (pkey, lkey)
                    msg += "\npval='%s', type(pval)='%s'" % (pval, type(pval))
                    print(msg)
                    print_exc(exc)
                    pval = 'N/A'
                try:
                    if  not filters:
                        if  pkey == 'summary':
                            page += 'Summary information:'
                        elif  pval and pval != 'N/A':
                            page += '%s: ' % lkey.capitalize()
                            if  lkey == 'parent' or lkey == 'child':
                                if  str(pval).find('.root') != -1:
                                    lkey = 'file'
                                else:
                                    lkey = 'dataset'
                            if  lkey in not_to_link():
                                page += '%s' % pval
                            elif  isinstance(pval, list):
                                page += ', '.join(['<span class="highlight>"'+\
                                    '<a href="/das/request?%s">%s</a></span>'\
                                    % (make_args(lkey, i, inst), i) for i in pval])
                            else:
                                args  = make_args(lkey, pval, inst)
                                page += '<span class="highlight">'+\
                                    '<a href="/das/request?%s">%s</a></span>'\
                                    % (args, pval)
                        else:
                            page += '%s: N/A' % lkey.capitalize()
                    plist = self.dasmgr.mapping.presentation(lkey)
                    linkrec = None
                    for item in plist:
                        if  'link' in item:
                            linkrec = item['link']
                            break
                    if  linkrec and pval and pval != 'N/A' and \
                        not isinstance(pval, list) and not error:
                        links += [l for l in make_links(linkrec, pval, inst)]
                    if  pkey and pkey == 'file.name':
                        try:
                            lfn = DotDict(row).get('file.name')
                            val = '<a href="/das/download?lfn=%s">Download</a>'\
                                    % lfn if lfn else ''
                            if  val: links.append(val)
                        except:
                            pass
                    if  pkey and pkey == 'site.name':
                        try:
                            site = DotDict(row).get('site.name')
                            val = self.templatepage(\
                            'sitedb', item=site, api="sites") if site else ''
                            if  val: links.append(val)
                        except:
                            pass
                    if  pkey and pkey == 'user.name':
                        try:
                            user = DotDict(row).get('user.username')
                            val = self.templatepage(\
                            'sitedb', item=user, api="people") if user else ''
                            if  val: links.append(val)
                        except:
                            pass
                    if  pkey and pkey == 'dataset.name':
                        try:
                            path = DotDict(row).get('dataset.name')
                            if  path:
                                links.append(self.templatepage(\
                                    'makepy', path=path, inst=inst))
                                if  inst == self.dbs_global:
                                    links.append(self.templatepage(\
                                        'phedex_subscription', path=path))
                                    links.append(self.templatepage(\
                                        'xsecdb', primds=path.split('/')[1]))
                        except:
                            pass
                    if  pkey and pkey == 'release.name':
                        rel  = '["%s"]' % DotDict(row).get('release.name')
                        url  = 'https://cmstags.cern.ch/tc/py_getReleasesTags?'
                        url += 'diff=false&releases=%s' % urllib.quote(rel)
                        links.append('<a href="%s">Packages</a>' % url)
                except Exception as exc:
                    print_exc(exc)
                    pval = 'N/A'
            gen   = self.convert2ui(row, pkey)
            if  self.dasmgr:
                func  = self.dasmgr.mapping.daskey_from_presentation
                if  filters and not aggrtrs:
                    page += add_filter_values(row, filters)
                else:
                    page += adjust_values(func, gen, links, pkey)
            pad   = ""
            try:
                if  'das' in row and 'system' in row['das']:
                    systems = self.systems(row['das']['system'])
                else:
                    systems = "" # no das record
                    print(dastimestamp('DAS ERROR '), \
                            'record without DAS key', row)
            except KeyError as exc:
                print_exc(exc)
                systems = "" # we don't store systems for aggregated records
            except Exception as exc:
                print_exc(exc)
                systems = "" # we don't store systems for aggregated records
            jsonhtml = das_json(dasquery, row, pad)
            jsonhtml = jsonhtml.replace(\
                'request?', 'request?instance=%s&' % inst)
            if  not links:
                page += '<br />'
            if  'das' in row and 'conflict' in row['das']:
                conflict = ', '.join(row['das']['conflict'])
            else:
                conflict = ''
            hints = ''
            for hint in row.get('hints', {}):
                if  hint:
                    hints += self.templatepage('hint',
                            hint=hint, base=self.base, dbs=self.dbs_global)
            page += self.templatepage('das_row', systems=systems, \
                    sanitized_data=jsonhtml, id=mongo_id, rec_id=mongo_id,
                    conflict=conflict, hints=hints)
            page += '</div>'
            old = row
        main += fltpage
        if  dup and not dasquery.aggregators:
            main += self.templatepage('das_duplicates', uinput=uinput,
                        instance=inst)
        main += page
        if total>10:
            main += '<hr class="line" />'
            main += pager
            main += '<hr class="line" />'
        proc_time = self.processing_time(dasquery)
        if  proc_time:
            msg = 'processing time: %5.3f sec, ' % proc_time
        else:
            msg   = ''
        msg  += 'cache server time: %5.3f sec' % head['ctime']
        main += '<div align="right">%s</div>' % msg
        return main

    def tableview(self, head, data):
        """
        Represent data in tabular view.
        """
        kwargs   = head.get('args')
        total    = head.get('nresults', 0)
        apilist  = head.get('apilist')
        dasquery = head.get('dasquery')
        filters  = dasquery.filters
        sdir     = getarg(kwargs, 'dir', '')
        titles   = []
        page     = self.pagination(head)
        fltbar   = self.filter_bar(dasquery)
        if  filters:
            for flt in filters:
                if  flt.find('=') != -1 or flt.find('>') != -1 or \
                    flt.find('<') != -1:
                    continue
                titles.append(flt)
        style   = 1
        tpage   = ""
        pkey    = None
        status  = head.get('status', None)
        if  status == 'fail':
            reason = head.get('reason', '')
            if  reason:
                page += '<br/><span class="box_red">%s</span>' % reason
        for row in data:
            if  not fltbar:
                fltbar = self.fltpage(dasquery)
            try: # we don't need to show qhash in table view
                del row['qhash']
            except:
                pass
            rec  = []
            if  not pkey and 'das' in row and 'primary_key' in row['das']:
                pkey = row['das']['primary_key'].split('.')[0]
            if  filters:
                for flt in filters:
                    rec.append(DotDict(row).get(flt))
            else:
                gen = self.convert2ui(row)
                titles = []
                for uikey, val, _link, _desc, _examples in gen:
                    skip = 0
                    if  not filters:
                        if  uikey in titles:
                            skip = 1
                        else:
                            titles.append(uikey)
                    if  not skip:
                        rec.append(val)
            if  style:
                style = 0
            else:
                style = 1
            link = '<a href="/das/records/%s?collection=merge">link</a>' \
                        % quote(str(row['_id'])) # cgi.escape the id
            tpage += self.templatepage('das_table_row', rec=rec, tag='td', \
                        style=style, encode=1, record=link)
        if  sdir == 'asc':
            sdir = 'desc'
        elif sdir == 'desc':
            sdir = 'asc'
        else: # default sort direction
            sdir = 'asc' 
        theads = []
        for title in titles:
            theads.append(title)
        theads.append('Record')
        thead = self.templatepage('das_table_row', rec=theads, tag='th', \
                        style=0, encode=0, record=0)
        page += fltbar
        page += '<br />'
        page += '<table class="das_table">' + thead + tpage + '</table>'
        page += '<br />'
        page += '<div align="right">DAS cache server time: %5.3f sec</div>' \
                % head['ctime']
        return page

    @exposetext
    def plainview(self, head, data):
        """
        Represent data in DAS plain view for queries with filters.
        """
        dasquery = head['dasquery']
        fields   = dasquery.mongo_query.get('fields', [])
        filters  = [f for f in dasquery.filters if f != 'unique' and \
                        f.find('=') == -1 and f.find('<') == -1 and \
                        f.find('>') == -1]
        results  = ""
        status   = head.get('status', None)
        if  status == 'fail':
            reason = head.get('reason', '')
            if  reason:
                results += 'ERROR: %s' % reason
        lookup_items = [i for i in fields if i not in das_record_keys()]
        for row in data:
            if  filters:
                for flt in filters:
                    try:
                        for obj in DotDict(row).get_values(flt):
                            results += str(obj) + ' '
                    except:
                        pass
                results += '\n'
            else:
                for item in lookup_items:
                    if  item != lookup_items[0]:
                        results += ', '
                    try:
                        systems = row['das']['system']
                        mapkey  = self.dasmapping.find_mapkey(systems[0], item)
                        if  not mapkey:
                            mapkey = '%s.name' % item
                        key, att = mapkey.split('.')
                        if  key in row:
                            val = row[key]
                            if  isinstance(val, dict):
                                results += val.get(att, '')
                            elif isinstance(val, list):
                                results += \
                                ' '.join(set([str(i.get(att, '')) for i in val]))
                    except:
                        pass
                results += '\n'
        # use DAS sort_rows function instead of python set, since we need to
        # preserve the order of records in final output
        rows = [r for r in results.split('\n') if r]
        results = '\n'.join([r for r in sort_rows(rows)])
        return results
