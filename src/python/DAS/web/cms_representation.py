#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=W0201,W0703,W0702,R0914,R0912,R0904,R0915,W0107
"""
File: cms_representation.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: CMS DAS records representation
"""

# system modules
import time
import urllib
from itertools import groupby

# DAS modules
from DAS.core.das_ql import das_aggregators, das_filters
from DAS.core.das_query import DASQuery
from DAS.utils.ddict import DotDict
from DAS.utils.utils import print_exc, getarg, size_format, access
from DAS.utils.utils import identical_data_records, deepcopy
from DAS.web.utils import das_json, quote, gen_color
from DAS.web.utils import not_to_link
from DAS.web.tools import exposetext
from DAS.web.das_representation import DASRepresentation

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
            if  link.has_key('query'):
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
            elif link.has_key('link'):
                if  link['name']:
                    key = link['name']
                else:
                    key = val
                url = link['link'] % val
                url = """<a href="%s">%s</a>""" % (quote(url), key)
                yield url

def add_filter_values(row, filters):
    """Add filter values for a given row"""
    page = ''
    if filters:
        for flt in filters:
            if  flt.find('<') == -1 and flt.find('>') == -1:
                values = set([str(r) for r in DotDict(row).get_values(flt)])
                val = ', '.join(values)
                if  val:
                    if  flt.lower() == 'run.run_number':
                        if  isinstance(val, str) or isinstance(val, unicode):
                            val = int(val.split('.')[0])
                    page += "<br />Filter <em>%s:</em> %s" % (flt, val)
                else:
                    page += "<br />Filter <em>%s</em>" % flt
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
        tooltip = title + 'is a total number of files at the site\
 divided by total number of files in all block at this site'
    elif title == 'Block completion':
        tooltip = title + \
        'is a total number of blocks fully transferred to the site\
 divided by total number of blocks at this site'
    else:
        title = ''
    if  title:
        page = \
 """<span class="tooltip">%s<span class="classic">%s</span></span>""" \
        % (title, tooltip)
    return page

def adjust_values(func, gen, links):
    """
    Helper function to adjust values in UI.
    It groups values for identical key, make links for provided mapped function,
    represent "Number of" keys as integers and represents size values in GB format.
    The mapped function is the one from das_mapping_db which convert
    UI key into triplet of das key, das access key and link, see 
    das_mapping_db:daskey_from_presentation
    """
    rdict = {}
    for uikey, value in [k for k, _g in groupby(gen)]:
        val = quote(value)
        if  rdict.has_key(uikey):
            existing_val = rdict[uikey]
            if  not isinstance(existing_val, list):
                existing_val = [existing_val]
            if  val not in existing_val:
                rdict[uikey] = existing_val + [val]
        else:
            rdict[uikey] = val
    page = ""
    to_show = []
    error = 0
    green = 'style="color:green"'
    red = 'style="color:red"'
    for key, val in rdict.iteritems():
        lookup = func(key)
        if  key.lower() == 'reason' or key.lower() == 'qhash':
            continue
        if  key.lower() == 'error':
            key = '<span %s>Error</span>' % red
            error = 1
        if  lookup:
            if  isinstance(val, list):
                value = ', '.join([str(v) for v in val])
                if  len(set(val)) > 1 and \
                    (key.lower().find('number') != -1 or \
                        key.lower().find('size') != -1):
                    value = '<span %s>%s</span>' % (red, value)
            elif  key.lower().find('size') != -1 and val:
                value = size_format(val)
            elif  key.find('Number of ') != -1 and val:
                value = int(val)
            elif  key.find('Run number') != -1 and val:
                value = int(val)
            elif  key.find('Lumi') != -1 and val:
                value = int(val)
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
            if  key == 'Open':
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
                val.has_key('value'): # result of aggregation function
                if  rdict.has_key('key') and \
                    rdict['key'].find('.size') != -1:
                    val = size_format(val['value'])
                elif isinstance(val['value'], float):
                    val = '%.2f' % val['value']
                else:
                    val = val['value']
            to_show.append((key, val))
    if  to_show:
        page += '<br />'
        tdict = {}
        for key, val in to_show:
            tdict[key] = val
        if  set(tdict.keys()) == set(['function', 'result', 'key']):
            page += '%s(%s)=%s' \
                % (tdict['function'], tdict['key'], tdict['result'])
        else:
            rlist = ["%s: %s" \
                % (k[0].capitalize()+k[1:], v) for k, v in to_show]
            rlist.sort()
            page += ', '.join(rlist)
    if  links and not error:
        page += '<br />' + links
    return page

class CMSRepresentation(DASRepresentation):
    """
    CMSRepresentation is an abstract class which represents DAS records
    """
    def __init__(self, config, dasmgr):
        DASRepresentation.__init__(self, config)
        self.dasmgr     = dasmgr
        self.dasmapping = self.dasmgr.mapping
        if  config.has_key('dbs'):
            self.dbs_global = config['dbs'].get('dbs_global_instance', None)
        else:
            self.dbs_global = None
        self.colors     = {}
        for system in self.dasmgr.systems:
            self.colors[system] = gen_color(system)

    def sort_dict(self, titles, pkey):
        """Return dict of daskey/mapkey for given list of titles"""
        tdict = {}
        for uikey in titles:
            pdict = self.dasmapping.daskey_from_presentation(uikey)
            if  pdict and pdict.has_key(pkey):
                mapkey = pdict[pkey]['mapkey']
            else:
                mapkey = uikey
            tdict[uikey] = mapkey
        return tdict

    def get_one_row(self, dasquery):
        """
        Invoke DAS workflow and get one row from the cache.
        """
        mongo_query = deepcopy(dasquery.to_bare_query())
        data = [r for r in self.dasmgr.get_from_cache(\
                DASQuery(mongo_query), idx=0, limit=1)]
        if  len(data):
            return data[0]

    def fltpage(self, row):
        """Prepare filter snippet for a given query"""
        rowkeys = []
        page = ''
        if  row and row.has_key('das') and row['das'].has_key('primary_key'):
            pkey = row['das']['primary_key']
            if  pkey and (isinstance(pkey, str) or isinstance(pkey, unicode)):
                try:
                    mkey = pkey.split('.')[0]
                    if  isinstance(row[mkey], list):
                        # take first five or less entries from the list to cover
                        # possible aggregated records and extract row keys
                        lmax    = len(row[mkey]) if len(row[mkey]) < 5 else 5
                        sublist = [row[mkey][i] for i in range(0, lmax)]
                        ndict   = DotDict({mkey:sublist})
                        rowkeys = [k for k in ndict.get_keys(mkey)]
                    else:
                        rowkeys = [k for k in DotDict(row).get_keys(mkey)]
                    rowkeys.sort()
                    rowkeys += ['das.conflict']
                    dflt = das_filters() + das_aggregators()
                    dflt.remove('unique')
                    page = self.templatepage('das_filters', \
                            filters=dflt, das_keys=rowkeys)
                except Exception as exc:
                    msg = "Fail to pkey.split('.') for pkey=%s" % pkey
                    print msg
                    print_exc(exc)
                    pass
        return page

    def convert2ui(self, idict, not2show=None):
        """
        Convert input row (dict) into UI presentation
        """
        for key in idict.keys():
            if  key == 'das' or key.find('_id') != -1:
                continue
            for item in self.dasmapping.presentation(key):
                try:
                    daskey = item['das']
                    if  not2show and not2show == daskey:
                        continue
                    uikey  = item['ui']
                    for value in access(idict, daskey):
                        if  value:
                            yield uikey, value
                except:
                    yield key, idict[key]

    def systems(self, slist):
        """Colorize provided sub-systems"""
        page = ""
        if  not self.colors:
            return page
        pads = "padding-left:7px; padding-right:7px"
        for system in slist:
            page += '<span style="background-color:%s;%s">&nbsp;</span>' \
                % (self.colors[system], pads)
        return page

    def listview(self, head, data):
        """
        Represent data in list view.
        """
        kwargs   = head['args']
        uinput   = kwargs.get('input', '')
        total    = head.get('nresults', 0)
        dasquery = head['dasquery']
        inst     = dasquery.instance
        filters  = dasquery.filters
        main     = self.pagination(total, kwargs)
        if  main.find('das_noresults') == -1:
            main += self.templatepage('das_colors', colors=self.colors)
        style   = 'white'
        rowkeys = []
        if  uinput.find('|') != -1:
            # if we have filter/aggregator get one row from the given query
            try:
                if  dasquery.mongo_query:
                    fltpage = self.fltpage(self.get_one_row(dasquery))
                else:
                    fltpage = uinput.split('|')[-1]
            except Exception as exc:
                fltpage = 'N/A, please check DAS record for errors'
                msg = 'Fail to apply filter to query=%s' % dasquery.query
                print msg
                print_exc(exc)
        else:
            fltpage = ''
        page = ''
        old  = None
        dup  = False
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
            links = ""
            pkey  = None
            lkey  = None
            if  row.has_key('das') and row['das'].has_key('primary_key'):
                pkey = row['das']['primary_key']
                if  pkey and not rowkeys and not fltpage:
                    fltpage = self.fltpage(row)
                try:
                    lkey = pkey.split('.')[0]
                    pval = list(set(DotDict(row).get_values(pkey)))
                    if  len(pval) == 1:
                        pval = pval[0]
                    if  pkey == 'run.run_number' or pkey == 'lumi.number':
                        pval = int(pval)
                    if  pval:
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
                            page += '<span class="highlight">'+\
                                '<a href="/das/request?%s">%s</a></span>'\
                                % (make_args(lkey, pval, inst), pval)
                    else:
                        page += '%s: N/A' % lkey.capitalize()
                    plist = self.dasmgr.mapping.presentation(lkey)
                    linkrec = None
                    for item in plist:
                        if  item.has_key('link'):
                            linkrec = item['link']
                            break
                    if  linkrec and pval and pval != 'N/A' and \
                        not isinstance(pval, list) and not error:
                        links = ', '.join(make_links(linkrec, pval, inst))
                    if  pkey and pkey == 'file.name':
                        try:
                            lfn = DotDict(row).get('file.name')
                            if  lfn:
                                links += ', ' + \
                                        self.templatepage('filemover', lfn=lfn)
                        except:
                            pass
                    if  pkey and pkey == 'dataset.name':
                        try:
                            path = DotDict(row).get('dataset.name')
                            if  path:
                                links += ', ' + self.templatepage(\
                                    'makepy', path=path, inst=inst)
                                if  inst.find('global') != -1:
                                    links += ', ' + self.templatepage(\
                                        'phedex_subscription', path=path)
                        except:
                            pass
                    if  pkey and pkey == 'release.name':
                        rel  = '["%s"]' % DotDict(row).get('release.name')
                        url  = 'https://cmstags.cern.ch/tc/py_getReleasesTags?'
                        url += 'diff=false&releases=%s' % urllib.quote(rel)
                        links += ', <a href="%s">Packages</a>' % url
                except:
                    pval = 'N/A'
            gen   = self.convert2ui(row, pkey)
            if  self.dasmgr:
                func  = self.dasmgr.mapping.daskey_from_presentation
                page += add_filter_values(row, filters)
                page += adjust_values(func, gen, links)
            pad   = ""
            try:
                systems = self.systems(row['das']['system'])
                if  row['das']['system'] == ['combined'] or \
                    row['das']['system'] == [u'combined']:
                    if  lkey:
                        rowsystems = DotDict(row).get('%s.combined' % lkey) 
                        try:
                            systems = self.systems(rowsystems)
                        except TypeError as _err:
                            systems = self.systems(['combined'])
            except KeyError:
                systems = "" # we don't store systems for aggregated records
            except Exception as exc:
                print_exc(exc)
                systems = "" # we don't store systems for aggregated records
            jsonhtml = das_json(row, pad)
            jsonhtml = jsonhtml.replace(\
                'request?', 'request?instance=%s&' % inst)
            if  not links:
                page += '<br />'
            if  row.has_key('das') and row['das'].has_key('conflict'):
                conflict = ', '.join(row['das']['conflict'])
            else:
                conflict = ''
            page += self.templatepage('das_row', systems=systems, \
                    sanitized_data=jsonhtml, id=mongo_id, rec_id=mongo_id,
                    conflict=conflict)
            page += '</div>'
            old = row
        main += fltpage
        if  dup:
            main += self.templatepage('das_duplicates', uinput=uinput)
        main += page
        main += '<div align="right">DAS cache server time: %5.3f sec</div>' \
                % head['ctime']
        return main

    def tableview(self, head, data):
        """
        Represent data in tabular view.
        """
        kwargs   = head['args']
        total    = head['nresults']
        dasquery = head['dasquery']
        uinput   = kwargs.get('input', dasquery.query)
        inst     = dasquery.instance
        filters  = dasquery.filters
        idx      = getarg(kwargs, 'idx', 0)
        limit    = getarg(kwargs, 'limit', 10)
        sdir     = getarg(kwargs, 'dir', '')
        titles   = []
        page     = self.pagination(total, kwargs)
        if  filters:
            for flt in filters:
                if  flt.find('=') != -1 or flt.find('>') != -1 or \
                    flt.find('<') != -1:
                    continue
                titles.append(flt)
        style   = 1
        tpage   = ""
        pkey    = None
        for row in data:
            try: # we don't need to show qhash in table view
                del row['qhash']
            except:
                pass
            rec  = []
            if  not pkey and row.has_key('das') and \
                row['das'].has_key('primary_key'):
                pkey = row['das']['primary_key'].split('.')[0]
            if  filters:
                for flt in filters:
                    rec.append(DotDict(row).get(flt))
            else:
                gen = self.convert2ui(row)
                titles = []
                for uikey, val in gen:
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
        sdict  = self.sort_dict(titles, pkey)
        if  sdir == 'asc':
            sdir = 'desc'
        elif sdir == 'desc':
            sdir = 'asc'
        else: # default sort direction
            sdir = 'asc' 
        args   = {'input':uinput, 'idx':idx, 'limit':limit, 'instance':inst, \
                         'view':'table', 'dir': sdir}
        theads = []
        for title in titles:
            args.update({'skey':sdict[title]})
            url = '<a href="/das/request?%s">%s</a>' \
                % (urllib.urlencode(args), title)
            theads.append(url)
        theads.append('Record')
        thead = self.templatepage('das_table_row', rec=theads, tag='th', \
                        style=0, encode=0, record=0)
        self.sort_dict(titles, pkey)
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
        for row in data:
            if  filters:
                for flt in filters:
                    try:
                        for obj in DotDict(row).get_values(flt):
                            results += str(obj) + '\n'
                    except:
                        pass
            else:
                for item in fields:
                    systems = self.dasmgr.systems
                    mapkey  = self.dasmapping.find_mapkey(systems[0], item)
                    try:
                        if  not mapkey:
                            mapkey = '%s.name' % item
                        key, att = mapkey.split('.')
                        if  row.has_key(key):
                            val = row[key]
                            if  isinstance(val, dict):
                                results += val.get(att, '') + '\n'
                            elif isinstance(val, list):
                                results += \
                                '\n'.join([i.get(att, '') for i in val]) + '\n'
                    except:
                        pass
        results = '\n'.join(set([r for r in results.split('\n') if r]))
        return results
