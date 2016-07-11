#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0301

"""
DAS autocomplete function for web UI
"""

__author__ = "Gordon Ball and Valentin Kuznetsov"

import re
from DAS.utils.regex import RE_DBSQL_0, RE_DBSQL_1, RE_DBSQL_2
from DAS.utils.regex import RE_SITE, RE_SUBKEY, RE_KEYS
from DAS.utils.regex import RE_HASPIPE, RE_K_SITE, RE_K_FILE
from DAS.utils.regex import RE_K_PR_DATASET, RE_K_PARENT, RE_K_CHILD
from DAS.utils.regex import RE_K_CONFIG, RE_K_GROUP, RE_K_DATASET
from DAS.utils.regex import RE_K_BLOCK, RE_K_RUN, RE_K_RELEASE, RE_K_STATUS
from DAS.utils.regex import RE_K_TIER, RE_K_MONITOR, RE_K_JOBSUMMARY
from DAS.utils.regex import PAT_RELEASE, PAT_TIERS, PAT_SITE, PAT_SE
from DAS.utils.regex import PAT_BLOCK, PAT_RUN, PAT_FILE, PAT_DATATYPE
from DAS.utils.regex import PAT_SLASH, word_chars
from DAS.core.das_ql import das_aggregators, das_filters

AGG_PAT = re.compile(''.join([word_chars(a) for a in das_aggregators()]))
FLT_PAT = re.compile(''.join([word_chars(a) for a in das_filters()]))

def autocomplete_helper(query, dasmgr, daskeys):
    """
    Interface to the DAS keylearning system, for a
    as-you-type suggestion system. This is a call for AJAX
    in the page rather than a user-visible one.

    This returns a list of JS objects, formatted like::

    {'css': '<ul> css class', 'value': 'autocompleted text', 'info': '<html> text'}

    Some of the work done here could be moved client side, and
    only calls that actually require keylearning look-ups
    forwarded. Given the number of REs used, this may be necessary
    if load increases.
    """
    uinput = str(query)
    qsplit = uinput.split()
    last_word = qsplit[-1] # operate on last word in a query
    if  last_word.find(',') != -1:
        last_word = last_word.strip().replace(',', '').split()[-1]
    result = []
    prev = ""
    if  len(qsplit) != 1:
        prev = ' '.join(qsplit[:-1])
    query = last_word
    if RE_DBSQL_0.match(query):
        #find...
        match1 = RE_DBSQL_1.match(query)
        match2 = RE_DBSQL_2.match(query)
        if match1:
            daskey = match1.group(1)
            if daskey in daskeys:
                if match2:
                    operator = match2.group(3)
                    value = match2.group(4)
                    if operator == '=' or operator == 'like':
                        result.append({'css': 'ac-warning sign', 'value':'%s=%s' % (daskey, value),
                                       'info': "This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key, and the condition should <b>probably</b> be expressed like this." % (daskey)})
                    else:
                        result.append({'css': 'ac-warning sign', 'value':daskey,
                                       'info': "This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key. However, I'm not sure how to interpret the condition (<b>%s %s<b>)." % (daskey, operator, value)})
                else:
                    result.append({'css': 'ac-warning sign', 'value': daskey,
                                   'info': 'This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key.' % daskey})
            else:
                result.append({'css': 'ac-error sign', 'value': '',
                               'info': "This appears to be a DBS-QL query, and the key (<b>%s</b>) isn't known to DAS." % daskey})

                key_search = dasmgr.keylearning.key_search(daskey)
                #do a key search, and add info elements for them here
                for keys, members in key_search.items():
                    result.append({'css': 'ac-info', 'value': ' '.join(keys),
                                   'info': 'Possible keys <b>%s</b> (matching %s).' % (', '.join(keys), ', '.join(members))})
                if not key_search:
                    result.append({'css': 'ac-error sign', 'value': '',
                                   'info': 'No matches found for <b>%s</b>.' % daskey})


        else:
            result.append({'css': 'ac-error sign', 'value': '',
                           'info': 'This appears to be a DBS-QL query. DAS queries are of the form <b>key</b><span class="faint">[ operator value]</span>'})
    elif RE_HASPIPE.match(uinput) and RE_SUBKEY.match(query):
        subkey = RE_SUBKEY.match(query).group(1)
        daskey = subkey.split('.')[0]
        if  daskey in daskeys and dasmgr.keylearning.col and\
            dasmgr.keylearning.col.count():
            if  dasmgr.keylearning.has_member(subkey):
                result.append({'css': 'ac-info', 'value': subkey,
                               'info': 'Correct DAS query'})
            else:
                result.append({'css': 'ac-warning sign', 'value': subkey,
                               'info': "Correct DAS query, but <b>%s</b> is not known in DAS keylearning system" % subkey})
                key_search = dasmgr.keylearning.key_search(subkey, daskey)
                for keys, members in key_search.items():
                    for member in members:
                        result.append({'css': 'ac-info', 'value':'%s' % member,
                                       'info': 'Possible member match <b>%s</b> (for daskey <b>%s</b>)' % (member, daskey)})
    elif RE_HASPIPE.match(uinput):
        keystr = uinput.split('|')[0]
        keys = set()
        for keymatch in RE_KEYS.findall(keystr):
            if keymatch[0]:
                keys.add(keymatch[0])
            else:
                keys.add(keymatch[2])
        keys = list(keys)
        if not keys:
            result.append({'css':'ac-error sign', 'value': '',
                           'info': "You seem to be trying to write a pipe command without any keys."})

        agg_pat = AGG_PAT.match(query)
        flt_pat = FLT_PAT.match(query)
        daskey  = query.split('.')[0]
        if  agg_pat:
            matches = filter(lambda x: x.startswith(query), das_aggregators())
            if  matches:
                for match in matches:
                    result.append({'css': 'ac-info', 'value': '%s' % match,
                                   'info': 'Aggregated function <b>%s</b>' % (match)})
        elif  flt_pat:
            matches = filter(lambda x: x.startswith(query), das_filters())
            if  matches:
                for match in matches:
                    result.append({'css': 'ac-info', 'value': '%s' % match,
                                   'info': 'Filter function <b>%s</b>' % (match)})
        elif daskey.strip() == '|':
            result.append({'css': 'ac-warning sign', 'value': query,
           'info': 'DAS pipe must follow either by filter or aggregator function'})
        elif daskey not in daskeys and daskey.find('(') == -1:
            result.append({'css': 'ac-warning sign', 'value': query,
           'info': '<b>%s</b> is neither aggregator, filter or DAS key' % query})
    elif PAT_RELEASE.match(query):
        if  query[0] == 'C': # CMS releases all starts with CMSSW
            release = '%s*' % query
        else:
            release = 'CMSSW_%s*' % query
        result.append({'css': 'ac-info', 'value': 'release=%s' % release, 'info': 'Seems like CMSSW release'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif PAT_TIERS.match(query):
        result.append({'css': 'ac-info', 'value': 'tier=*%s*' % query, 'info': 'Seems like data tier'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif PAT_SLASH.match(query):
        if  PAT_FILE.match(query):
            result.append({'css': 'ac-info', 'value': 'file=%s' % query, 'info': 'Seems like file pattern'})
        elif PAT_BLOCK.match(query):
            result.append({'css': 'ac-info', 'value': 'block=%s' % query, 'info': 'Seems like block name'})
        else:
            result.append({'css': 'ac-info', 'value': 'block=%s*' % query, 'info': 'Seems like block name'})
            result.append({'css': 'ac-info', 'value': 'file=%s*' % query, 'info': 'Seems like file pattern'})
            result.append({'css': 'ac-info', 'value': 'dataset=%s*' % query, 'info': 'Seems like dataset pattern'})
    elif PAT_RUN.match(query):
        result.append({'css': 'ac-info', 'value': 'run=%s' % query, 'info': 'Seems like run number'})
    elif PAT_DATATYPE.match(query):
        result.append({'css': 'ac-info', 'value': 'datatype=%s*' % query, 'info': 'Seems like data type'})
        result.append({'css': 'ac-info', 'value': 'dataset=%s*' % query, 'info': 'Seems like dataset pattern'})
    elif PAT_SITE.match(query):
        result.append({'css': 'ac-info', 'value': 'site=%s*' % query, 'info': 'Seems like site name'})
        result.append({'css': 'ac-info', 'value': 'dataset=%s*' % query, 'info': 'Seems like dataset pattern'})
    elif PAT_SE.match(query):
        result.append({'css': 'ac-info', 'value': 'site=%s' % query, 'info': 'Seems like SE'})
    elif RE_K_SITE.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: site'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_FILE.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: file'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_PR_DATASET.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: primary_dataset'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_JOBSUMMARY.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: jobsummary'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_MONITOR.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: monitor'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_TIER.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: tier'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_RELEASE.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: release'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_CONFIG.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: config'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_GROUP.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: group'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_CHILD.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: child'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_PARENT.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: parent'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_DATASET.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: dataset'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_RUN.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: run'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_BLOCK.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: block'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_STATUS.match(query):
        result.append({'css': 'ac-info', 'value': query, 'info': 'Valid DAS key: status'})
        if  query.find('=') == -1:
            result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    elif RE_K_DATASET.match(query):
        #/something...
        result.append({'css': 'ac-warning sign', 'value': query,
                       'info':'''Seems like dataset query'''})
    elif RE_SITE.match(query):
        #T{0123}_...
        result.append({'css': 'ac-warning sign', 'value':'site=%s' % query,
                       'info':'''Seems like site query. The correct syntax is <b>site=TX_YY_ZZZ</b>'''})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'Seems like dataset pattern'})
    else:
        #we've no idea what you're trying to accomplish, do a search
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query,
                       'info': 'Seems like dataset pattern'})

    if  prev:
        new_result = []
        for idict in result:
            newval = prev + ' ' + idict['value']
            new_result.append({'css':idict['css'], 'value':newval, 'info':idict['info']})
        return new_result
    return result
