#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301

"""
DAS autocomplete function for web UI
"""

__author__ = "Gordon Ball and Valentin Kuznetsov"

from DAS.utils.regex import RE_DBSQL_0, RE_DBSQL_1, RE_DBSQL_2
from DAS.utils.regex import RE_SITE, RE_SUBKEY, RE_KEYS
from DAS.utils.regex import RE_HASPIPE, RE_PIPECMD, RE_AGGRECMD
from DAS.utils.regex import RE_FILTERCMD, RE_K_SITE, RE_K_FILE
from DAS.utils.regex import RE_K_PR_DATASET, RE_K_PARENT, RE_K_CHILD
from DAS.utils.regex import RE_K_CONFIG, RE_K_GROUP, RE_K_DATASET
from DAS.utils.regex import RE_K_BLOCK, RE_K_RUN, RE_K_RELEASE
from DAS.utils.regex import RE_K_TIER, RE_K_MONITOR, RE_K_JOBSUMMARY
from DAS.utils.regex import PAT_RELEASE, PAT_TIERS, PAT_SITE, PAT_SE
from DAS.utils.regex import PAT_BLOCK, PAT_RUN, PAT_FILE, PAT_DATATYPE
from DAS.utils.regex import PAT_SLASH
from DAS.core.das_ql import das_aggregators, das_filters

DAS_PIPECMDS = das_aggregators() + das_filters()

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
    result = []
    prev = ""
    if  len(qsplit) != 1:
        prev = ' '.join(qsplit[:-1])
    query = last_word
#    print "\n### input=%s, last=%s, rel match=%s" % (uinput, query, PAT_RELEASE.match(query))
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
                        result.append({'css': 'ac-warinig sign', 'value':'%s=%s' % (daskey, value),
                                       'info': "This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key, and the condition should <b>probably</b> be expressed like this." % (daskey)})
                    else:
                        result.append({'css': 'ac-warinig sign', 'value':daskey,
                                       'info': "This appears to be a DBS-QL query, but the key (<b>%s</b>) is a valid DAS key. However, I'm not sure how to interpret the condition (<b>%s %s<b>)." % (daskey, operator, value)})
                else:
                    result.append({'css': 'ac-warinig sign', 'value': daskey,
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
        
        pipecmd = RE_PIPECMD.match(query)
        filtercmd = RE_FILTERCMD.match(query)
        aggrecmd = RE_AGGRECMD.match(query)
        
        if pipecmd:
            cmd = pipecmd.group(1)
            precmd = query[:pipecmd.start(1)]
            matches = filter(lambda x: x.startswith(cmd), DAS_PIPECMDS)
            if matches:
                for match in matches:
                    result.append({'css': 'ac-info', 'value': '%s%s' % (precmd, match),
                                   'info': 'Function match <b>%s</b>' % (match)})
            else:
                result.append({'css': 'ac-warinig sign', 'value': precmd,
                               'info': 'No aggregation or filter functions match <b>%s</b>.' % cmd})
        elif aggrecmd:
            cmd = aggrecmd.group(1)
            if not cmd in das_aggregators():
                result.append({'css':'ac-error sign', 'value': '',
                               'info': 'Function <b>%s</b> is not a known DAS aggregator.' % cmd})
            
        elif filtercmd:
            cmd = filtercmd.group(1)
            if not cmd in das_filters():
                result.append({'css':'ac-error sign', 'value': '',
                               'info': 'Function <b>%s</b> is not a known DAS filter.' % cmd})
        
        if aggrecmd or filtercmd:
            match = aggrecmd if aggrecmd else filtercmd
            subkey = match.group(2)
            prekey = query[:match.start(2)]
            members = dasmgr.keylearning.members_for_keys(keys)
            if members:
                matches = filter(lambda x: x.startswith(subkey), members)
                if matches:
                    for match in matches:
                        result.append({'css': 'ac-info', 'value': prekey+match,
                                       'info': 'Possible match <b>%s</b>' % match})
                else:
                    result.append({'css': 'ac-warinig sign', 'value': prekey,
                                   'info': 'No data members match <b>%s</b> (but this could be a gap in keylearning coverage).' % subkey})
            else:
                result.append({'css': 'ac-warinig sign', 'value': prekey,
                               'info': 'No data members found for keys <b>%s</b> (but this might be a gap in keylearning coverage).' % ' '.join(keys)})
            
        
    elif RE_SUBKEY.match(query):
        subkey = RE_SUBKEY.match(query).group(1)
        daskey = subkey.split('.')[0]
        if daskey in daskeys:
            if dasmgr.keylearning.has_member(subkey):
                result.append({'css': 'ac-warinig sign', 'value': '%s | grep %s' % (daskey, subkey),
                               'info': 'DAS queries should start with a top-level key. Use <b>grep</b> to see output for one data member.'})
            else:
                result.append({'css': 'ac-warinig sign', 'value': '%s | grep %s' % (daskey, subkey),
                               'info': "DAS queries should start with a top-level key. Use <b>grep</b> to see output for one data member. DAS doesn't know anything about the <b>%s</b> member but keylearning might be incomplete." % (subkey)})
                key_search = dasmgr.keylearning.key_search(subkey, daskey)
                for keys, members in key_search.items():
                    for member in members:
                        result.append({'css': 'ac-info', 'value':'%s | grep %s' % (daskey, member),
                                       'info': 'Possible member match <b>%s</b> (for daskey <b>%s</b>)' % (member, daskey)})
#                if not key_search:
#                    result.append({'css': 'ac-error sign', 'value': '',
#                                   'info': 'No matches found for <b>%s</b>.' % (subkey)})
        else:
            result.append({'css': 'ac-error sign', 'value': '',
                           'info': "Das queries should start with a top-level key. <b>%s</b> is not a valid DAS key." % daskey})
            key_search = dasmgr.keylearning.key_search(subkey)
            for keys, members in key_search.items():
                result.append({'css': 'ac-info', 'value': ' '.join(keys),
                               'info': 'Possible keys <b>%s</b> (matching <b>%s</b>).' % (', '.join(keys), ', '.join(members))})
#            if not key_search:
#                result.append({'css': 'ac-error sign', 'value': '',
#                               'info': 'No matches found for <b>%s</b>.' % subkey})
                
    elif PAT_RELEASE.match(query):
        if  query[0] == 'C': # CMS releases all starts with CMSSW
            release = '%s*' % query
        else:
            release = 'CMSSW_%s*' % query
        result.append({'css': 'ac-info', 'value': 'release=%s' % release, 'info': 'seems like CMSSW release'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif PAT_TIERS.match(query):
        result.append({'css': 'ac-info', 'value': 'tier=*%s*' % query, 'info': 'seems like data tier'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif PAT_SLASH.match(query):
        result.append({'css': 'ac-info', 'value': 'block=%s*' % query, 'info': 'seems like block name'})
        result.append({'css': 'ac-info', 'value': 'file=%s*' % query, 'info': 'seems like file pattern'})
        result.append({'css': 'ac-info', 'value': 'dataset=%s*' % query, 'info': 'seems like dataset pattern'})
    elif PAT_RUN.match(query):
        result.append({'css': 'ac-info', 'value': 'run=%s*' % query, 'info': 'seems like run number'})
    elif PAT_DATATYPE.match(query):
        result.append({'css': 'ac-info', 'value': 'datatype=%s*' % query, 'info': 'seems like data type'})
        result.append({'css': 'ac-info', 'value': 'dataset=%s*' % query, 'info': 'seems like dataset pattern'})
    elif PAT_SITE.match(query):
        result.append({'css': 'ac-info', 'value': 'site=%s*' % query, 'info': 'seems like site name'})
        result.append({'css': 'ac-info', 'value': 'dataset=%s*' % query, 'info': 'seems like dataset pattern'})
    elif PAT_SE.match(query):
        result.append({'css': 'ac-info', 'value': 'site=%s*' % query, 'info': 'seems like SE'})
    elif RE_K_SITE.match(query):
        result.append({'css': 'ac-info', 'value': 'site=', 'info': 'Valid DAS key: site'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_FILE.match(query):
        result.append({'css': 'ac-info', 'value': 'file=', 'info': 'Valid DAS key: file'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_PR_DATASET.match(query):
        result.append({'css': 'ac-info', 'value': 'primary_dataset=', 'info': 'Valid DAS key: primary_dataset'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_JOBSUMMARY.match(query):
        result.append({'css': 'ac-info', 'value': 'jobsummary=', 'info': 'Valid DAS key: jobsummary'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_MONITOR.match(query):
        result.append({'css': 'ac-info', 'value': 'monitor=', 'info': 'Valid DAS key: monitor'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_TIER.match(query):
        result.append({'css': 'ac-info', 'value': 'tier=', 'info': 'Valid DAS key: tier'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_RELEASE.match(query):
        result.append({'css': 'ac-info', 'value': 'release=', 'info': 'Valid DAS key: release'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_CONFIG.match(query):
        result.append({'css': 'ac-info', 'value': 'config=', 'info': 'Valid DAS key: config'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_GROUP.match(query):
        result.append({'css': 'ac-info', 'value': 'group=', 'info': 'Valid DAS key: group'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_CHILD.match(query):
        result.append({'css': 'ac-info', 'value': 'child=', 'info': 'Valid DAS key: child'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_PARENT.match(query):
        result.append({'css': 'ac-info', 'value': 'parent=', 'info': 'Valid DAS key: parent'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_DATASET.match(query):
        result.append({'css': 'ac-info', 'value': 'dataset=', 'info': 'Valid DAS key: dataset'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_RUN.match(query):
        result.append({'css': 'ac-info', 'value': 'run=', 'info': 'Valid DAS key: run'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_BLOCK.match(query):
        result.append({'css': 'ac-info', 'value': 'block=', 'info': 'Valid DAS key: block'})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_K_DATASET.match(query):
        #/something...
        result.append({'css': 'ac-warinig sign', 'value':'dataset=%s' % query,
                       'info':'''Seems like dataset query. The correct syntax is <b>dataset=/some/dataset</b>'''})
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_SITE.match(query):
        #T{0123}_...
        result.append({'css': 'ac-warinig sign', 'value':'site=%s' % query,
                       'info':'''Seems like site query. The correct syntax is <b>site=TX_YY_ZZZ</b>'''})    
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    elif RE_KEYS.match(query):
        keys = set()
        for keymatch in RE_KEYS.findall(query):
            if keymatch[0]:
                keys.add(keymatch[0])
            else:
                keys.add(keymatch[2])
        for key in keys:
            if not key in daskeys:
#                result.append({'css':'ac-error sign', 'value': '',
#                               'info': 'Key <b>%s</b> is not known to DAS.' % key})
                key_search = dasmgr.keylearning.key_search(query)
                for keys, members in key_search.items():
                    result.append({'css': 'ac-info', 'value': ' '.join(keys),
                                   'info': 'Possible keys <b>%s</b> (matching <b>%s</b>).' % (', '.join(keys), ', '.join(members))})
#                if not key_search:
#                    result.append({'css': 'ac-error sign', 'value': '',
#                                   'info': 'No matches found for <b>%s</b>.' % query})
#                    result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query, 'info': 'seems like dataset pattern'})
    else:
        #we've no idea what you're trying to accomplish, do a search
        result.append({'css': 'ac-info', 'value': 'dataset=*%s*' % query,
                       'info': 'seems like dataset pattern'})
#        key_search = dasmgr.keylearning.key_search(query)
#        for keys, members in key_search.items():
#            result.append({'css': 'ac-info', 'value': ' '.join(keys),
#                           'info': 'Possible keys <b>%s</b> (matching <b>%s</b>).' % (', '.join(keys), ', '.join(members))})
#        if not key_search:
#            result.append({'css': 'ac-error sign', 'value': '',
#                           'info': 'No matches found for <b>%s</b>.' % query})
        
    if  prev:
        new_result = []
        for idict in result:
            new_result.append(prev + ' ' + idict['value'])
        return new_result
    return result
