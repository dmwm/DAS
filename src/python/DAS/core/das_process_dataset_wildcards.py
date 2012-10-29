import pymongo
from pymongo import Connection

# TODO: use global connection
conn = Connection('localhost', 8230)

import re


REPLACE_IF_STRINGS_SAME = 1
DEBUG = 1

from DAS.utils.das_db import db_connection, is_db_alive, create_indexes


from string import Template

# allowed characters: letters, numbers, dashes and obviously  *
dataset_forbidden_symbols = re.compile(r'[^a-zA-Z0-9_\-*]*')


def substitute_multiple(target, what ='*', replacements= []):
    """
    Replaces multiple occurences of a search string by different values given in replacements list
    """
    subs = {}
    for index, value in enumerate(replacements):
        subs['v%d' %index] = value
        
    if DEBUG: print 'trying to substitute * in %s with %s' % (target, subs)
        
    template = target
    for index, a in enumerate(replacements):
        template = template.replace('*', '${v%d}' % index, 1)
    if DEBUG: print 'template:', template
        
    result = Template(template).substitute(subs)      
    if DEBUG: print 'result:', result
    
    return result




"""
def __init__(self, config = None):
    # self.dbcoll     = dbs_instance(dbs_url)
    self.dbname = config.get('dbname', 'dbs')
    self.dburi = self.dasconfig['mongodb']['dburi']
    self.conn = db_connection(self.dburi)
"""


def test_standalone(pattern):
    db = conn.dbs

    if not '*' in pattern:
        pattern = '*' + pattern + '*'

    pat_re = '^'+ pattern.replace('*', '(.*)') + '$'
    #pat_re_compiled = re.compile(pat_re, re.IGNORECASE)

    # DAS regexp as total datasets DB is only 8MB ;)

    # takes < 2secs for 700results but that is quite normal (Zmm) because of MongoDB
    r = db.cms_dbs_prod_global.find({ 'dataset' : { '$regex' : pat_re, '$options': 'i' } } );
    r = [item['dataset'] for item in r]

    return r


def process_dataset_wildcards(pattern, dbs_mngr):
    """
    The current algorithm is simple
    1) Fetch all the matches (regexp from MongoDB)
    2) for each of them check is what wildcard (*) matched has a slash [running again the regexp in python]
        if so: we will replace * in initial pattern with '*/*'
        otherwise: we leave it as it was

        track all these possible replacements and their counts, and finally apply them

        possible tune ups (however would not match super new datasets (could be fixed by loading datasets very often and Incrementaly!):
        if all matches for a certain replacement option contain the same string:
            replace it by that string simplifying the expression further and saving time at the provider!

        e.g. for '*Zmm*special*RECO*' would give: /RelValZmm/*/*special*RECO*
        while '*Zmm*' would still give: ['/*/*Zmm*/*', '/*Zmm*/*/*']

    >>> process_dataset_wildcards('*Zmm*special*RECO*')
    [u'/RelValZmm/*special*/*RECO']

    >>> process_dataset_wildcards('*Zmm*')
    ['/*/*Zmm*/*', '/*Zmm*/*/*']

    >>> process_dataset_wildcards('*herwig*/AODSIM')
    ['/*herwig*/*/AODSIM']

    >>> process_dataset_wildcards('*Zjkjmm*')
    []

    >>> process_dataset_wildcards('*RelValPyquen_ZeemumuJets_pt10_2760GeV*')
    ['/RelValPyquen_ZeemumuJets_pt10_2760GeV/*/*']

    >>> process_dataset_wildcards('RelValPyquen_ZeemumuJets_pt10_2760GeV')
    ['/RelValPyquen_ZeemumuJets_pt10_2760GeV/*/*']


    Other tests:
    */4C_TuneZ2_7TeV-alpgen-pythia6/Summer11-PU_S4_START42_V11-v1/AODSIM*
    *SingleMu*
    /QCD*/Summer11-START311_V2-v1/GEN-SIM
    /RelVal*CMSSW_5_0_0_pre7*RECO*
    /EG/Run2010A*/AOD
    /*/*2011*/*/*

    """

    # TODO: it is quite probably people event by writing Zmm mean *Zmm*, how to handle this?
    # while however if they end it by RECO, it signifies the end
    # especially if it do not start with /, it's 100% *

    if pattern.count('/') == 3:
        return pattern

    # clean up any possible not allowed symbols in the query pattern so it would not mess up our regexps
    pattern = re.sub(dataset_forbidden_symbols, '', pattern)

    # we want to have all prefixes and suffixes, so if possible we could autocomplete it fully


    # TODO: However if pattern do not start with / or do not end with * shall we will add it?
    # we know dataset must ALWAYS START with /
    # Currently I don't
    print 'starting dbs mgr:', dbs_mngr, 'pat:', pattern

    #dbs_mngr_query = pattern + pattern.endswith('*') and '$' or ''
    dbs_mngr_query = pattern


    if DEBUG: print 'dbs.find pattern:', dbs_mngr_query

    dataset_matches = dbs_mngr.find(dbs_mngr_query, limit=-1)

    #TODO: remove this
    if DEBUG:
        dataset_matches = [d for d in dataset_matches]
        print 'returned by dbs.find:', dataset_matches
        print 'len:',len(dataset_matches)

    # this is for internal matching
    pat_re = '^'+ pattern.replace('*', '(.*)') + '$'
    pat_re_compiled = re.compile(pat_re, re.IGNORECASE)

    # now match the positions of slash
    options = {}
    options_matches = {}
    results = []

    for item in dataset_matches:
        match = pat_re_compiled.match(item)

        # cover cases if we used a bit different index, than the this internal regexp
        if not match:
            continue

        groups = match.groups()
        if DEBUG: print "matched groups", groups

        #  a group may contain more than one slash
        f_replace_group = lambda group: (group.count('/') == 3 and '*/*/*/*') \
                                        or (group.count('/') == 2 and '*/*/*') \
                                        or (group.count('/') == 1 and '*/*') \
                                        or '*'

        replacements = tuple([ f_replace_group(group)  for group in groups])
        options[replacements] = options.get(replacements, 0) + 1

        # add this into list of possible options
        updated = options_matches.get(replacements, [])
        updated.append(groups)
        options_matches[replacements] = updated


    # process diffrent match types

    for option, count in options.items():
        if DEBUG: print 'option', option, count

        subs = []
        # TODO: check if all groups are the same, if so replace by a string
        my_matches = options_matches.get(option)
        #print my_matches
        for index, group in enumerate(option):
            if REPLACE_IF_STRINGS_SAME:
                matches = [match[index] for match in my_matches]
                matches_first_char = [match[index][:1] for match in my_matches]
                matches_last_char = [match[index][-1:] for match in my_matches]
                #print matches
                all_matches_equal = len(set(matches)) == 1
                # if DEBUG: print all_matches_equal

                # if all matches are same, we replace that wildcard with a string
                if all_matches_equal:
                    group = matches[0]

                # if all matches start with /
                if DEBUG: print 'matches_first_char', set(matches_first_char)
                if set(matches_first_char) == set('/'):
                    group = group.replace('*/', '/', 1)

                # if all non empty matches ends with /, we transform */* into */
                if DEBUG: print 'matches_last_char', set(matches_last_char)
                if set(matches_last_char) == set('/'):
                    if group.endswith('/*'):
                        group = group[:-1]
            subs.append(group)


        result = substitute_multiple(pattern, what ='*', replacements=subs)

        # the pattern should always start with /
        if result.startswith('*/*'):
            result = result.replace('*/*', '/*', 1)
        if DEBUG: print result
        results.append(result)

    results.sort()
    return results

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    #print process_dataset_wildcards('*Zmm*')
    #DEBUG = 1
    #print process_dataset_wildcards('*Zmm*special*RECO*')


