#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0511
"""
File: das_process_dataset_wildcards.py
Author: Vidmantas Zemleris
Description: The class handles the wildcard search for dataset,
 which given a wildcard pattern (e.g. *Zmm*) produces a wildcard pattern with
 three slashes as required by the data-services.
 It also solves possible ambiguities in the user's input.
"""
from __future__ import print_function
import re
import string
from collections import defaultdict

from DAS.web.dbs_daemon import initialize_global_dbs_mngr, find_datasets
from DAS.utils.regex import DATASET_FORBIDDEN_SYMBOLS, RE_3SLASHES
# should we simplify the wildcard replacement, if all its matches are similar
# in some way (equality, same beginning or end)
REPLACE_IF_STRINGS_SAME = True

# nasty debugging
DEBUG = False


def substitute_multiple(target, replacements, to_replace ='*',):
    """
    Replaces multiple occurences of a search string by individual values
    given in replacements list

    >>> substitute_multiple('A*B*C', ['1', '2'])
    'A1B2C'

    """
    subs = {}
    for index, value in enumerate(replacements):
        subs['v%d' % index] = value
        
    if DEBUG:
        print('trying to substitute * in %s with %s' % (target, subs))
        
    template = target
    for index, value in enumerate(replacements):
        template = template.replace(to_replace, '${v%d}' % index, 1)

    result = string.Template(template).substitute(subs)
    return result


def extract_wildcard_patterns(dbs_inst, pattern, ignorecase=False):
    """
    Given a wildcard query and a list of datasets, we interested in
    how many slashes are matched by each of wildcard (because the slashes has to
    be included in the result).

    it returns counts per each combination of different patterns e.g.
      *Zmm* used regexp (.*)Zmm(.*) where one of the results is the following
       match /RelValZmm/CMSSW.../tier that yield such a combination:
        query   match                   transformed into pattern
        *       '/RelVal'     ->        */*
        Zmm     (query)
        *       '/CMSSW.../tier' ->     */*/*
    """
    # get matching datasets from out cache (through dbs manager instance)
    dbs_mngr_query = pattern
    dataset_matches = find_datasets(dbs_mngr_query, dbs_inst, limit=-1,
                                    ignorecase=ignorecase)

    # we will use these regexps  to extract different dataset patterns
    pat_re = '^' + pattern.replace('*', '(.*)') + '$'
    pat_re = re.compile(pat_re, re.IGNORECASE if ignorecase else 0)

    # now match the positions of slash
    counts = defaultdict(int)
    interpretations = defaultdict(list)
    for item in dataset_matches:
        match = pat_re.match(item)

        # just in case the pat_re regexp was more restrictive than db filtering
        if not match:
            continue

        groups = match.groups()
        if DEBUG:
            print("matched groups", groups)

        # a group may contain more than one slash
        f_replace_group = lambda group: (group.count('/') == 3 and '*/*/*/*')\
                                        or (group.count('/') == 2 and '*/*/*')\
                                        or (group.count('/') == 1 and '*/*')\
                                        or '*'
        replacements = tuple(f_replace_group(group) for group in groups)
        counts[replacements] += 1
        # add this into list of possible options
        interpretations[replacements].append(groups)

    return counts, interpretations


def simplify_wildcard_matches(group, index, the_matches):
    """
    simplify the wildcard replacement, if all its matches are similar
     in some way (equality, same beginning or end)
    """
    matches = [match[index] for match in the_matches]
    # if all matches of '*' are equal, replace them with a match
    if len(set(matches)) == 1:
        group = matches[0]

    # if all matches start with /
    matches_first_char = [match[index][:1] for match in the_matches]
    if set(matches_first_char) == set('/'):
        group = group.replace('*/', '/', 1)

    # if all non empty matches end with /, transform '*/*' into '*/'
    matches_last_char = [match[index][-1:] for match in the_matches]
    if set(matches_last_char) == set('/'):
        if group.endswith('/*'):
            group = group[:-1]
    return group


def process_dataset_wildcards(pattern, dbs_inst, ignorecase=False):
    """
    The current algorithm is simple
    1) Fetch all the matching data-sets (regexp from MongoDB)
    2) for each of them check if the wildcard (*) matched has a slash
        - if so: we will replace * in initial pattern with '*/*'
        otherwise: we leave it as it was

        track all these possible replacements and their counts, and apply them

        possible tune ups:
        if all matches for a certain replacement option contain the same string:
            replace it by that string, simplifying the query for the providers

        e.g. for '*Zmm*special*RECO*' would give: /RelValZmm/*/*special*RECO*
        while '*Zmm*' would still give: ['/*/*Zmm*/*', '/*Zmm*/*/*']

    Tests:
    >>> dbs_inst='prod/global'

    # TODO: case sensitive wildcard suggestions, e.g.
    # *Zmm*CMSSW*RECO* --> /RelValZMM*/CMSSW*/*RECO
    >>> process_dataset_wildcards('*Zmm*CMSSW*RECO*', dbs_inst)
    []

    >>> process_dataset_wildcards('*ZMM*CMSSW*RECO*', dbs_inst)
    [u'/RelValZMM*/CMSSW*/*RECO']

    >>> process_dataset_wildcards('*Zmm*', dbs_inst)
    ['/*/*Zmm*/*']

    #>>> process_dataset_wildcards('*Zmm*', dbs_inst, ignorecase=True)
    #['/*/*Zmm*/*', '/*ZMM*/*/*']

    >>> process_dataset_wildcards('*herwig*/AODSIM', dbs_inst)
    ['/*herwig*/*/AODSIM']

    >>> process_dataset_wildcards('*Zjkjmm*', dbs_inst)
    []

    >>> process_dataset_wildcards('*RelValPyquen_ZeemumuJets_pt10_2760GeV*', dbs_inst)
    [u'/RelValPyquen_ZeemumuJets_pt10_2760GeV/*/*']

    An example of input which is NOT currently converted into a wildcard one
    (but may be done later)
    >>> process_dataset_wildcards('RelValPyquen_ZeemumuJets_pt10_2760GeV', dbs_inst)
    []

    (giving [], instead of: [u'/RelValPyquen_ZeemumuJets_pt10_2760GeV/*/*'])

    >>> process_dataset_wildcards('/ZMM/*', dbs_inst)
    [u'/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM']



    TODO: Other tests, e.g.
    */4C_TuneZ2_7TeV-alpgen-pythia6/Summer11-PU_S4_START42_V11-v1/AODSIM*
    *SingleMu*
    /QCD*/Summer11-START311_V2-v1/GEN-SIM
    /RelVal*CMSSW_5_0_0_pre7*RECO*
    /EG/Run2010A*/AOD
    /*/*2011*/*/*
    """

    # TODO: it is quite probable that people writing Zmm actually mean *Zmm*

    if  RE_3SLASHES.match(pattern):
        return [pattern]

    # clean up any not allowed symbols in pattern that could mess up our regexps
    pattern = re.sub(DATASET_FORBIDDEN_SYMBOLS, '', pattern)

    # first load matching data-sets from cache
    # when group then by different cases (by how many '/' is a '*' matched)
    options, dataset_matches = extract_wildcard_patterns(dbs_inst,  pattern)

    # process each different pattern
    results = []

    #TODO: use the counts, e.g. display number of datasets for each pattern
    for input_interpretation, count in options.items():
        if DEBUG:
            print('option', input_interpretation, count)

        subs = []
        # we check if all groups are the same, if so replace by a string
        the_matches = dataset_matches.get(input_interpretation)
        #print my_matches
        for index, group in enumerate(input_interpretation):
            if REPLACE_IF_STRINGS_SAME:
                group = simplify_wildcard_matches(group, index, the_matches)
            subs.append(group)

        result = substitute_multiple(pattern, to_replace='*', replacements=subs)

        # the pattern should always start with /
        if result.startswith('*/*'):
            result = result.replace('*/*', '/*', 1)

        if DEBUG:
            print('result', result)
        results.append(result)

    return sorted(results)


def test():
    """ Local test function """
    # TODO: init  DAS:   Reading DAS configuration from ...
    print('setUp: getting dbs manager to access current datasets ' \
          '(and fetching them if needed)')

    # make sure DBS dataset cache is initialized
    initialize_global_dbs_mngr(update_required=False)
    import doctest
    doctest.testmod(verbose=True)

if __name__ == "__main__":
    test()
