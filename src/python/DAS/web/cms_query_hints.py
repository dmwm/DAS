#!/usr/bin/env python
#-*- coding: utf8 -*-
"""
provide hints on rather ambiguous input:
- dataset contained in other DBS instances
- case-insensitive dataset matches
"""

from DAS.web.cms_adjust_input import match_dataset_all_inst,\
    format_dataset_match
from DAS.web.dbs_daemon import find_datasets


def get_dataset_token(query):
    """ extract the (possible) dataset value for the query """
    dataset_tokens = [token.replace('dataset=', '')
                      for token in query.split()
                      if 'dataset=' in token]
    if dataset_tokens:
        return dataset_tokens[0]
    else:
        return ''

def repl_dataset_val(query, repl):
    original = get_dataset_token(query)
    return query.replace(original, repl)


# TODO: what if we have keyword search?
def hint_dataset_in_other_insts(query, cur_inst):
    """ find datasets in other DBS instances
     (shown only if no matches in current instance)"""
    dataset_pat = get_dataset_token(query)
    if  not dataset_pat:
        return {}
    matches = match_dataset_all_inst(dataset_pat, cur_inst)

    # for now, display hints ONLY on no matches in the current instance
    if any(m['inst'] == cur_inst for m in matches):
        return

    results = [{'inst': m['inst'],
                'match': m['inst'],
                'query': repl_dataset_val(query, m['match']) +
                         ' instance=' + m['inst'],
                'examples': list(find_datasets(m['match'], m['inst']))}
               for m in matches
               if m['inst'] != cur_inst]
    #print results
    return {'title': 'Matching datasets in other DBS instances',
            'results': results}


def hint_dataset_case_insensitive(query, cur_inst):
    """ case insensitive dataset suggestions
     shown only if current query return no results """
    dataset_pat = get_dataset_token(query)
    if  not dataset_pat:
        return {}
    good_result = lambda m: m != dataset_pat
    if '*' in dataset_pat:
        # the mongo query is quite slow
        # we shall care only if case sensitive search return no results
        exact_matches = find_datasets(dataset_pat, cur_inst, ignorecase=False)
        if next(exact_matches, False):
            return

    matches = [{'match': m,
                'query': repl_dataset_val(query, m)}
               for m in find_datasets(dataset_pat, cur_inst)
               if good_result(m)]
    return {'title': 'Case-insensitive dataset matches (NEW)',
            'descr': '(dataset selection in DBS3 is now case-sensitive)',
            'results': matches}
