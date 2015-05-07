#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
a common use-case is retrieving an entity by it's primary key. this module
checks if the input can be unambiguously matched as a single value token and
if so returns a valid DAS Query
"""
from __future__ import print_function
from DAS.utils.regex import NON_AMBIGUOUS_INPUT_PATTERNS, \
    DATASET_PATTERN_RELAXED
from DAS.keywordsearch.entity_matchers.value_matching_dataset import \
    match_value_dataset
from DAS.web.dbs_daemon import list_dbs_instances
from cherrypy import response


def match_dataset_all_inst(kwd, cur_inst):
    """ list matching dataset patterns in all DBS instances """
    if len(kwd) < 3:
        return []
    matches = []
    for inst in list_dbs_instances():
        score, data = match_value_dataset(kwd, inst)
        if not score:
            continue
        data['inst'] = inst
        data['match'] = data.get('adjusted_keyword', kwd)
        # score matches in other DBS instances lower
        score = score - 0.15 if inst != cur_inst else score
        data['score'] = score
        matches.append(data)
    return sorted(matches, key=lambda item: item['score'], reverse=True)


def match_dataset(kwd, cur_inst):
    """ check for dataset match in current DBS instances """
    if len(kwd) < 3:
        return None
    score, data = match_value_dataset(kwd, cur_inst)
    if score:
        return data.get('adjusted_keyword', kwd)


def format_dataset_match(match, dbs_inst):
    """ return an adjusted dataset query """
    new_query = 'dataset={0:s}'.format(match['match'])
    #if match['inst'] != dbs_inst:
    new_query += ' instance={0:s}'.format(match['inst'])
    return new_query


def identify_apparent_query_patterns(uinput, inst=None):
    """
    identify and rewrite the input that is little ambiguous directly into DAS QL
     (the results will contain links to related entities so it's OK if user
     intended something else)

    .. doctest::
        >>> identify_apparent_query_patterns('/A/B/C')
        'dataset=/A/B/C'

        >>> identify_apparent_query_patterns('/A/B1*B2/C')
        'dataset=/A/B1*B2/C'

        >>> identify_apparent_query_patterns('/A/B/C#D')
        'block=/A/B/C#D'

        >>> identify_apparent_query_patterns('T1_CH*')
        'site=T1_CH*'

        >>> identify_apparent_query_patterns('/store/mc/Summer11.root')
        'file=/store/mc/Summer11.root'

    More ambiguous but still unique use-cases:
    .. doctest::
        >>> identify_apparent_query_patterns('/TT*StoreResults*')
        'dataset=/TT*StoreResults*'
    """
    uinput = uinput.strip()

    # only rewrite the value expressions of 1 token
    if len(uinput.split(' ')) > 1 or '=' in uinput:
        return uinput

    matches = [daskey for daskey, pattern in NON_AMBIGUOUS_INPUT_PATTERNS
               if pattern.match(uinput)]
    if len(matches) == 1:
        return '{0}={1}'.format(matches[0], uinput)
    elif len(matches) == 0:
        # on no matches, try slightly more ambiguous dataset pattern:
        # starts with slash,  contains no  #, and not ending with '.root'
        if DATASET_PATTERN_RELAXED.match(uinput):
            return 'dataset={0}'.format(uinput)

    # on no pattern matches, try matching datasets on either dbs instance

    # if input matches some datasets in current dbs instance,
    # return straight away
    dataset_match = match_dataset(kwd=uinput, cur_inst=inst)
    if dataset_match:
        return 'dataset={0:s}'.format(dataset_match)

    # TODO: the code below might need to be reorganized as we have hints...
    dbs_inst = inst
    inst_matches = match_dataset_all_inst(kwd=uinput, cur_inst=dbs_inst)

    # in case of unique match submit adjusted query immediately
    if len(inst_matches) == 1:
        match = inst_matches[0]

        # if the match is over current DBS instance, immediate redirect
        if match['inst'] == dbs_inst:
            return format_dataset_match(match, dbs_inst)
        else:
            # ask user for a confirmation
            msg = 'P.S. The input matches datasets only in a different' \
                  ' DBS instance than currently selected:'
            msg += '\n'
            msg += format_dataset_match(match, dbs_inst)
            response.dataset_matches_msg = msg

    elif len(inst_matches) > 0:
        # we will display all the matches but will not execute any query
        msg = 'P.S. The input is ambiguous. ' \
              'It matches these dataset patterns over some DBS instances:'
        msg += '\n'
        msg += '\n'.join(format_dataset_match(m, dbs_inst)
                         for m in inst_matches)
        print(msg)
        response.dataset_matches_msg = msg

    return uinput


if __name__ == '__main__':
    import doctest
    doctest.testmod()
