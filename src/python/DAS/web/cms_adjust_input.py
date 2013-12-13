#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
a common use-case is retrieving an entity by it's primary key. this module
checks if the input can be unambiguously matched as a single value token and
if so returns a valid DAS Query
"""
from DAS.utils.regex import NON_AMBIGUOUS_INPUT_PATTERNS


def identify_apparent_query_patterns(uinput):
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
    """
    # only rewrite the value expressions of 1 token
    if len(uinput.split(' ')) > 1 or '=' in uinput:
        return uinput
    matches = [daskey for daskey, pattern in NON_AMBIGUOUS_INPUT_PATTERNS
               if pattern.match(uinput)]
    if len(matches) == 1:
        return '{}={}'.format(matches[0], uinput)
    else:
        print matches
    return uinput


if __name__ == '__main__':
    import doctest
    doctest.testmod()
