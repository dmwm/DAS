#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : nltk_legacy.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: provides necessary functions used by DAS for NLTK version 3.
"""

# system modules
import re

def convert_regexp_to_nongrouping(pattern):
    """
    Convert all grouping parentheses in the given regexp pattern to
    non-grouping parentheses, and return the result.  E.g.:

        >>> from nltk.internals import convert_regexp_to_nongrouping
        >>> convert_regexp_to_nongrouping('ab(c(x+)(z*))?d')
        'ab(?:c(?:x+)(?:z*))?d'

    :type pattern: str
    :rtype: str
    """
    # Sanity check: back-references are not allowed!
    for item in re.findall(r'\\.|\(\?P=', pattern):
        if item[1] in '0123456789' or item == '(?P=':
            raise ValueError('Regular expressions with back-references '
                             'are not supported: %r' % pattern)

    def subfunc(pat):
        """This regexp substitution function replaces the string '('
           with the string '(?:', but otherwise makes no changes."""
        return re.sub(r'^\((\?P<[^>]*>)?$', r'(?:', pat.group())

    # Scan through the regular expression.  If we see any backslashed
    # characters, ignore them.  If we see a named group, then
    # replace it with "(?:".  If we see any open parens that are part
    # of an extension group, ignore those too.  But if we see
    # any other open paren, replace it with "(?:")
    return re.sub(r'''(?x)
        \\.           |  # Backslashed character
        \(\?P<[^>]*>  |  # Named group
        \(\?          |  # Extension group
        \(               # Grouping parenthesis''', subfunc, pattern)
