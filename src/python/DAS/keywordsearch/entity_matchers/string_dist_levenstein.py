# coding=utf-8
"""
module provide custom Levenshtein distance function
"""
from __future__ import print_function
# adapted from http://en.labs.wikimedia.org/wiki/Algorithm_implementation/
# Strings/Levenshtein_distance#Python


def levenshtein(string1, string2, subcost=3, modification_middle_cost=2):
    """ string-edit distance returning min cost of edits needed """
    len1, len2 = len(string1), len(string2)
    dist = [range(len2+1)]
    dist += [[i] for i in xrange(1, len1+1)]

    for i in xrange(0, len1):
        for j in xrange(0, len2):
            cost = subcost
            # are we currently considering middle of the string
            is_middle = not (i in [0, len1-1] or j in [0, len2-1])

            # downrank substitutions inside string.
            if not (string1[i] == string2[j]) and is_middle:
                cost = subcost*1.5
            if string1[i] == string2[j]:
                cost = 0
            mod_cost = modification_middle_cost or (is_middle and 1)
            options = [
                dist[i][j+1] + mod_cost,  # deletion
                dist[i+1][j] + mod_cost,  # insertion
                dist[i][j]+cost  # substitution or leave as is
            ]
            dist[i+1].append(min(options))
    return dist[len1][len2]


def levenshtein_normalized(string1, string2, subcost=2, maxcost=3):
    """ return a levenshtein distance normalized between [0-1]  """
    dist = levenshtein(string1.lower(), string2.lower(), subcost)

    if dist > maxcost:
        return 0.0

    if dist == 0:
        return 1.0

    return 1.0 - float(dist)/(maxcost*1.6)


if __name__ == "__main__":
    print(levenshtein_normalized('dataset', 'dateset', subcost=2, maxcost=3))
    print(levenshtein_normalized('lumi', 'luminosity', subcost=2, maxcost=3))
    # deletions in the beginning, or in the middle shall be also quite expensive
    # should this be allowed at all?
    print(levenshtein_normalized('are', 'parent', subcost=2, maxcost=3))
    print(levenshtein_normalized('config', 'configuration',
                                 subcost=2, maxcost=3))
    print(levenshtein_normalized('size', 'site', subcost=2, maxcost=3))
    print(levenshtein_normalized('size', 'sizes', subcost=2, maxcost=3))
