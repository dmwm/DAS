__author__ = 'vidma'
# adapted from http://en.labs.wikimedia.org/wiki/Algorithm_implementation/Strings/Levenshtein_distance#Python

def levenshtein(s, t, subcost=3, maxcost=3, modification_middle_cost=2):
    m, n = len(s), len(t)
    d = [range(n+1)]
    d += [[i] for i in range(1,m+1)]
    #subs = [range(n+1)]
    #subs += [[0] for i in range(1,m+1)]

    for i in range(0,m):
        for j in range(0,n):
            cost = subcost
            # are we currently considering middle of the string
            is_middle = not (i  in [0, m-1] or j in [0, n-1])

            # downrank substitutions inside string.
            # TODO: misspellings could be propocessed even in a smarter way...
            if not (s[i] == t[j]) and is_middle:
                cost = subcost*1.5

            if s[i] == t[j]:
                cost = 0


            mod_cost = modification_middle_cost or (is_middle and 1)

            opts = [
                d[i][j+1] + mod_cost, # deletion
                d[i+1][j] + mod_cost # insertion
            ]

            opts.append(d[i][j]+cost) #substitution or leave as is

            d[i+1].append(min(opts))

    return d[m][n]


def levenshtein_normalized(s, t, subcost=2, maxcost=3):
    dist = levenshtein(s.lower(), t.lower(), subcost, maxcost)

    if dist > maxcost:
        return 0.0

    if dist == 0:
        return 1.0

    return 1.0 - float(dist)/(maxcost*1.6)


if __name__=="__main__":
    #print levenshtein(argv[1],argv[2])

    print levenshtein_normalized('dataset', 'dateset', subcost=2, maxcost=3)

    print levenshtein_normalized('lumi', 'luminosity', subcost=2, maxcost=3)


    # deletions in the beginning, or in the middle shall be also quite expensive
    # should this be allowed at all?
    print levenshtein_normalized('are', 'parent', subcost=2, maxcost=3)

    print levenshtein_normalized('config', 'configuration', subcost=2, maxcost=3)

    print levenshtein_normalized('size', 'site', subcost=2, maxcost=3)

    print levenshtein_normalized('size', 'sizes', subcost=2, maxcost=3)