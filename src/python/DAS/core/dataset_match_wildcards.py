import pymongo
from pymongo import Connection

# TODO: use global connection
conn = Connection('localhost', 8230)

import re


REPLACE_IF_STRINGS_SAME = 1
DEBUG = 0




from string import Template

dataset_forbidden_symbols = re.compile(r'[^a-zA-Z0-9_-]*')


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




def process_dataset_wildcards(pattern):
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
    """
    db = conn.dbs

    if pattern.count('/') == 3:
        return pattern
        
    # clean up any possible not allowed symbols in the query pattern so it would not mess up our regexps
    pattern = re.sub(dataset_forbidden_symbols, '', pattern)
    
    if not '*' in pattern:
        pattern = '*' + pattern + '*'
        
    # TODO: escape regexp!!!
    # TODO: can we have some strange symbols in the input? shall we just ignore them?

        
    pat_re = '^'+ pattern.replace('*', '(.*)') + '$'
    pat_re_compiled = re.compile(pat_re, re.IGNORECASE)

    # DAS regexp as total datasets DB is only 8MB ;)

    # takes around 2secs for 700results but that is quite normal (Zmm) because of MongoDB
    r = db.cms_dbs_prod_global.find({ 'dataset' : { '$regex' : pat_re, '$options': 'i' } } );
    r = [item['dataset'] for item in r]



    # now match the positions of slash
    options = {}
    options_matches = {}
    results = []
    
    for item in r:
        groups = pat_re_compiled.match(item).groups()
        if DEBUG: print groups
        
        #  a group may contain more than one slash        
        f_replace_group = lambda group: (group.count('/') == 3 and '*/*/*/*') or (group.count('/') == 2 and '*/*/*') or (('/' in group) and '*/*') or '*'

        replacements = tuple([ f_replace_group(group)  for group in groups])
        options[replacements] = options.get(replacements, 0) + 1
        
        # add this into list of possible options
        updated = options_matches.get(replacements, [])
        updated.append(groups)
        options_matches[replacements] = updated


    # process differnet match types
    
    for option, count in options.items():
        if DEBUG: print option, count

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
                
                # all matches start with /
                if DEBUG: print 'matches_first_char', set(matches_first_char)
                if set(matches_first_char) == set('/'):
                    group = group.replace('*/', '/', 1)
                    
                # all matches ends with /
                if DEBUG: print 'matches_last_char', set(matches_last_char)
                if set(matches_last_char) == set('/'):
                    group = group.rreplace('/*', '/', 1)
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


