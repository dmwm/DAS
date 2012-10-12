"""
The current algorithm is simple
1) Fetch all the matches (regexp from MongoDB)
2) for each of them check is what wildcard (*) matched has a slash [running again the regexp in python]
	if so: we will replace * in initial pattern with '*/*'
	otherwise: we leave it as it was

	track all these possible replacements and their counts, and finally apply them

	# possible tune ups (however would not match super new datasets (could be fixed by loading datasets very often and Incrementaly!):
	if all matches for a certain replacement option contain the same string:
		replace it by that string simplifying the expression further and saving time at the provider!

	e.g. for '*Zmm*special*RECO*' would give: /RelValZmm/*/*special*RECO*
	while '*Zmm*' would still give: */*/*Zmm*
"""
import pymongo
from pymongo import Connection

conn = Connection('localhost', 8230)

import re
pattern = '*Zmm*special*RECO*'


REPLACE_IF_STRINGS_SAME = 1

# TODO: ^ $
pat_re = pattern.replace('*', '(.*)')
pat_re_compiled = re.compile(pat_re, re.IGNORECASE)

# DAS regexp? actually it's only 8MB ;)

# 3seconds
results = [item for item in conn.dbs.cms_dbs_prod_global.find() if pat_re_compiled.match(item['dataset'])]

db = conn.dbs
# TODO: escape regexp!!!
# takes around 2secs for 700results but that is quite normal (Zmm) because of MongoDB
r = db.cms_dbs_prod_global.find({ 'dataset' : { '$regex' : pat_re, '$options': 'i' } } );
r = [item['dataset'] for item in r]

# TODO: we could also get rid of empty * (if none of them have items)
# also if all entries all the same, we could replace by the string

# now match the positions of slash
options = {}
options_matches = {}
for item in r:
	groups = pat_re_compiled.match(item).groups()
	print groups
	replacements = tuple([ ('/' in group) and '*/*' or '*'  for group in groups])
	options[replacements] = options.get(replacements, 0) + 1
	# add this into list of possible options
	updated = options_matches.get(replacements, [])
	updated.append(groups)
	options_matches[replacements] = updated



for option, count in options.items():
	print option, count
	pat = pattern
	# TODO: check if all groups are the same, if so replace by a string
	my_matches = options_matches.get(option)
	#print my_matches
	for index, group in enumerate(option):
		if REPLACE_IF_STRINGS_SAME:
			matches = [match[index] for match in my_matches]
			#print matches
			all_matches_equal = len(set(matches)) == 1
			print all_matches_equal
			if all_matches_equal:
				group = matches[0]
		
		pat = pat.replace('*', group, 1)
	print pat



# we could also add this, but only if it always starts with that group.startswith('/') and '/*'
#print [  ('/' in group) and '*/*' or '*'  for group in pat_re_compiled.match(item).groups()]

 
