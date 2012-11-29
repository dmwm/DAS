#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

#TODO: implement simplified version of
# /home/vidma/Desktop/DAS/DAS_code/DAS/prototypes/!!! Keyword Search over Relational Databases_Metadata_BergamaschiDGLV11.pdf
"""
Standard vocabulary:
field -- api input parameter
value

Entry points for keyword search:
- system ? [eg dbs/phedex only?]

- entity name
- field name (das short, das long?, of the service?)
- field value matching a it's regexp constraint at some API:
    especially useful for sufficiently restrictive constraintse.g:
    * dataset name
        (worse if that's a wildcard pattern --> but don't we allow wildcard only here?)
    * filename
    * email
    * release: CMSSW_*

    * date?


    and fields with (almost) static low-arity values:
    * site
    * tier
    * status
    * datatype


    we may by chance have some of the values requested in our long-term historical cache, so to
    figure out matching fields


Other important part are the selections and maybe even filters:
    (users may be interested only in specific sub-field, currently the summaries e.g. for dataset costs a lot)
    - so it may match a result item's field name
    - result items field value? (worse, because we don't have any data about this)

Post processing:
a number of best and cheap matches could be port-processed by running the queries live...


General ideas:
 - each of these are assigned a weight



Any synonyms we have?


--------
A keyword may be matched into:
- result type (entity)
- result fields

- input parameter name
- input parameter value


----

TODO: Springer 2012
The fundamental differ-
ence is that they do not assume any a-priori access to the database instance. Un-
avoidably, the approaches are based on schema and meta-data, i.e., the intensional
information, which makes them applicable to scenarios where the other techniques
cannot work.


--------------------
  Statisttics
----------------------
* services
    - execution time: min max avg [some deviation that could be calculated cheaply]
    - number of results returned

* data
    - (!) arity of each entity (i.e. number of possible values)
        --> calculate historically or request from services (doable for DBS for now)
    -



--------------------------
    Historical data
--------------------------
    historical query results?
    --> if arity of some parameters is high, the possible caching combinations are also VERY high
"""
# TODO: check how databases are managing views and their optimizations (statistcis etc)


# TODO: check on interconnectiins between services (data returned, compatibility etc)


# TODO: approximate keyword search? feasible on DBs, worse here

# TODO: approach to history

# TODO: a high performance key-value store for cache and historical info


import re
import pprint
import difflib
import math

# for handling semantic and string similarities
import jellyfish
from nltk.corpus import wordnet
from nltk.corpus import stopwords

from DAS.core.das_core import DASCore


dascore = DASCore()
mappings = dascore.mapping


DEBUG = False


# TODO: if field has fairly static values, then given value is not in there, is shall be penalized
static_field_values = {
    'site': [],
    'tier': [],
    'status': [],
    'datatype': []
}

entity_wordnet_synsets = {
    'site': wordnet.synset('site.n.01'),
    'status': wordnet.synset('condition.n.01'),
    'dataset': wordnet.synset('datum.n.01'), # an item of factual information derived from measurement or research (TODO: actually it is a set of these)
    'date': wordnet.synset('date.n.01'),
    'config': wordnet.synset('configuration.n.01'),
    'block': wordnet.synset('block.n.06'),
    'lumi': wordnet.synset('luminosity.n.01'),
    # ("Synset('block.n.06')",
    #'(computer science) a sector or group of sectors that function as the smallest data unit permitted',
    #["since blocks are often defined as a single sector, the terms `block' and `sector' are sometimes used interchangeably"]),

}


#print mappings.find_system(system)

# api: listFileProcQuality api2daskey: [u'file', u'dataset', u'file_quality'] apitag: None
#primary_key file
#primary_mapkey file.name
# api returns entities: [{u'map': u'file.name', u'key': u'file', u'pattern': u''}, {u'map': u'dataset.name', u'key': u'dataset', u'pattern': u''}, {u'map': u'file_quality', u'key': u'file_quality', u'pattern': u''}]

apis_by_their_input_contraints = {}

# TODO: Other possibility is just look up DB:
# find .match(value)
# but mongodb is slow and I'd prefer not to relay on it


entity_names = {} #  dataset.name -> values, user, user.email, user.name
search_field_names = [] # i.e. das key

api_input_params = []

#TODO: entities of API results !!!

# Discover APIs
def discover_apis_and_fields():
    UGLY_DEBUG = False

    global system, apis, api, entity_long, entity_short, parameters, api_info, api_params, p, i, param, param_constraint
    for system in dascore.mapping.list_systems():
        print 'Sys:', system
        print ''
        apis = mappings.list_apis(system)

        for api in apis:
            entity_long = mappings.primary_mapkey(system, api)
            entity_short = mappings.primary_key(system, api)
            parameters = mappings.api2daskey(system=system, api=api)


            entity_names[entity_long or entity_short] = entity_short

            #print 'primary_key', mappings.primary_key(system, api)
            #print 'primary_mapkey', mappings.primary_mapkey(system, api)


            #pprint(api_info)

            # TODO: in schema mapping, the entity returned is not allways exactly that, e.g.
            # api: people_via_email(user) --> user.email
            #entity returned: user.email (short daskey: user)
            #api input constraints: [{u'map': u'user.email', u'key': u'user', u'pattern': u'[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]{2,4}'}]
            #
            #
            #api: people_via_name(user) --> user.name
            #entity returned: user.name (short daskey: user)
            #api input constraints: [{u'map': u'user.name', u'key': u'user', u'pattern': u''}]



            #print 'api info', mappings.api_info(api)
            api_info = mappings.api_info(api)


            # could contain some low level params like API version, default values etc
            api_int_params_list = api_info.get('params', {})

            # das map also contains the mapping of the result entity, so the list of params is intersection of the two
            api_params = [param for param in api_info.get('das_map', [])
                            if param.get('api_arg', param['das_key']) in set(api_int_params_list.keys())]

            # figure out what are the required params
            api_params_required = set([p_name
                                   for p_name, p_spec in api_int_params_list.items()
                                   if p_spec == 'required'])
            required_params = [param['rec_key']
                               for param in api_params
                               if param.has_key('api_arg') and param['api_arg'] in api_params_required]
            #print required_params

            if UGLY_DEBUG:
                print '--------------------------'
                #print 'api: %s(%s) --> %s' %(api, ','.join(parameters), entity_long)
                print 'api: %s(%s) --> %s' % (api, ','.join([p['rec_key'] for p in api_params]), entity_long)
                print 'entity returned: %s (short daskey: %s)' % (entity_long, entity_short)
                #print 'api input constraints:', api_info.get('daskeys', [])
                print 'required:', required_params, ' params:', api_params
                pprint.pprint(api_info)

            # TODO: "required"
            #print api_params

            for param in api_params:
                param_constraint = param.get('pattern', '')
                apis_by_their_input_contraints[param_constraint] = apis_by_their_input_contraints.get(param_constraint,
                    [])

                apis_by_their_input_contraints[param_constraint].append(
                    {'api': api, 'system': system, 'key': param['das_key'], 'entity_long': param['rec_key'] , 'constr': param_constraint})


            # response entity, fields (entity.attr), required_params
            api_def = (entity_long, set([param['rec_key'] for param in api_params]), set(required_params), api)
            if not api_def in api_input_params:
                api_input_params.append(api_def)

            #mappings.das2api()
            if DEBUG: print '\n'


            # we want both global (DAS) naming (smf.smf + das key) + TODO: naming at the service (with lower priority)


            # TODO: get the service result format

discover_apis_and_fields()
search_field_names = set(entity_names.values())


print "APIS without required params:"
print '\n'.join(["%s(%s) --> %s" % (api, ','.join(params), entity)
                                        for (entity, params, required, api_name) in api_input_params
                                        if not required])



# inverted term frequency in the schema, to lower the importance of very common term (e.g. name (dataset.name, file.name)
idf = {
}

for entity_long in entity_names.keys():
    # TODO: stemming?
    # term = entity_long # dataset.name
    if '.' in entity_long:
        term = entity_long.split('.')[1]
        idf[term] = idf.get(entity_long, 0) + 1

for term in search_field_names:
    # TODO: stemming?
    idf[term] = idf.get(entity_long, 0) + 1

N = len(idf)
for (term, frequency) in idf.items():
    idf[term] = math.log(float(N) / frequency)

# TODO: normalize them
min_idf = min(idf.values())
for (term, idf_value) in idf.items():
    idf[term] = idf_value / min_idf


# TODO: we should also include fields, but with lower importance than the search keys (entities)


print 'entity_names'
pprint.pprint(entity_names)
print 'search_field_names'
pprint.pprint(search_field_names)

#print 'apis:', apis_by_their_input_contraints

#from pprint import pprint
#pprint( mappings.api_info('listFiles'))


# TODO: use mapping to entity attributes even independent of the entity itself (idf-like inverted index)

def string_distance(keyword, match_to, semantic=False, allow_low_scores= False):
    # TODO: use some good string similarity metrics: string edit distance, jacard, levenshtein, hamming, etc
    # TODO: use ontology

    # if contains is good, insertions are worse
    score = (jellyfish.jaro_winkler(keyword, match_to) + \
             difflib.SequenceMatcher(a=keyword, b=match_to).ratio() ) / 2

             # TODO: promote matching substrings

    #if score > 0.5 or keyword == 'location':
    #    print 'score:', score

    # TODO: similarity shall not be used at all if the words are not similar enough

    # TODO: we shall be able to handle attributes also
    if semantic and not '.' in match_to:
        ks = wordnet.synsets(keyword)
        # TODO: we shall can select the relevant synsets for our schema entities manually for improved results
        if entity_wordnet_synsets.has_key(match_to):
            ms = [entity_wordnet_synsets[match_to]]
            #else:
            #ms = wordnet.synsets(match_to)
            if ms and ks:
                avg = lambda l: sum(l)/len(l)

                if DEBUG and keyword == 'location':
                    print 'location similarities to ', match_to, ['%.2f' %  k.wup_similarity(m) for k in ks for m in ms if k.wup_similarity(m)]
                similarities = [k.wup_similarity(m) for k in ks for m in ms if k.wup_similarity(m)]
                semantic_score = similarities and max(similarities) or 0.0

                if score < 0.7:
                    score = semantic_score
                else:
                    score = max(semantic_score, (score + 2*semantic_score)/3)

    if allow_low_scores:
        return score if score > 0.1 else  0
    else:
        return score if score > 0.5 else  0



def keyword_schema_weights(keyword,  use_fields=True, include_fields =False, include_operators=False):
    """
    for each schema term (entity, entity attribute) calculates keyword's semantic relatedness with it

    based on:
    - similarity to schema terms (string similarity, language ontology [WordNet, ])
        relatedness to entity could also be based on it's fields (e.g. email being field of user,
        especially when other entities do not have such field)

    - semantic distance (google similarity distance) TODO: ref [11]

    later this could also include operators (last 24h, 2days, 10 largest datasets, aggreation and attribute selection)
    e.g. 'latest' would be closest to 'date', 'largest' -> size

    (TODO: ideally a separate method shall use schema ontology if exists to use relations between different entities)

    Unit tests:
    >>> keyword_schema_weights('time')[0][1] # shall be close to date
    u'date'
    >>> keyword_schema_weights('location')[0][1] # shall be close to site
    u'site'
    >>> keyword_schema_weights('configuration')[0][1] # shall be close to config
    u'config'
    >>> keyword_schema_weights('email')[0][1] # user contains email field
    u'user'
    >>> keyword_schema_weights('email', include_fields=True)[0][1] # user contains email field
    u'user.email'
    """
    # TODO: could we use HEP ontology (i'm affraid it has less to do with DAS)
    # TODO: use fields to map to entities
    # TODO: operators

    # TODO: use IDF (some field subparts are very common, e.g. name)

    if include_fields:
        result =  [(string_distance(keyword, entity, semantic=True), entity_long)
                   for (entity_long, entity) in entity_names.items()]
    else:
        if use_fields:
            # for now a very dumb matching, to both entity.name and entity by string comparison
            result =  [(string_distance(keyword, entity_with_field, semantic=True), entity)
                       for (entity_with_field, entity) in entity_names.items()]
            result.extend([(string_distance(keyword, entity, semantic=True), entity)
                       for (entity_with_field, entity) in entity_names.items()])
        else:
            result =  [(string_distance(keyword, entity, semantic=True), entity) for entity in search_field_names]


    result = filter(lambda item: item[0] > 0, result)

    result.sort(key=lambda item: item[0], reverse=True)
    return result

def keyword_value_weights(keyword, api_results_allowed=False):
    """
    for each attribute, calculates possibility that given keyword is a value of the attribute
    (we are mostly interested in API parameters, but TODO: this could be extended to API result fields also for post filtering)

    this is done by employing some of these methods:
    - string similarity to known (or historical) values
        (while positives confirm the belonging to certain field,
        the negatives shall not exclude such possibility  as historical values we have are not complete, )
    - matching to regexps defining the values
    """
    # TODO: compare values: dataset, static values and slowly changing, and historical data

    # TODO: we wish the generated query to match the APIs (so we promote such queries
    # even if combined queries were implemented/enabled, we still wish to match API parameters with higher priority
    scores = []
    apis =  keyword_regexp_weights(keyword)
    for (score, matches) in apis:
        scores.extend([(score, m['entity_long']) for m in matches])
    scores = filter(lambda item: item[0]>0, scores)
    scores = list(set(scores))

    # check for matching of existing datasets
    # TODO: use instance from elsewhere (from web server if available)
    from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr, process_dataset_wildcards
    import DAS.web.dbs_daemon
    DAS.web.dbs_daemon.KEEP_EXISTING_RECORDS_ON_RESTART = 1
    DAS.web.dbs_daemon.SKIP_UPDATES = 1
    dbsmgr = get_global_dbs_mngr()


    dataset_score = None
    adj_keyword = keyword


    # dbsmgr.find returns a generator, to check if it's non empty we have to access it's entities
    if next(dbsmgr.find(pattern=keyword, limit=1), False):
        # TODO: if contains wildcards score shall be a bit lower
        if '*' in keyword and not '/' in keyword:
            dataset_score = 0.8
        elif '*' in keyword and '/' in keyword:
            dataset_scores = 0.9
        elif not '*' in keyword:
            if next(dbsmgr.find(pattern='*%s*' % keyword, limit=1), False):
                dataset_score = 0.7
                adj_keyword = '*%s*' % keyword
        else:
                dataset_score = 1.0




    found = 0
    for (index, (score, entity)) in enumerate(scores):
        # TODO: check for other static entities

        # TODO: a dataset pattern could be even *Zmm* -- we need minimum length here!!

        if entity == 'dataset.name':
            # TODO: check for existence
            # TODO: patterns like *Zmm*, attention: this may be quite expensive
            # TODO: fuzzy matching

            scores[index] = (max(score, dataset_score) or score, {'map_to': 'dataset.name', 'adjusted_keyword': adj_keyword})

    if dataset_score and not found:
        scores.append(   (dataset_score, {'map_to': 'dataset.name', 'adjusted_keyword': adj_keyword}) )


    scores.sort(key=lambda item: item[0], reverse=True)
    return scores


def keyword_regexp_weights(keyword):
    # TODO: shall value match definitions of ALL APIs or some!? I'd say some is enough
    scores = []

    # TODO: define that is more restrictive regexp

    for (constraint, apis) in apis_by_their_input_contraints.items():
    #print (constraint, apis)


        # TODO: I've hacked file/dataset regexps to be more restrictive as these are well defined
        if not '^' in constraint and not '$' in constraint and apis[0]['key'] in ['dataset', 'file',]:
            constraint = '^' + constraint + '$'
        score = 0

        # We shall prefer non empty constraints
        # We may also have different weights for different types of regexps
        if re.search(constraint, keyword):
            #print apis

            if constraint.startswith('^') and  constraint.endswith('$'):
                score = 0.7
            elif constraint.startswith('^') or  constraint.endswith('$'):
                score = 0.6
            elif constraint != '':
                score = 0.5

            score = (score, apis)

            scores.append(score)

    scores.sort(key=lambda item: item[0], reverse=True)
    #print scores
    return scores



def entities_for_input_params(params):
    """
    returns the list of entities that could be returned with given parameter set
    """

    entities = {}
    for (api_entity, api_params_set, api_required_params_set, api) in api_input_params:
    #print set(params), api_params_set, ' api ent:', api_entity
        if params.issubset(api_params_set)\
           and params.issuperset( api_required_params_set):
            # yield (api_entity, api_params_set, api_required_params_set)
            #print 'ok', api_entity
            if api_entity is None:
                print 'API RESULT TYPE IS NONE:',(api_entity, api_params_set, api_required_params_set)
                continue

            entities[api_entity] = entities.get(api_entity, [])
            entities.get(api_entity, []).append( (api_entity, api_params_set, api_required_params_set) )
    #print entities

    return entities



def validate_input_params_mapping(params, entity=None, final_step=False):
    """
    input parameters mapping (from keywords to input parameters) is that there must exist an API
    """
    # TODO: this could also check if actual values are matching API input param requirements
    if final_step:
        # TODO: check that all required params are included
        #print 'validate_input_params_mapping(final):',params, entity
        # currently - a valid mapping shall contain attributes from some api and must satisfy it's minimum input requirements
        # TODO: at the moment we do not allow additional selection
        for (api_entity, api_params_set, api_required_params_set, api) in api_input_params:
            #print set(params), api_params_set, ' api ent:', api_entity
            if set(params).issubset(api_params_set) \
                and set(params).issuperset( api_required_params_set) \
                and (entity is None or entity == api_entity):
                    return api_entity, api_params_set, api_required_params_set
    else:
        # requirement for any params mapping is that there must exist such a
        for (api_entity, api_params_set,  api_required_params_set, api) in api_input_params:
            if set(params).issubset(api_params_set) and (entity is None or entity == api_entity):
                return True

    return False



def generate_value_mappings(requested_entity, fields_included, schema_ws, values_ws, probability, values_mapping = {}, keywords_used = set([]), keywords_list=[], keyword_index=0,):
    SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT = 1.5

    # TODO: modify the value and schema mappings weights according to previous mappings
    global final_mappings
    UGLY_DEBUG = False

    fields_included = set(fields_included)
    fields_covered_by_values_set = set(values_mapping.keys())

    # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
    # newones could still be added

    if UGLY_DEBUG:
        print 'generate_value_mappings(', requested_entity, fields_included, schema_ws, values_ws, probability, values_mapping, keywords_used, keywords_list, keyword_index,')'

    if keyword_index == len(keywords_list):
        #print keyword_index, 'final'
        # DAS requires at least one filtering attribute
        if fields_covered_by_values_set and fields_covered_by_values_set.issuperset(fields_included) and \
           validate_input_params_mapping(fields_included, final_step=True, entity=requested_entity):
            if UGLY_DEBUG: print 'VALUES MATCH:', (requested_entity, fields_included, values_mapping ),\
            validate_input_params_mapping(fields_included, final_step=True, entity=requested_entity)

            # Adjust the final score to favour mappings that cover most keywords
            N_keywords = len(schema_ws.keys())

            if not requested_entity:
                # if not entity was guessed, infer it from service parameters
                entities = entities_for_input_params(fields_included)
                if UGLY_DEBUG: print 'Result entities matching:', entities

                for requested_entity in entities.keys():
                    adjusted_score = probability * len(keywords_used) / N_keywords
                    if requested_entity in values_mapping.keys():
                        adjusted_score *= SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT
                    final_mappings.append( (adjusted_score, requested_entity, tuple(values_mapping.items())) )
            else:
                adjusted_score = probability * len(keywords_used) / N_keywords
                final_mappings.append( (adjusted_score, requested_entity, tuple(values_mapping.items())) )




        return

    #print 'continuing at index:', keyword_index

    # we either take keyword[i] or not
    keyword = keywords_list[keyword_index]
    keyword_weights = values_ws[keyword]

    # case 1) we do not take keyword[i]:
    generate_value_mappings(requested_entity, fields_included, schema_ws, values_ws, probability, values_mapping,
        keywords_used, keywords_list, keyword_index=keyword_index+1,)

    # case 2) we do take keyword[i]:
    if keyword not in keywords_used:

        for score, possible_mapping in keyword_weights:
            keyword_adjusted = keyword
            if isinstance(possible_mapping, dict):
                keyword_adjusted = possible_mapping['adjusted_keyword']
                possible_mapping = possible_mapping['map_to']

            # currently we do not allow mapping two keywords to the same value
            # TODO: It could be in theory useful combining a number of consecutive keywords refering to the same value
            if possible_mapping in fields_covered_by_values_set:
                continue

            vm_new = values_mapping.copy()
            vm_new[possible_mapping] = keyword_adjusted

            new_score = probability + score

            # we favour the values mappings that have also been refered in schema mapping (e.g. dataset *Zmm*)
            # TODO: It could be in theory useful combining a number of consecutive keywords refering to the same value
            if possible_mapping in fields_included:
                new_score *= SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT

            new_fields = fields_included | set([possible_mapping])

            if validate_input_params_mapping(new_fields, final_step=False, entity=requested_entity):
                generate_value_mappings(requested_entity, fields_included = new_fields, \
                    values_mapping = vm_new,\
                    probability=new_score, keywords_used = keywords_used | set([keyword]),\
                    schema_ws=schema_ws, values_ws=values_ws, keyword_index=keyword_index+1, keywords_list=keywords_list)


            # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
            # newones could still be added





# TODO: the recursion is dumb, we could at least use some pruning
def generate_schema_mappings(requested_entity, fields_old, schema_ws, values_ws, probability, keywords_list=[], keyword_index=0, keywords_used = set([])):
    # TODO: keyword order is important
    UGLY_DEBUG = False

    # generate_values_mappings()
    # TODO: shall we modify the value and schema mappings weights according to previous mappings HERE or only when doing VALUE matching?

    # TODO: it would be better to consider all items in decreasing scores
    global final_mappings


    if keyword_index  == len(keywords_list):

        # TODO: check if required fields are functioning properly !!!
        if validate_input_params_mapping(fields_old, final_step=True, entity=requested_entity):
            if UGLY_DEBUG: print 'SCHEMA MATCH:', (requested_entity, fields_old), validate_input_params_mapping(fields_old, final_step=True, entity=requested_entity)
            #yield (requested_entity, fields_included)
            # TODO: for now, we immediately do recursion on values mappings, but this could be separated into two steps


            if not fields_old: # to be final answer fields must be covered by keywords that represent values
                # and as currently no APIs without parameters are supported, we just skip this...
                #final_mappings.append( (probability, requested_entity, tuple(set(fields_included))) )
                pass

        # try to map values based on this
        generate_value_mappings(requested_entity, fields_old, schema_ws, values_ws, probability,
            keywords_used = keywords_used, keywords_list=keywords_list)

        # TODO: I'm still unable to validate some options because the value attributes are missing (if not specified explicitly!)
        # E.G. dataset provided but no keyword 'dataset'

        #if len(keywords_used) == len(schema_ws.items()):
        #    return

        if UGLY_DEBUG: print (requested_entity, fields_old, schema_ws, values_ws)
        return



    """ At keyword position (i) we can either:
        1) leave it out (a value, or non relevant/not mappable)
        2) take keyword i and map it to:
            a) requested entity (result type)
            b) schema entity (api input param)
            c) both

    """
    # TODO: later we may consider more complex options -- aggregation, ordering

    #
    #for keyword,schema_w  in schema_ws.items():
    #for index, keyword in enumerate(keywords_list):
    #    # so we visit every combination only once
    #    if index < keyword_index:
    #        return


    keyword = keywords_list[keyword_index]
    schema_w = schema_ws[keyword]

    #if keyword in keywords_used:
    #    #print 'exclud'
    #    pass
    #print 'keyword:', keyword


    # opt 1) do not take keyword[i]
    generate_schema_mappings(requested_entity, fields_old, schema_ws, values_ws,
        keywords_list=keywords_list, keyword_index=keyword_index + 1, probability = probability,\
        keywords_used = keywords_used)

    # opt 2) take it:
    for score, possible_mapping in schema_w:
        if possible_mapping in fields_old:
            continue

        fields_new = fields_old[:]
        fields_new.append(possible_mapping)
        #print 'validating', (f, requested_entity)

        # opt 2.a) take as api input param entity
        if validate_input_params_mapping(fields_new, entity=requested_entity):
            if UGLY_DEBUG: print 'validated', (requested_entity, fields_new)
            # | set([keyword]
            generate_schema_mappings(requested_entity, fields_new, schema_ws, values_ws,
                keywords_list=keywords_list, keyword_index=keyword_index + 1, probability = probability + score, keywords_used = keywords_used)




        # opt 2.b) take as requested entity (result type)
        if not requested_entity:
            entity_score = score
            # if this is the first keyword mapped to schema (we expect entity name to come first)
            if not keywords_used and keyword_index * 1.5 < len(keywords_list):
                entity_score *= 1.8 * (float(len(keywords_list)) - keyword_index) / len(keywords_list)

            if validate_input_params_mapping(fields_old, entity=possible_mapping):
                if UGLY_DEBUG:  print 'validated', (possible_mapping, fields_old)

                # TODO: currently the score is anyway being increased if a value is being mapped...
                generate_schema_mappings(possible_mapping, fields_old, schema_ws, values_ws,
                    keywords_list=keywords_list, keyword_index=keyword_index + 1, probability = probability + entity_score, \
                    keywords_used = keywords_used | set([keyword]))

            # opt 2.c) take both as requested entity (result type) and  input param entity
            if validate_input_params_mapping(fields_new, entity=possible_mapping):
                if UGLY_DEBUG:  print 'validated', (possible_mapping, fields_new)
                #  could this be final mapping

                # TODO: currently the score is anyway being increased if a value is being mapped...
                # as later we may need promote items mapped to operators, we may need to increase the score either here or there
                generate_schema_mappings(possible_mapping, fields_new, schema_ws, values_ws,
                    keywords_list=keywords_list, keyword_index=keyword_index + 1, probability = probability + entity_score,\
                    keywords_used = keywords_used | set([keyword]))




# TODO: we may need some extra stop condition...





def search(query, inst= None):
    """
    unit tests
    >>> search('vidmasze@cern.ch')
    user user=vidmasze@cern.ch
    """

    # TODO: add DBS instance as parameter

    DEBUG = True

    weight_matrix = []
    weight_columns = []
    schema_ws = {}
    values_ws = {}

    if DEBUG: print 'Query:', query

    # TODO: some of EN 'stopwords' may be quite important e.g. 'at', 'between', 'where'
    en_stopwords = stopwords.words('english')
    keywords = [kw.strip() for kw in query.split(' ')
                if kw.strip() and kw.strip() not in en_stopwords]
    for keyword in keywords:
        schema_ws[keyword] = keyword_schema_weights(keyword, include_fields=True)
        values_ws[keyword] = keyword_value_weights(keyword)

    print '============= Q: %s ==========' % query
    if DEBUG:
        print '============= Schema mappings (TODO) =========='
        pprint.pprint(schema_ws)
        print '=============== Values mappings (TODO) ============'
        pprint.pprint(values_ws)

    global final_mappings
    final_mappings = []
    generate_schema_mappings(None, [], schema_ws, values_ws,  keywords_list=keywords, keyword_index=0, probability = 0)

    print "============= Results for: %s ===" % query
    final_mappings = list(set(final_mappings))
    final_mappings.sort(key=lambda item: item[0], reverse=True)

    best_scores = {}

    for (score, result_type, input_params) in final_mappings:
        # short entity names
        s_result_type = entity_names[result_type]
        s_input_params = [(entity_names.get(field, field), value) for (field, value) in input_params]
        s_input_params.sort(key=lambda item: item[0])

        s_query = s_result_type + ' ' +  ' '.join(['%s=%s' % (field, value) for (field, value) in s_input_params])
        best_scores[s_query] = max(best_scores.get(s_query, 0.0), score)

        # TODO: print debuging info of how scores were composed!!!
        # print "%.2f: %s %s" % (score, result_type, ' '.join(['%s=%s' % (field, value) for (field, value) in input_params]))
        #print schema_mapping
    best_scores = best_scores.items()
    best_scores.sort(key=lambda item: item[1], reverse=True)

    print '\n'.join(['%.2f: %s' % (score, query) for (query, score) in best_scores ])

    return best_scores


def crap(query):
    keyword = query
    for keyword in query.split(' '):
        best = keyword_regexp_weights(keyword)[0]
        print best
    return '%s=%s' % (best[1][0]['key'], keyword)

    #return score_keyword(keyword)[0]


if __name__ == '__main__':
    import doctest

    # TODO: schema discovery (what are the fields not defined by the mapping)


    # we can see difflib by itself is quite bad. we must penalize mutations, insertions, etc. containment is the best
    print keyword_schema_weights('configuration') # shall be close to config
    print '! time:'
    print keyword_schema_weights('time')
    print '! location:'
    print keyword_schema_weights('location')
    # e.g. 'location of /Smf/smf/smf (dataset)
    print keyword_schema_weights('email')

    # TODO: use either presentation map, or entity.attribute
    print 'file name:', keyword_schema_weights('file name')


    #print search('vidmasze@cern.ch')
    print 'expect email:', keyword_value_weights('vidmasze@cern.ch')
    print 'expect dataset:', keyword_value_weights('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')
    print 'expect file:', keyword_value_weights('/store/backfill/1/T0TEST_532p1Run2012C_BUNNIES/DoubleMu/RAW-RECO/Zmmg-PromptSkim-v1/000/196/363/00000/DEBD64D4-A4C0-E111-A042-002618943826.root')

    # TODO: more restrictive regexp shall win
    #print search('/Aaaa/Bbbb/Cccc')

    doctest.testmod()

    # TODO: can we use word sense disambiguation
    print search('configuration of dataset /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO which location is at T1_* ')

    # TODO: semantic similarity between different parts of speech (e.g. site, located)

    #give me config of dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*
    print search('configuration /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')

    print search('configuration of /*Zmm*/*/*')

    print search('files of /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO  located at site T1_* ')

    # jobsummary  last 24h  --> jobsummary date last 24h
    # infer date

    """
    Not working queries:
    /*Zmm*/*/*
    /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO
    """

    print search('/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO')
    print search('/*Zmm*/*/*')

    # use stopwords
    # TODO: luminosity value is not mapped
    print search('files /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO   run 12345 lumi 666702')


    print search('name of vidmantas.zemleris@cern.ch')
    print search('username of vidmantas.zemleris@cern.ch')

    # statistics for Run/lumi

    # TODO: IT IS QUITE SAFE TO DISPLAY RESULTS OF A QUERY THAT JUST FINDS AN ENTITY
    # (especially if only one api param which is same as the result)

    # TODO: allow combining keyword query and structured query


    # TODO: we may wish to be able to interpret the semantics behind the dataset...
    """
    the only numbers in preconditions are lumi and run

    old the others are post-conditions...
    """