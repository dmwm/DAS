
"""
This module generates candidates for matching keywords into entity names, attributes or values.
It uses a number of similarity metrics and heuristics, along with exiting values.
"""

# for handling semantic and string similarities

from DAS.keywordsearch.metadata.das_schema_adapter import cms_synonyms, entity_names
from DAS.keywordsearch.nlp import string_distance

def keyword_schema_weights(keyword, keyword_index=-1,):
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

    # TODO: this weight shall be lower than the one below?
    >>> keyword_schema_weights('email')[0][1] # user contains email field
    u'user'
    >>> keyword_schema_weights('email', include_fields=True)[0][1] # user contains email field
    u'user.email'
    """

    # TODO: use fields to map to entities
    # TODO: operators
    # TODO: use IDF (some field subparts are very common, e.g. name)

    result =  [(string_distance(keyword, entity_short, semantic=True), entity_long)
               for (entity_long, entity_short) in entity_names.items()]

    # check synonyms
    for (entity_long, synonyms) in cms_synonyms['daskeys'].items():
        for synonym in synonyms:
            result.extend([ (string_distance(keyword, synonym, semantic=True), entity_long)   ])

    # apply some simple patterns
    if keyword_index == 0:
        # TODO: we actually know even more: this is the result type
        if keyword == 'where':
            result.extend( [(0.75, 'site.name'), ])
        if keyword == 'who':
            result.extend( [(0.5, 'user.name'),])

        #if keyword == 'when':
        #    result.extend( [(0.5, 'date'),])
        # TODO: how many [number]
        # TODO: how large/big/... [size (result attribute)]
        # when -- date


    result = filter(lambda item: item[0] > 0, result)

    result.sort(key=lambda item: item[0], reverse=True)
    return result

