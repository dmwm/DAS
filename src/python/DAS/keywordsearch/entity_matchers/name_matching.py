#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
This module generates candidates for matching keywords into entity names.
"""
from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
from DAS.keywordsearch.nlp import string_distance


def keyword_schema_weights(keyword, kwd_idx=-1):
    """
    for each schema term (entity, entity attribute) calculates likelihood for
    the keyword to match it
    """
    entities = get_schema().entity_names.items()
    result = [(string_distance(keyword, entity_short), entity_long)
              for entity_long, entity_short in entities]

    # check synonyms
    entity_synonyms = get_schema().cms_synonyms['daskeys'].items()
    for entity_long, synonyms in entity_synonyms:
        for synonym in synonyms:
            result.extend([(string_distance(keyword, synonym), entity_long), ])

    # apply some simple patterns
    if kwd_idx == 0:
        if keyword == 'where':
            result.extend([(0.75, 'site.name'), ])
        if keyword == 'who':
            result.extend([(0.5, 'user.name'), ])

    result = [item for item in result
              if item[0] > 0]
    result.sort(key=lambda item: item[0], reverse=True)
    return result
