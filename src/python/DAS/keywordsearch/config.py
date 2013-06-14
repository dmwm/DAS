__author__ = 'vidma'


ENABLED_MODULES = {
    'STRING_DIST_ENABLE_NLTK_SEMANTICS': False,
    'STRING_DIST_ENABLE_NLTK_STEM': True,

    'DOWNRANK_TERMS_REFERRING_TO_SCHEMA': True,

    #  fields in results (or values if specified field_name=smf,
    # e.g. number of events > 10

    'SERVICE_RESULT_FIELDS': 1,
    'SERVICE_RESULT_FIELDS_WITH_OPERATOR_AS_VALUES': 1,

    'RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS': 0.1,

    # threshold or false
    'PRUNE_NEGATIVE_SCORES': -0.5,
}


def mod_enabled(name):
    return ENABLED_MODULES.get(name, False)
