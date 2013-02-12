__author__ = 'vidma'


ENABLED_MODULES = {
    'STRING_DIST_ENABLE_NLTK_SEMANTICS': False,
    'STRING_DIST_ENABLE_NLTK_PORTER': True,

    'DOWNRANK_TERMS_REFERRING_TO_SCHEMA': True,
}


def mod_enabled(name):
    return ENABLED_MODULES.get(name, False)
