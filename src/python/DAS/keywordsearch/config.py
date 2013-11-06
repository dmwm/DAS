# coding=utf-8
"""
The file stores Keyword Search specific constants
which do not have to be changed often.
"""
DEBUG = False
# important debug, generating minimal output
MINIMAL_DEBUG = 0
USEFUL_STOPWORDS = ['where', 'when', 'who']
USE_LOG_PROBABILITIES = True
USE_IR_SCORE_SMOOTHING = False

# find only K-best results for input query (K=0 for unlimited)
K_RESULTS_TO_STORE = 100
# max length of a value to be displayed without shortening it down in web UI
UI_MAX_DISPLAYED_VALUE_LEN = 26

# probabilities of (not) taking keywords -- i.e. matching to nothing
P_NOT_TAKEN = 0.3
P_NOT_TAKEN_STOPWORD = 0.9
P_NOT_SPECIFIED_RES_TYPE = 0.5  # e.g. keyword with score 0.5 would take over...

#if USE_LOG_PROBABILITIES:
#    # logarithm of zero or negative shall be very large
#    logP = lambda score: score > 0.0 and log(score) or \
#                         (score < 0) and score or 0.0
#                         # (score < -1) and -log(-score) or  0.0


# -----------------------
# Entry point generation - fields in outputs
# -----------------------
# weight increase for phrases in multi-word matching
W_PHRASE = 1.5
# maximum number of IR results to fetch
CHUNK_N_PHRASE_RESULTS = 20
CHUNK_N_TOKEN_COMBINATION_RESULTS = 10
# max len of tokens to consider as a sequence
# (e.g. number of events --> "number of events")
MAX_TOKEN_COMBINATION_LEN = 4
# just dividing by the best score, may introduce some unnecessarily high scores
# however the other normalization method do not work much better
USE_IR_SCORE_NORMALIZATION_LOCAL = False

# whether to exclude records from entity fields which contained some errors
# that could be just a specific service which is not available...
EXCLUDE_RECORDS_WITH_ERRORS = False

KWS_SETTINGS = {
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

    # threshold or false
    # this would prune out all the results earlier (based on partial matching)
    #  even if later some heuristic may increase the score
    # results would be not 100% best, but shall be returned faster...
    'PRUNE_SCORES_LESS_THAN_WORST': -0.3
}


def get_setting(name):
    """ gets a KWS setting """
    return KWS_SETTINGS.get(name, False)
