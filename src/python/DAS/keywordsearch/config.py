__author__ = 'vidma'



DEBUG = False


processed_stopwords = ['where', 'when', 'who']


from nltk import stem
stemmer = stem.PorterStemmer()

from math import log
USE_LOG_PROBABILITIES = True

USE_IR_SCORE_SMOOTHING = False

# just divide by the best score, may introduce some unnecessarily high scores...
USE_IR_SCORE_NORMALIZATION_LOCAL = False

# find only K-best results for input query (K=0 for unlimited)
K_RESULTS_TO_STORE = 20
#K_RESULTS_TO_STORE = 0


logP = lambda score: score

# TODO: probablity of not taking a keyword shall depend on: stopword? known schema entity?
# TODO: another issue! complex queries that system can not solve...!
P_NOT_TAKEN = 0.3
P_NOT_TAKEN_STOPWORD = 0.9
P_NOT_SPECIFIED_RES_TYPE = 0.5 # e.g. keyword with score 0.5 would take over...

if USE_LOG_PROBABILITIES:
    # logarithm of zero or negative shall be very large
    logP = lambda score: score > 0.0 and log(score) or \
                          (score < 0) and score or 0.0
                         # (score < -1) and -log(-score) or  0.0



# whether to exclude records from entity fields which contained some errors
# that could be just a specific service which is not available...
EXCLUDE_RECORDS_WITH_ERRORS = False


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
