#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
provide basic natural language processing functionality:
* filtering stopwords
* stemming
* lemmatization
* custom string similarity metric based on strig-edit distance
"""
from nltk import stem
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords

from DAS.keywordsearch.config import USEFUL_STOPWORDS
from DAS.keywordsearch.config import get_setting
from DAS.keywordsearch.entity_matchers.string_dist_levenstein \
    import levenshtein_normalized as levenshtein_norm
from DAS.keywordsearch.utils import memo

STEMMER = stem.PorterStemmer()
LMTZR = WordNetLemmatizer()
EN_STOPWORDS = stopwords.words('english')
EN_STOPWORDS_SET = set(EN_STOPWORDS)
USEFUL_STOPWORDS_SET = set(USEFUL_STOPWORDS)

lemmatize = memo(LMTZR.lemmatize)
lemmatize.__doc__ = "cached version of lmtzr.lemmatize"

getstem = memo(STEMMER.stem)
getstem.__doc__ = "cached version of PorterStemmer() stem"

# load the lemmatization DB now
lemmatize("dataset")


def filter_stopwords(kwd_list):
    """ filter out useless stopwords from given keyword list
    (this leaves out the useful ones, e.g. where, when...)
     which are defined in USEFUL_STOPWORDS."""
    return [k for k in kwd_list
            if k not in EN_STOPWORDS_SET or k in USEFUL_STOPWORDS_SET]


def string_distance(keyword, match_to, allow_low_scores=False):
    """
    Basic string-edit distance metrics do not perform well,
    they either introduce too many false positives (file as site), or do not
    recognize fairly similar words, such as 'config' vs 'configuration'.

    Therefore, to minimize the false positives (which have direct effect
    to ranking), we use a combination of more trustful metrics
    listed in the order decreasing score:
    * full match
    * lemma match (e.g. only the word number differs)
    * stem match
    * stem match within a small edit distance (returning a low usable score)
        e.g. 1-2 characters differing, maximum 1 mutation
    """

    if keyword == match_to:
        return 1.0

    lemma = lemmatize(keyword)
    lemma2 = lemmatize(match_to)
    if lemma == lemma2:
        return 0.9

    if get_setting('STRING_DIST_ENABLE_NLTK_STEM'):
        kwd_stem = getstem(keyword)
        match_stem = getstem(match_to)

        if kwd_stem == match_stem:
            return 0.7
    else:
        kwd_stem = keyword
        match_stem = match_to

    score = 0.7 * levenshtein_norm(kwd_stem, match_stem, subcost=2, maxcost=3)

    if allow_low_scores:
        return score if score > 0.1 else 0.0
    else:
        return score if score > 0.35 else 0.0
