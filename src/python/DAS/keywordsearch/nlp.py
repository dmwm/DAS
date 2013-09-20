__author__ = 'vidma'



from nltk import stem
from nltk.stem.wordnet import WordNetLemmatizer

from DAS.keywordsearch.config import  processed_stopwords

stemmer = stem.PorterStemmer()
lmtzr = WordNetLemmatizer()


from nltk.corpus import stopwords
en_stopwords = stopwords.words('english')

create_dict = lambda l: dict((v, v) for v in l)

from DAS.keywordsearch.config import mod_enabled
from DAS.keywordsearch.entity_matchers.string_dist_levenstein import levenshtein_normalized

from DAS.keywordsearch.utils import memo

lemmatize = memo(lmtzr.lemmatize)
lemmatize.__doc__ = "cached version of lmtzr.lemmatize"

getstem = memo(stemmer.stem)
getstem.__doc__ = "cached version of PorterStemmer() stem"

# load the lemmatization DB now
lemmatize("dataset")

# shall be slightly faster...
en_stopwords_dict = create_dict(en_stopwords)
processed_stopwords_dict = create_dict(processed_stopwords)
def filter_stopwords(kwd_list):
    return filter(lambda k: k not in en_stopwords_dict \
                            or k in processed_stopwords_dict, kwd_list)

def string_distance(keyword, match_to, semantic=False, allow_low_scores= False):
    """
    Basic string-edit distance metrics do not perform well, it either introduces too many
    false positives (file as site), or do not recognize fairly similar word combinations, such as
    config vs configuration.

    Therefore, to minimize the false positives (which have direct effect to ranking),
    we use a combination of more trustful metrics with decreasing score:
    * full match
    * lemma match (e.g. only the word number differs)
    * stem match
    * stem match only within a small edit distance (returning a low usable score)
        1-2 characters, maximum 1 mutation
    """

    if keyword == match_to:
        return 1.0

    # TODO: lemmatizer takes part of speech
    lemma = lemmatize(keyword)
    lemma2 = lemmatize(match_to)
    if lemma == lemma2:
        return 0.9

    # TODO: similarity shall not be used at all if the words are not similar enough

    if mod_enabled('STRING_DIST_ENABLE_NLTK_STEM'):
        kwd_stem = getstem(keyword)
        match_stem = getstem(match_to)

        if kwd_stem == match_stem:
            return 0.7
            # TODO: shall we do string-distance on top of stemmer?
    else:
        kwd_stem = keyword
        match_stem = match_to

    score = 0.7 * levenshtein_normalized(kwd_stem, match_stem, subcost=2, maxcost=3)

    if allow_low_scores:
        return score if score > 0.1 else  0.0
    else:
        return score if score > 0.35 else  0.0