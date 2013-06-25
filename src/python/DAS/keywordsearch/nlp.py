__author__ = 'vidma'



from nltk import stem
from nltk.stem.wordnet import WordNetLemmatizer

from DAS.keywordsearch.config import  processed_stopwords, DEBUG

stemmer = stem.PorterStemmer()
lmtzr = WordNetLemmatizer()


from nltk.corpus import stopwords
en_stopwords = stopwords.words('english')


# for handling semantic and string similarities
from nltk.corpus import wordnet

from DAS.keywordsearch.metadata.das_schema_adapter import *

from DAS.keywordsearch.config import mod_enabled

from DAS.keywordsearch.entity_matchers.string_dist_levenstein import levenshtein_normalized

# TODO: use mapping to entity attributes even independent of the entity itself (idf-like inverted index)




def filter_stopwords(kwd_list):
    return filter(lambda k: k not in en_stopwords or k in processed_stopwords, kwd_list)



def semantic_dist(keyword, match_to, score):
    # TODO: redo this or throw away
    ks = wordnet.synsets(keyword)
    # TODO: we shall can select the relevant synsets for our schema entities manually for improved results
    if entity_wordnet_synsets.has_key(match_to):
        ms = [entity_wordnet_synsets[match_to]]
        #else:
        #ms = wordnet.synsets(match_to)
        if ms and ks:
            avg = lambda l: sum(l) / len(l)

            if DEBUG and keyword == 'location':
                print 'location similarities to ', match_to, [
                    '%.2f' % k.wup_similarity(m) for k in ks for m in ms if
                    k.wup_similarity(m)]
            similarities = [k.wup_similarity(m) for k in ks for m in ms if
                            k.wup_similarity(m)]
            semantic_score = similarities and max(similarities) or 0.0

            if score < 0.7:
                score = semantic_score
            else:
                score = max(semantic_score, (score + 2 * semantic_score) / 3)
    return score


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
    # TODO: use some good string similarity metrics: string edit distance, jacard, levenshtein, hamming, etc
    # TODO: use ontology

    if keyword == match_to:
        return 1.0

    # TODO: add specific terms: dataset

    # TODO: lemmatizer takes part of speech
    lemma = lmtzr.lemmatize(keyword)
    lemma2 = lmtzr.lemmatize(match_to)
    if lemma == lemma2:
        return 0.9

    # TODO: similarity shall not be used at all if the words are not similar enough

    if mod_enabled('STRING_DIST_ENABLE_NLTK_STEM'):
        kwd_stem = stemmer.stem(keyword)
        match_stem = stemmer.stem(match_to)

        if kwd_stem == match_stem:
            return 0.7
            # TODO: shall we do string-distance on top of stemmer?
    else:
        kwd_stem = keyword
        match_stem = match_to

    score = 0.7 * levenshtein_normalized(kwd_stem, match_stem, subcost=2, maxcost=3)


    # TODO: we shall be able to handle attributes also
    if mod_enabled('STRING_DIST_ENABLE_NLTK_SEMANTICS') and semantic and not '.' in match_to:
        score = semantic_dist(keyword, match_to, score)

    if allow_low_scores:
        return score if score > 0.1 else  0
    else:
        return score if score > 0.35 else  0




if __name__ == '__main__':
    string_distance('confguration', 'config', semantic=True)