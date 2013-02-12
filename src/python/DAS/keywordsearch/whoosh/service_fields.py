#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
__author__ = 'vidma'

import pprint

from whoosh import analysis
from whoosh.index import create_in
from whoosh.index import open_dir

from whoosh.fields import *
from whoosh.query import *
from whoosh.analysis import *

from DAS.keywordsearch.das_schema_adapter import *


INDEX_DIR = '/home/vidma/Desktop/DAS/DAS_code/DAS/src/python/DAS/keywordsearch/whoosh/idx'

_DEBUG=False

def build_index():
    '''
    Schema:

    entity_results:
        entity
        field name ?
        field title (optional)

    values:
        entity

        [optionally it could be dependent on specific api?]
    '''
    fields_by_entity = list_result_fields()
    # _result_fields_by_entity[result_entity].get(field, {'title': ''})['title']

    print 'starting to build the index...'
    # TODO: entity names are not so much informative here?

    # replica_fraction -> replica fraction
    # TODO: store entity as we are filtering by it?
    schema = Schema(fieldname=TEXT(stored=True, field_boost=3),
                    fieldname_current=TEXT(stored=True, field_boost=2.0),

                    fieldname_processed_parents=TEXT(stored=True,
                                                     field_boost=0.2,
                                                     analyzer=analysis.KeywordAnalyzer(
                                                         lowercase=True), ),
                    fieldname_processed_current=TEXT(stored=True,
                                                     field_boost=1.0,
                                                     analyzer=analysis.KeywordAnalyzer(
                                                         lowercase=True), ),
                    # TODO: to keep stopwords somewhere?
                    title=TEXT(stored=True, ),
                    title_stemmed=TEXT(analyzer=analysis.StemmingAnalyzer(),
                                       field_boost=0.5),
                    # KeywordAnalyzer leaves text as it was in the beginning
                    title_exact=TEXT(
                        analyzer=analysis.KeywordAnalyzer(lowercase=True),
                        field_boost=0.5),

                    entity_and_fn=ID,
                    result_type=KEYWORD)
    idx = create_in(INDEX_DIR, schema, "idx_name")

    writer = idx.writer()

    print 'initializing service fields index'
    for result_type, fields in fields_by_entity.items():
        for field_name, field in fields.items():
            # Whoosh don't except '' as values, so we use 'or None'
            writer.add_document(
                fieldname=field_name,
                fieldname_current=field_name.split('.')[-1],
                # e.g. block.replica (for block.replica.custodial)
                fieldname_processed_parents=' '.join(
                    field_name.split('.')[:-1]).replace('_', ' ') or None,

                # e.g. .custodial (for block.replica.custodial)
                fieldname_processed_current=field_name.split('.')[-1].replace(
                    '_', ' '),
                # title that is not always present...
                title=field['title'] or None,
                title_stemmed=field['title'] or None,
                title_exact=field['title'] or None,
                # TODO: porter stemmer, or other processing
                entity_and_fn=result_type + ':' + field_name,
                result_type=result_type)

    writer.commit()
    print 'index creation done.'


_ix = None


def load_index():
    global _ix
    _ix = open_dir(INDEX_DIR, "idx_name")
    return _ix


def search_index(keywords, result_type, full_matches_only=False):
    global _ix

    if _DEBUG:
        print 'Evaluating query: ', keywords
    with _ix.searcher() as s:

        # Title do not contain stop words, so use a filter
        _text = lambda l: [t.text for t in l]
        tokenizer = SpaceSeparatedTokenizer()
        stopword_filter = StopFilter()
        lower_filter = LowercaseFilter()
        stemmer = StemFilter()

        keyword_list = _text(lower_filter(tokenizer(keywords)))
        keyword_list_no_stopw = _text(stopword_filter(
            lower_filter(tokenizer(keywords))))
        keyword_list_stemmed = _text(stemmer(stopword_filter(
            lower_filter(tokenizer(keywords)))))


        # build the terms to search
        fields_to_search = ['fieldname', 'fieldname_processed_parents',
                            'fieldname_processed_current', 'fieldname_current',
                            'title', ]
        all_fields_and_terms = [Term(f, kw) for kw in
                                keyword_list_no_stopw
                                for f in fields_to_search
        ]
        all_fields_and_terms.extend([Term('title_stemmed', kw)
                                     for kw in keyword_list_stemmed])

        # TODO: Is Stemmer needed here manually ?

        if keyword_list_no_stopw:
            all_fields_and_terms.append(
                Phrase('title', keyword_list_no_stopw, boost=3,
                       slop=1))

            # TODO: the problem is that exact matching has to match the whole phrase, overwise there's not so much of use
            # e.g. 'number events' and 'number of' gets the same scores,
            # while 'number events' is much more informative
            #  (so at least it is scoring more than over results)
            # TODO: so now, the question is shall we tokenize it or not!?
            all_fields_and_terms.append(
                Phrase('title_exact', keyword_list, boost=2, slop=1))

            # TODO: an easy way to check for full (complete) match is to count the unique matched terms.

        # TODO: ADD FUZZY SEARCH

        if not all_fields_and_terms:
            return []

        q = And([Term('result_type', result_type),
                 Or(all_fields_and_terms)])

        if _DEBUG:
            print 'Q:'
            pprint.pprint(q)

        hits = s.search(q, terms=True, optimize=False, )

        if _DEBUG:
            print 'OR KWS RESULTS:'

            pprint.pprint(
                [{'result': hit['fieldname'], 'rank': hit.rank,
                  'score': hit.score,
                  'keywords_matched': hit.matched_terms(),
                  } for hit in hits])

        if False:
            for i in range(1, 10):
                # TODO: join fuzzy terms...

                hits = s.search(
                    FuzzyTerm('title', keywords, maxdist=i, prefixlength=0))
                print "Pass: %s" % i
                if len(hits) > 0:
                    for hit in hits:
                        pprint.pprint(
                            (hit, hit.rank, hit.score, hit.matched_terms()))
                        # myquery = And([Term("content", u"apple"), Term("content", "bear")])
        # TODO: problem it gives either stemmed or actual
        #filter_matched_terms = lambda (term, val): not term == 'result_type'

        # easiest is just to require everything, but TODO: what is relevant term is somewhere really far in the query!!?
        return [{ 'field': hit['fieldname'],
                   # 'len': len(remove_stopwords(f['title']).split(' ')),
                   'score': hit.score,
                   # TODO: shall we divide by variance or stddev?
                   'keywords_matched': hit.matched_terms(),
                   'fieldname_matched': [1 for (field, val) in  hit.matched_terms()
                                        if field == 'fieldname'],

                   } for hit in hits ]


#build_index()
if __name__ == '__main__':
    build_index()
    load_index()

    # TODO: Phrase search

    if False:
        search_index(
            keywords=u'files of Zmm with number of events more than 10',
            result_type=u'dataset')
        search_index(keywords=u'number events', result_type=u'dataset')

        search_index(keywords=u'number evented', result_type=u'dataset')

        search_index(keywords=u'dataset.nevents', result_type=u'dataset')
        search_index(keywords=u'dataset.numevents', result_type=u'dataset')


        # TODO: block.replica.subscribed vs block.replica.custodial (the deepest one is the most important)
        search_index(keywords=u'replica fraction', result_type=u'block')
        search_index(keywords=u'replica fraction', result_type=u'site')

        search_index(keywords=u'custodial replica', result_type=u'block')

        search_index(keywords=u'replica_fraction', result_type=u'site')

        print '======================================================================='

        search_index(keywords=u'number', result_type=u'dataset')
        search_index(keywords=u'of', result_type=u'dataset')
        search_index(keywords=u'events', result_type=u'dataset')
        search_index(keywords=u'number of', result_type=u'dataset')
        search_index(keywords=u'of events', result_type=u'dataset')
        search_index(keywords=u'Number OF Events', result_type=u'dataset')

    print 'Q: dataset_fraction'
    pprint.pprint(
        search_index(keywords=u'dataset_fraction', result_type=u'site'))
    print 'Q: dataset fraction'
    pprint.pprint(
        search_index(keywords=u'dataset fraction', result_type=u'site'))

    # TODO: do we support semantic relations??!!
    print 'Q: dataset part'
    pprint.pprint(
        search_index(keywords=u'dataset part', result_type=u'site'))

    #  lumi flag in run 176304