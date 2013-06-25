#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
__author__ = 'vidma'

import pprint
import os

from whoosh import analysis
from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import *
from whoosh.query import *
from whoosh.analysis import *
import whoosh.scoring


INDEX_DIR = os.environ['DAS_KWS_IR_INDEX']

_DEBUG= False

_USE_FAKE_FIELDS = False # this also impact IDFs!!!
# if not, include only processed data!

def build_index(fields_by_entity, remove_old = False):
    """
    Schema:

    entity_results:
        entity
        field name ?
        field title (optional)

    values:
        entity

        [optionally it could be dependent on specific api?]
    """

    # _result_fields_by_entity[result_entity].get(field, {'title': ''})['title']

    print 'starting to build the index...'
    # TODO: entity names are not so much informative here?

    # remove old index. TODO: is there a better way?
    if remove_old:
        import shutil, os
        shutil.rmtree(os.path.join(INDEX_DIR, '*'))

    # replica_fraction -> replica fraction
    # TODO: store entity as we are filtering by it?
    if _USE_FAKE_FIELDS:
        schema = Schema(fieldname=TEXT(stored=True, field_boost=3),
                        fieldname_current=TEXT(stored=True, field_boost=1),

                        fieldname_processed_parents=TEXT(stored=True,
                                                         field_boost=0.2,
                                                         analyzer=analysis.KeywordAnalyzer(
                                                             lowercase=True), ),
                        fieldname_processed_current=TEXT(stored=True,
                                                         field_boost=1.0,
                                                         analyzer=analysis.KeywordAnalyzer(
                                                             lowercase=True), ),
                        # TODO: to keep stopwords somewhere?
                        title=TEXT(stored=True, field_boost=0.7),
                        title_stemmed=TEXT(analyzer=analysis.StemmingAnalyzer(),
                                           field_boost=1.5),
                        # KeywordAnalyzer leaves text as it was in the beginning
                        title_exact=TEXT(
                            analyzer=analysis.KeywordAnalyzer(lowercase=True),
                            field_boost=1),

                        entity_and_fn=ID,
                        result_type=KEYWORD(stored=True))
    else:
        schema = Schema(fieldname=TEXT(stored=True, field_boost=3),
                        # fieldname_current=TEXT(stored=True, field_boost=1),

                        fieldname_processed_parents=TEXT(stored=True,
                                                         field_boost=0.2,
                                                         analyzer=analysis.KeywordAnalyzer(
                                                             lowercase=True), ),
                        fieldname_processed_current=TEXT(stored=True,
                                                         field_boost=1.0,
                                                         analyzer=analysis.KeywordAnalyzer(
                                                             lowercase=True), ),
                        # TODO: to keep stopwords somewhere?
                        #title=TEXT(stored=True, field_boost=0.7),
                        title_stemmed=TEXT(analyzer=analysis.StemmingAnalyzer(),
                                           field_boost=1.5),
                        # KeywordAnalyzer leaves text as it was in the beginning
                        #title_exact=TEXT(
                        #    analyzer=analysis.KeywordAnalyzer(lowercase=True),
                        #    field_boost=1),

                        entity_and_fn=ID,
                        result_type=KEYWORD(stored=True))

    idx = create_in(INDEX_DIR, schema, "idx_name")

    writer = idx.writer()


    print 'initializing service fields index'
    for result_type, fields in fields_by_entity.items():
        for field_name, field in fields.items():
            # Whoosh don't except '' as values, so we use 'or None'

            writer.delete_by_term('entity_and_fn',
                                  result_type + ':' + field_name)

            if _USE_FAKE_FIELDS:
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
            else:
                writer.add_document(
                    fieldname=field_name,
                    #fieldname_current=field_name.split('.')[-1],
                    # e.g. block.replica (for block.replica.custodial)
                    fieldname_processed_parents=' '.join(
                        field_name.split('.')[:-1]).replace('_', ' ') or None,

                    # e.g. .custodial (for block.replica.custodial)
                    fieldname_processed_current=field_name.split('.')[-1].replace(
                        '_', ' '),
                    # title that is not always present...
                    #title=field['title'] or None,
                    title_stemmed=field['title'] or None,
                    #title_exact=field['title'] or None,
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


def search_index(keywords, result_type=False, full_matches_only=False, limit=10,
                 use_bm25f=True,):
    global _ix

    if _DEBUG:
        print 'Evaluating query: ', keywords

    weighting=whoosh.scoring.TF_IDF()
    #weighting=whoosh.scoring.PL2()

    if use_bm25f:
        weighting = whoosh.scoring.BM25F()
    with _ix.searcher(weighting=weighting) as s:

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
        if _USE_FAKE_FIELDS:
            fields_to_search = ['fieldname', 'fieldname_processed_parents',
                                'fieldname_processed_current', 'fieldname_current',
                                'title',
                                ]

            all_fields_and_terms = [Term(f, kw) for kw in
                                    keyword_list_no_stopw
                                    for f in fields_to_search
            ]
            all_fields_and_terms.extend([Term('title_stemmed', kw)
                                         for kw in keyword_list_stemmed])



            if keyword_list_no_stopw and len(keyword_list_no_stopw) > 1:
                # noinspection PyTypeChecker
                all_fields_and_terms.append(
                    Phrase('title', keyword_list_no_stopw, boost=2,
                           slop=1))
        else:
            fields_to_search = ['fieldname', 'fieldname_processed_parents',
                                'fieldname_processed_current',
                                ]

            all_fields_and_terms = [Term(f, kw) for kw in
                                    keyword_list_stemmed
                                    for f in fields_to_search
            ]
            all_fields_and_terms.extend([Term('title_stemmed', kw)
                                         for kw in keyword_list_stemmed])

            # WE no not search by stopwords anymore....
            # here phrase also uses title_stemmed, it will match title stemmed, so score boost shouldn't be too high
            if keyword_list_no_stopw and len(keyword_list_no_stopw) > 1:
                # noinspection PyTypeChecker
                all_fields_and_terms.append(
                    Phrase('title_stemmed', keyword_list_stemmed, boost=1.5,
                           slop=1))


            # TODO: the problem is that exact matching has to match the whole phrase, overwise there's not so much of use
            # e.g. 'number events' and 'number of' gets the same scores,
            # while 'number events' is much more informative
            #  (so at least it is scoring more than over results)

            # TODO: problem is that stopwords match too much ('file in|of'-> file size, replica fraction,
            # and it seems they're not accounted in the current default ranking
            # so we have to either change the cost model or allow only
            # full matching (not sure if possible in whoosh)

            #all_fields_and_terms.append(
            #    Phrase('title_exact', keyword_list, boost=2, slop=1))

            # TODO: an easy way to check for full (complete) match is to count the unique matched terms.

        # TODO: ADD FUZZY SEARCH

        if not all_fields_and_terms:
            return []

        q = Or(all_fields_and_terms)

        if result_type:
            q = And([Term('result_type', result_type),
                     Or(all_fields_and_terms)])

        if _DEBUG:
            print 'Q:'
            pprint.pprint(q)

        hits = s.search(q, terms=True, optimize=True, limit=limit)


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

        def get_matched_keywords(hit):
            # which keywords have been matched
            f_kw_stemmed = lambda kw: _text(stemmer(stopword_filter(
                lower_filter(tokenizer(kw)))))
            f_kw_no_stopword = lambda kw: _text(stopword_filter(
                lower_filter(tokenizer(kw))))

            keyword_list_no_stopw = _text(stopword_filter(
                lower_filter(tokenizer(keywords))))

            terms_matched = set([val for (field, val) in hit.matched_terms() ])

            if _DEBUG:
                print 'matched_terms_list', terms_matched

            matched_kws = set()
            for kw in keyword_list_no_stopw:
                keyword_stemmed = f_kw_stemmed(kw)[0]

                if _DEBUG:
                    print 'kw', kw, 'stemmed:', keyword_stemmed, 'kw no stopw', f_kw_no_stopword(kw)

                if keyword_stemmed in terms_matched or kw in terms_matched:
                    matched_kws |= set([kw])

            return matched_kws



        # easiest is just to require everything, but TODO: what is relevant term is somewhere really far in the query!!?
        results =  [{ 'field': hit['fieldname'],
                  'result_type': hit['result_type'],
                   # 'len': len(remove_stopwords(f['title']).split(' ')),
                   'score': hit.score,
                   # TODO: shall we divide by variance or stddev?
                   'keywords_matched': get_matched_keywords(hit),
                   'hit_matched_terms': hit.matched_terms(),
                   'fieldname_matched': [1 for (field, val) in  hit.matched_terms()
                                        if field == 'fieldname'],

                   } for hit in hits ]

        if _DEBUG:
            pprint.pprint(results)
        return results



def not_used_manual_tests():
    from DAS.keywordsearch.metadata import das_schema_adapter
    from DAS.core.das_core import DASCore

    dascore = DASCore()
    das_schema_adapter.init(dascore)
    fields_by_entity = das_schema_adapter.list_result_fields()
    build_index(fields_by_entity)
    load_index()

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



    print '============================================'
    print 'Q: file'
    # TODO: maybe it's not a good idea constraining on result type?
    pprint.pprint(
        search_index(keywords=u'file in', result_type='file', limit=4))



    print '============================================'
    print 'Q: file in'
    # TODO: maybe it's not a good idea constraining on result type?
    pprint.pprint(
        search_index(keywords=u'file in', result_type='file', limit=4))



if __name__ == '__main__':
    not_used_manual_tests()