#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103
# pylint disabled: C0103 - we use short names
"""
this is the IR-based ranker used in matching keywords into names of
fields in service outputs. These may be composed of unclean,technical terms.
"""
import pprint
import os
import shutil
from itertools import chain

from whoosh import analysis
from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import TEXT, ID, KEYWORD, Schema
from whoosh.query import Term, Phrase, Or, And
from whoosh.analysis import SpaceSeparatedTokenizer, StopFilter, \
    LowercaseFilter, StemFilter
import whoosh.scoring

from DAS.keywordsearch.config import DEBUG as _DEBUG


INDEX_DIR = os.environ['DAS_KWS_IR_INDEX']
_USE_FAKE_FIELDS = False  # this also impact IDFs!!!
# if not, include only processed data!
_ix = None


def create_idx_with_redundant_fields(field_list):
    """
    creates a document in IR engine for each field in the
    service outputs (e.g. dataset.tag, weather.temperature)
    This include creating fields in this document for:
     * title (exact, without stopwords and stemmed each as separate fields)
     * parental information (e.g. dataset for dataset.tag)
    """
    # remove the old index, if any
    shutil.rmtree(os.path.join(INDEX_DIR, '*'), ignore_errors=True)
    schema = Schema(fieldname=TEXT(stored=True, field_boost=3),
                    fieldname_current=TEXT(stored=True, field_boost=1),
                    fieldname_processed_parents=TEXT(
                        stored=True,
                        field_boost=0.2,
                        analyzer=analysis.KeywordAnalyzer(lowercase=True)),
                    fieldname_processed_current=TEXT(
                        stored=True,
                        field_boost=1.0,
                        analyzer=analysis.KeywordAnalyzer(lowercase=True)),
                    title=TEXT(stored=True, field_boost=0.7),
                    title_stemmed=TEXT(analyzer=analysis.StemmingAnalyzer(),
                                       field_boost=1.5),
                    # KeywordAnalyzer leaves text as it was in the beginning
                    title_exact=TEXT(
                        analyzer=analysis.KeywordAnalyzer(lowercase=True),
                        field_boost=1),
                    entity_and_fn=ID,
                    result_type=KEYWORD(stored=True))

    idx = create_in(INDEX_DIR, schema, "idx_name")
    idx_writer = idx.writer()
    for field_data in field_list:
        # create an IR "document" for each field
        field_name = field_data['name']
        lookup = field_data['lookup']  # TODO: will this work?!!!
        doc_id = lookup + ':' + field_name
        parents = field_name.replace('_', ' ').split('.')[:-1]

        idx_writer.delete_by_term('entity_and_fn', doc_id)  # remove the old doc
        # P.S. Whoosh don't except '' as values, so we need 'or None'
        idx_writer.add_document(
            fieldname=field_name,
            fieldname_current=field_name.split('.')[-1],
            # e.g. block.replica (for block.replica.custodial)
            fieldname_processed_parents=' '.join(parents) or None,
            # e.g. .custodial (for block.replica.custodial)
            fieldname_processed_current=
            field_name.split('.')[-1].replace('_', ' '),
            # title that is not always present...
            title=field_data['title'] or None,
            title_stemmed=field_data['title'] or None,
            title_exact=field_data['title'] or None,
            entity_and_fn=doc_id,
            result_type=lookup)
    idx_writer.commit()


def create_idx_wo_redundant_fields(field_list):
    """
    creates a document in IR engine to represent a single field in the
    service outputs (e.g. dataset.tag, weather.temperature)

    This include creating fields in this document for:
     * title (exact, without stopwords and stemmed)
     * parental information (e.g. dataset for dataset.tag)
     * fields main technical name
    """
    schema = Schema(fieldname=TEXT(stored=True, field_boost=3),
                    # fieldname_current=TEXT(stored=True, field_boost=1),
                    fieldname_processed_parents=TEXT(
                        stored=True,
                        field_boost=0.2,
                        analyzer=analysis.KeywordAnalyzer(lowercase=True)),
                    fieldname_processed_current=TEXT(
                        stored=True,
                        field_boost=1.0,
                        analyzer=analysis.KeywordAnalyzer(lowercase=True)),
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
    idx_writer = idx.writer()
    for field_data in field_list:
        # create an IR "document" for each field
        lookup = field_data['lookup']  # TODO: will this work?!!!
        field_name = field_data['name']
        doc_id = lookup + ':' + field_name
        parents = field_name.replace('_', ' ').split('.')[:-1]

        idx_writer.delete_by_term('entity_and_fn', doc_id)  # remove the old doc
        # P.S. Whoosh don't except '' as values, so we need 'or None'
        idx_writer.add_document(
            fieldname=field_name,
            #fieldname_current=field_name.split('.')[-1],
            # e.g. block.replica (for block.replica.custodial)
            fieldname_processed_parents=' '.join(parents) or None,
            # e.g. .custodial (for block.replica.custodial)
            fieldname_processed_current=
            field_name.split('.')[-1].replace('_', ' '),
            # title that is not always present...
            #title=field['title'] or None,
            title_stemmed=field_data['title'] or None,
            #title_exact=field['title'] or None,
            entity_and_fn=doc_id,
            result_type=lookup)
    idx_writer.commit()


def build_index(fields_by_entity):
    """
    build the index from given field list and remove the old index if any
    """
    print 'starting to build the index...'
    # remove the old index, if any
    shutil.rmtree(os.path.join(INDEX_DIR, '*'), ignore_errors=True)

    print 'initializing service fields index'
    # restructure data - flatten the field list
    field_list = chain(*(x.itervalues() for x in fields_by_entity.itervalues()))
    if _USE_FAKE_FIELDS:
        create_idx_with_redundant_fields(field_list)
    else:
        create_idx_wo_redundant_fields(field_list)
    print 'index creation done.'


def load_index():
    """
    load the whoosh IR index into memory
    """
    global _ix
    _ix = open_dir(INDEX_DIR, "idx_name")
    return _ix


def search_index(keywords, result_type=False, limit=10, use_bm25f=True):
    """
    perform search over the IR index on the given keywords

    the ranker:
      - promotes full phrase matches if any
      - promotes complete word match over partial (e.g. stem)
    """
    if _DEBUG:
        print 'Evaluating query: ', keywords

    weighting = whoosh.scoring.TF_IDF()
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

        #keyword_list = _text(lower_filter(tokenizer(keywords)))
        keyword_list_no_stopw = _text(stopword_filter(
            lower_filter(tokenizer(keywords))))
        keyword_list_stemmed = _text(stemmer(stopword_filter(
            lower_filter(tokenizer(keywords)))))

        # build the terms to search
        if _USE_FAKE_FIELDS:
            fields_to_search = ['fieldname', 'fieldname_processed_parents',
                                'fieldname_processed_current',
                                'fieldname_current', 'title']

            all_fields_and_terms = [Term(f, kw) for kw in
                                    keyword_list_no_stopw
                                    for f in fields_to_search]
            all_fields_and_terms.extend([Term('title_stemmed', kw)
                                         for kw in keyword_list_stemmed])

            if keyword_list_no_stopw and len(keyword_list_no_stopw) > 1:
                # noinspection PyTypeChecker
                all_fields_and_terms.append(
                    Phrase('title', keyword_list_no_stopw, boost=2, slop=1))
        else:
            fields_to_search = ['fieldname', 'fieldname_processed_parents',
                                'fieldname_processed_current']

            all_fields_and_terms = [Term(f, kw) for kw in
                                    keyword_list_stemmed
                                    for f in fields_to_search]
            all_fields_and_terms.extend([Term('title_stemmed', kw)
                                         for kw in keyword_list_stemmed])
            # Note: WE no not search by stopwords....
            # here phrase also uses title_stemmed, it will match title stemmed,
            #  so score boost shouldn't be too high
            if keyword_list_no_stopw and len(keyword_list_no_stopw) > 1:
                # noinspection PyTypeChecker
                all_fields_and_terms.append(
                    Phrase('title_stemmed', keyword_list_stemmed,
                           boost=1.5, slop=1))
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

        def get_matched_keywords(hit):
            """
            return the list of keyword that were matched
            """
            # which keywords have been matched
            f_kw_stemmed = lambda kw: _text(stemmer(stopword_filter(
                lower_filter(tokenizer(kw)))))
            f_kw_no_stopword = lambda kw: _text(stopword_filter(
                lower_filter(tokenizer(kw))))
            keyword_list_no_stopw = _text(stopword_filter(
                lower_filter(tokenizer(keywords))))
            terms_matched = set(val for (field, val) in hit.matched_terms())
            if _DEBUG:
                print 'matched_terms_list', terms_matched

            matched_kws = set()
            for kw in keyword_list_no_stopw:
                keyword_stemmed = f_kw_stemmed(kw)[0]
                if _DEBUG:
                    print 'kw', kw, 'stemmed:', keyword_stemmed, \
                        'kw no stopw', f_kw_no_stopword(kw)
                if keyword_stemmed in terms_matched or kw in terms_matched:
                    matched_kws |= set([kw])

            return matched_kws

        # easiest is just to require all terms, but
        results = [{'field': hit['fieldname'],
                    'result_type': hit['result_type'],
                    'score': hit.score,
                    'keywords_matched': get_matched_keywords(hit),
                    'hit_matched_terms': hit.matched_terms(),
                    'fieldname_matched': [1 for (field, _) in
                                          hit.matched_terms()
                                          if field == 'fieldname']}
                   for hit in hits]
        if _DEBUG:
            pprint.pprint(results)
        return results


def print_results(*args, **kwargs):
    """ run search and print results - used for testsing """
    search_index(*args, **kwargs)


def manual_tests():
    """
    manual tests
    """
    from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
    from DAS.core.das_core import DASCore

    schema_adapter = get_schema(DASCore(multitask=False))
    fields_by_entity = schema_adapter.list_result_fields()
    build_index(fields_by_entity)
    load_index()
    global _DEBUG
    _DEBUG = True
    if False:
        search_index(
            keywords=u'files of Zmm with number of events more than 10',
            result_type=u'dataset')
        search_index(keywords=u'number events', result_type=u'dataset')
        search_index(keywords=u'number evented', result_type=u'dataset')
        search_index(keywords=u'dataset.nevents', result_type=u'dataset')
        search_index(keywords=u'dataset.numevents', result_type=u'dataset')

        # block.replica.subscribed vs block.replica.custodial
        #  (the deepest name in here is the most important)
        search_index(keywords=u'replica fraction', result_type=u'block')
        search_index(keywords=u'replica fraction', result_type=u'site')
        search_index(keywords=u'custodial replica', result_type=u'block')
        search_index(keywords=u'replica_fraction', result_type=u'site')

        print '========================================================='

        search_index(keywords=u'number', result_type=u'dataset')
        search_index(keywords=u'of', result_type=u'dataset')
        search_index(keywords=u'events', result_type=u'dataset')
        search_index(keywords=u'number of', result_type=u'dataset')
        search_index(keywords=u'of events', result_type=u'dataset')
        search_index(keywords=u'Number OF Events', result_type=u'dataset')
    print 'Q: dataset_fraction'
    print_results(keywords=u'dataset_fraction', result_type=u'site')
    print 'Q: dataset fraction'
    print_results(keywords=u'dataset fraction', result_type=u'site')
    print 'Q: dataset part'
    print_results(keywords=u'dataset part', result_type=u'site')
    print '============================================'
    print 'Q: file'
    print_results(keywords=u'file in', result_type='file', limit=4)
    print '============================================'
    print 'Q: file in'
    print_results(keywords=u'file in', result_type='file', limit=4)


if __name__ == '__main__':
    manual_tests()
