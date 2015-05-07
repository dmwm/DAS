#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0103
# pylint disabled: C0103 - we use short names, helper functs are not constants
"""
this is the IR-based ranker used in matching keywords into names of
fields in service outputs. These may be composed of unclean,technical terms.
"""
from __future__ import print_function
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


INDEX_DIR = os.environ.get('DAS_KWS_IR_INDEX', '/tmp/das')

# Helper functions for manipulating keyword lists
tokenize = SpaceSeparatedTokenizer()
remove_stopwords = StopFilter()
tolower = LowercaseFilter()
stemmer = StemFilter()


def kwlist_no_stopwords(kwds):
    """ filter the keywords: remove stopwords, lower and tokenize """
    return [t.text for t in remove_stopwords(tolower(tokenize(kwds)))]


def kwlist_stemmed(kwds):
    """ filter the keywords: STEM, remove stopwords, lower and tokenize """
    return [t.text for t in stemmer(remove_stopwords(tolower(tokenize(kwds))))]


class SimpleIREntityAttributeMatcher(object):
    """
    Each instance of matcher recreates the index.
    """
    _ix = None

    def __init__(self, fields_by_entity):
        self._build_index(fields_by_entity)
        self._ix = self.__load_index()

        # other available whoosh.scoring.PL2() whoosh.scoring.TF_IDF()
        # but BM25F seems best, though not perfect...
        self.weighting = whoosh.scoring.BM25F()

    def _build_index(self, fields_by_entity):
        """
        build the index from given field list and remove the old index if any
        """
        print('starting to build the index...')
        # restructure data - flatten the field list
        field_list = chain(*(x.itervalues()
                             for x in fields_by_entity.itervalues()))
        self.__create_idx(field_list)
        print('index creation done.')

    @classmethod
    def __load_index(cls):
        """
        load the whoosh IR index into memory
        """
        _ix = open_dir(INDEX_DIR, "idx_name")
        return _ix

    def search_index(self, kwds, result_type=False, limit=10):
        """
        perform search over the IR index on the given keywords

        the ranker:
          - promotes full phrase matches if any
          - promotes complete word match over partial (e.g. stem)
        """
        if _DEBUG:
            print('Evaluating query: ', kwds)

        with self._ix.searcher(weighting=self.weighting) as s:
            all_fields_and_terms = self.__build_search_query(kwds)
            if not all_fields_and_terms:
                return []
            q = Or(all_fields_and_terms)
            if result_type:
                q = And([Term('result_type', result_type),
                         Or(all_fields_and_terms)])
            if _DEBUG:
                print('Q:')
                pprint.pprint(q)
            hits = s.search(q, terms=True, optimize=True, limit=limit)

            def get_matched_keywords(hit):
                """
                return the list of keyword that were matched
                """
                # which keywords have been matched
                keyword_list_no_stopw = kwlist_no_stopwords(kwds)
                terms_matched = set(val for _, val in hit.matched_terms())
                if _DEBUG:
                    print('matched_terms_list', terms_matched)

                matched_kws = set()
                for kw in keyword_list_no_stopw:
                    keyword_stemmed = kwlist_stemmed(kw)[0]
                    if _DEBUG:
                        print('kw', kw, 'stemmed:', keyword_stemmed, \
                            'kw no stopw', kwlist_no_stopwords(kw))
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

    @classmethod
    def __build_search_query(cls, keywords):
        """
        prepares the search query over IR engine
        """
        keyword_list_no_stopw = kwlist_no_stopwords(keywords)
        keyword_list_stemmed = kwlist_stemmed(keywords)

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
        return all_fields_and_terms

    def __create_idx(self, field_list):
        """
        creates a document in IR engine to represent a single field in the
        service outputs (e.g. dataset.tag, weather.temperature)

        This include creating fields in this document for:
         * title (exact, without stopwords and stemmed)
         * parental information (e.g. dataset for dataset.tag)
         * field's main technical name
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

        # make sure we have INDEX_DIR
        if not os.path.isdir(INDEX_DIR):
            os.mkdir(INDEX_DIR)
        # remove the old index, if any
        shutil.rmtree(os.path.join(INDEX_DIR, '*'), ignore_errors=True)
        # create the index
        idx = create_in(INDEX_DIR, schema, "idx_name")
        idx_writer = idx.writer()
        for field_data in field_list:
            # create an IR "document" for each field
            lookup = field_data['lookup']  # TODO: will this work?!!!
            field_name = field_data['name']
            doc_id = lookup + ':' + field_name
            parents = field_name.replace('_', ' ').split('.')[:-1]

            idx_writer.delete_by_term('entity_and_fn', doc_id)  # remove the old
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


class RedundantFieldEnityAttributeIndexWriter(SimpleIREntityAttributeMatcher):
    """
    In this representation for each attribute a its corresponding IR document
    will contain fields with the original values, and copies of the processed
    values (e.g. original title + title without stopwords + stemmed title).

    This do not perform very well, as with default BM25F scoring it accounts
    multiple times for the same keyword (the works then this keyword is not very
    informative).
    """
    def __build_search_query(self, keywords):
        keyword_list_no_stopw = kwlist_no_stopwords(keywords)
        keyword_list_stemmed = kwlist_stemmed(keywords)

        # build the terms to search
        fields_to_search = ['fieldname', 'fieldname_processed_parents',
                            'fieldname_processed_current',
                            'fieldname_current', 'title']

        all_fields_and_terms = [Term(f, kw) for kw in keyword_list_no_stopw
                                for f in fields_to_search]
        all_fields_and_terms.extend([Term('title_stemmed', kw)
                                     for kw in keyword_list_stemmed])

        if keyword_list_no_stopw and len(keyword_list_no_stopw) > 1:
            # noinspection PyTypeChecker
            all_fields_and_terms.append(Phrase('title', keyword_list_no_stopw,
                                               boost=2, slop=1))
        return all_fields_and_terms

    def __create_idx(self, field_list):
        """
        creates a document in IR engine for each field in the
        service outputs (e.g. dataset.tag, weather.temperature)
        This include creating fields in this document for:
         * title (exact, without stopwords and stemmed each as separate fields)
         * parental information (e.g. dataset for dataset.tag)
        """
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

        # make sure we have INDEX_DIR
        if not os.path.isdir(INDEX_DIR):
            os.mkdir(INDEX_DIR)
        # remove the old index, if any
        shutil.rmtree(os.path.join(INDEX_DIR, '*'), ignore_errors=True)

        # create the index
        idx = create_in(INDEX_DIR, schema, "idx_name")
        idx_writer = idx.writer()
        for field_data in field_list:
            # create an IR "document" for each field
            field_name = field_data['name']
            lookup = field_data['lookup']  # TODO: will this work?!!!
            doc_id = lookup + ':' + field_name
            parents = field_name.replace('_', ' ').split('.')[:-1]

            idx_writer.delete_by_term('entity_and_fn', doc_id)  # remove the old
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


def manual_tests():
    """
    manual tests
    """
    from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
    from DAS.core.das_core import DASCore

    schema_adapter = get_schema(DASCore(multitask=False))
    fields_by_entity = schema_adapter.list_result_fields()
    ir_matcher = SimpleIREntityAttributeMatcher(fields_by_entity)

    def print_results(*args, **kwargs):
        """ run search and print results - used for testsing """
        ir_matcher.search_index(*args, **kwargs)

    if False:
        print_results(
            keywords=u'files of Zmm with number of events more than 10',
            result_type=u'dataset')
        print_results(keywords=u'number events', result_type=u'dataset')
        print_results(keywords=u'number evented', result_type=u'dataset')
        print_results(keywords=u'dataset.nevents', result_type=u'dataset')
        print_results(keywords=u'dataset.numevents', result_type=u'dataset')

        # block.replica.subscribed vs block.replica.custodial
        #  (the deepest name in here is the most important)
        print_results(keywords=u'replica fraction', result_type=u'block')
        print_results(keywords=u'replica fraction', result_type=u'site')
        print_results(keywords=u'custodial replica', result_type=u'block')
        print_results(keywords=u'replica_fraction', result_type=u'site')

        print('=========================================================')

        print_results(keywords=u'number', result_type=u'dataset')
        print_results(keywords=u'of', result_type=u'dataset')
        print_results(keywords=u'events', result_type=u'dataset')
        print_results(keywords=u'number of', result_type=u'dataset')
        print_results(keywords=u'of events', result_type=u'dataset')
        print_results(keywords=u'Number OF Events', result_type=u'dataset')
    print('Q: dataset_fraction')
    print_results(keywords=u'dataset_fraction', result_type=u'site')
    print('Q: dataset fraction')
    print_results(keywords=u'dataset fraction', result_type=u'site')
    print('Q: dataset part')
    print_results(keywords=u'dataset part', result_type=u'site')
    print('============================================')
    print('Q: file')
    print_results(keywords=u'file in', result_type='file', limit=4)
    print('============================================')
    print('Q: file in')
    print_results(keywords=u'file in', result_type='file', limit=4)


if __name__ == '__main__':
    manual_tests()
