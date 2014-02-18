
import os
import shutil
import pprint
import itertools
from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import TEXT, ID, KEYWORD, Schema
from whoosh.query import Term, Or, And
from whoosh.analysis import LowercaseFilter, RegexTokenizer, StripFilter
import whoosh.scoring
from DAS.web.dbs_daemon import find_datasets, list_dbs_instances


_DEBUG = True
INDEX_DIR = os.environ.get('DAS_DATASETS_IR_INDEX', '/tmp/das_datasets')
INDEX_NAME = 'idx_dataset'


def flatten(iter_iter):
    return list(itertools.chain.from_iterable(iter_iter))


def DatasetTokenizer(lowercase=False):
    """Splits tokens by commas.

    Note that the tokenizer calls unicode.strip() on each match of the regular
    expression.

    >>> dt = DatasetTokenizer()
    >>> dataset = u'/ZMM_7TeV_cfi_GEN_Pythia-Dec01/ptan-ZMM_7TeV/USER'
    >>> [token.text for token in dt(dataset)]
    [u'ZMM', u'7TeV', u'cfi', u'GEN', u'Pythia', u'Dec01', u'ptan', u'ZMM', u'7TeV', u'USER']

    >>> dt = DatasetTokenizer(lowercase=True)
    >>> dataset = u'/ZMM_7TeV_cfi_GEN_Pythia-Dec01/ptan-ZMM_7TeV/USER'
    >>> [token.text for token in dt(dataset)]
    [u'zmm', u'7tev', u'cfi', u'gen', u'pythia', u'dec01', u'ptan', u'zmm', u'7tev', u'user']
    """

    tokenizer = RegexTokenizer(r"[^/\-_]+") | StripFilter()
    if lowercase:
        tokenizer = tokenizer | LowercaseFilter()
    return tokenizer


class DatasetIRIndex(object):
    _ix = None

    def __init__(self):
        self._ix = self.__load_index()
        # other available whoosh.scoring.PL2() whoosh.scoring.TF_IDF()
        # but BM25F seems best, though not perfect...
        self.weighting = whoosh.scoring.BM25F()

    def query(self, query, dbs_inst, limit=20):
        tokens = query.split(' ')
        with self._ix.searcher(weighting=self.weighting) as s:
            query_terms = flatten([Term('name_tok_low', kw.lower(), boost=1.0),
                                   Term('name_tok', kw, boost=0.3)]
                                  for kw in tokens)
            q = Or(query_terms)
            if dbs_inst:
                q = And([Term('dbs_instance', dbs_inst), Or(q)])
                # TODO: include dbs_inst in results!!
            hits = s.search(q, terms=True, optimize=True, limit=limit)
            results = [{'score': hit.score,
                        'inst': hit['dbs_instance'],
                        'terms': hit.matched_terms(),
                        'dataset': hit['name']}
                       for hit in hits]
            if _DEBUG:
                print 'Q:'
                pprint.pprint(q)
                pprint.pprint(results)
            return

    @classmethod
    def __load_index(cls):
        """
        load the whoosh IR index into memory
        """
        _ix = open_dir(INDEX_DIR, INDEX_NAME)
        return _ix

    @classmethod
    def create_index(cls):
        """ creates the index containing dataset names """
        print 'creating dataset index'
        schema = Schema(
            name=TEXT(stored=True),
            # fieldname_current=TEXT(stored=True, field_boost=1),
            name_tok_low=TEXT(analyzer=DatasetTokenizer(lowercase=True)),
            # increase the score on case sensitive match, but only slightly
            name_tok=TEXT(analyzer=DatasetTokenizer()),
            dbs_instance=KEYWORD(stored=True))

        # make sure we have INDEX_DIR
        if not os.path.isdir(INDEX_DIR):
            os.mkdir(INDEX_DIR)
        # remove the old index, if any
        shutil.rmtree(os.path.join(INDEX_DIR, '*'), ignore_errors=True)

        # create the index
        idx = create_in(INDEX_DIR, schema, INDEX_NAME)
        idx_writer = idx.writer()
        for inst in list_dbs_instances():
            print 'inst=', inst
            i = 0
            for dataset in find_datasets('*', dbs_instance=inst, limit=-1):
                # P.S. Whoosh don't except '' as values, so we need 'or None'
                idx_writer.add_document(name=dataset, name_tok_low=dataset,
                                        name_tok=dataset,
                                        dbs_instance=unicode(inst))
                i += 1
                if i % 10000 == 0:
                    print i
        print 'committing the index'
        idx_writer.commit()
        print 'dataset index creation done'


if __name__ == '__main__':
    import doctest
    doctest.testmod()

    # TODO: this is rather slow!
    DatasetIRIndex.create_index()
    ir = DatasetIRIndex()
    ir.query('RAW1 ZMM', dbs_inst=False)