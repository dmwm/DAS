

__author__ = 'vidma'

import pprint

from nltk.corpus import stopwords

import DAS.keywordsearch.entity_matchers.result_fields_by_chunks
from DAS.keywordsearch.entity_matchers.name_matching import keyword_schema_weights
from DAS.keywordsearch.entity_matchers.value_matching import keyword_value_weights

from DAS.keywordsearch.config import MINIMAL_DEBUG

en_stopwords = stopwords.words('english')

def get_entry_points(tokens, DEBUG=False):
    keywords = [kw.strip() for kw in tokens
                if kw.strip()]

    en_stopwords = stopwords.words('english')
    schema_ws = {}
    values_ws = {}
    for keyword_index, keyword in enumerate(keywords):
        # A=B, is a very good clue of what have to be actually mapped to what.
        #  TODO: use this is the ranking, and pruning

        kw_value = kw_schema = keyword

        if '=' in keyword:
            if len(keyword.split('=')) == 2:
                kw_schema, kw_value = keyword.split('=')

        is_stopword = keyword in en_stopwords

        schema_ws[keyword] = keyword_schema_weights(kw_schema,
                                                    keyword_index=keyword_index)

        if not is_stopword and kw_value:
            values_ws[keyword] = keyword_value_weights(kw_value)
    if MINIMAL_DEBUG:
        print '============= Schema mappings (TODO) =========='
        pprint.pprint(schema_ws)
        print '=============== Values mappings (TODO) ============'
        pprint.pprint(values_ws)
    chunks = DAS.keywordsearch.entity_matchers.result_fields_by_chunks.generate_chunks_no_ent_filter(keywords)
    return chunks, schema_ws, values_ws

