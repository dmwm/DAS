# coding=utf-8
"""
calculates the entry points - assignment of individual keywords (if taken alone)
 to the corresponding most possible interpretations
"""
from nltk.corpus import stopwords

from DAS.keywordsearch.entity_matchers.name_matching \
    import keyword_schema_weights
from DAS.keywordsearch.entity_matchers.value_matching \
    import keyword_value_weights
from DAS.keywordsearch.entity_matchers.result_fields_by_chunks \
    import generate_chunks_no_ent_filter

EN_STOPWORDS = stopwords.words('english')


def get_entry_points(tokens):
    """finds the entry points """
    keywords = [kw.strip() for kw in tokens
                if kw.strip()]
    schema_ws = {}
    values_ws = {}
    for keyword_index, keyword in enumerate(keywords):
        kw_value = kw_schema = keyword
        if '=' in keyword:
            if len(keyword.split('=')) == 2:
                kw_schema, kw_value = keyword.split('=')
        is_stopword = keyword in EN_STOPWORDS
        schema_ws[keyword] = keyword_schema_weights(kw_schema,
                                                    keyword_index=keyword_index)
        if not is_stopword and kw_value:
            values_ws[keyword] = keyword_value_weights(kw_value)

    chunks = generate_chunks_no_ent_filter(keywords)
    return chunks, schema_ws, values_ws
