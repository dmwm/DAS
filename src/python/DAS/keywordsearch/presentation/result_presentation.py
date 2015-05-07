#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
The module contain functions for presenting the results as DASQL
and formatting/coloring them in HTML.
"""
from __future__ import print_function

import cgi

from DAS.keywordsearch.config import DEBUG, UI_MAX_DISPLAYED_VALUE_LEN
from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema


DASQL_PATTERNS = {
    'text': {
        'RESULT_TYPE': '%s',
        'INPUT_FIELD_AND_VALUE': '%(field)s=%(value)s',
        'RESULT_FILTER_OP_VALUE': '%(field)s%(op)s%(value)s',
        'PROJECTION': '%s',
        'GREP': ' | grep ',

    },
    'html': {
        'RESULT_TYPE': '<span class="q-res-type">%s</span>',
        'INPUT_FIELD_AND_VALUE':
            '<span class="q-field-name">%(field)s</span>'
            '<span class="op" style="color: #f66;">=</span>%(value)s',
        'RESULT_FILTER_OP_VALUE':
            '<span class="q-post-filter-field-name">%(field)s'
            '<span class="q-op">%(op)s</span></span>%(value)s',
        'GREP': ' | <b>grep</b> ',
        'PROJECTION': '<span class="q-projection">%s</span>'},

}


def dasql_to_nl(dasql_tuple):
    """
    Returns natural language representation of a generated DAS query
     so to explain users what does it mean.
    """
    # TODO: get rid of dasql_tuple, use a namedtuple or dict!?
    (result_type, short_input_params, result_projections, result_filters,
     result_operators) = dasql_tuple

    filters = ['%s=%s' % (f, v) for (f, v) in short_input_params]
    get_title = lambda field: \
        get_schema().get_result_field_title(result_type, field,
                                            technical=True, html=True)
    if result_filters:
        # TODO: add verbose name if any
        filters.extend([
            '{0:s} {1:s} {2:s}'.format(get_title(field), op, val)
            for (field, op, val) in result_filters])

    filters = ' <b>AND</b> '.join(filters)

    if result_projections:
        projections = ', '.join(str(get_title(field))
                                for field in result_projections)
        return '<b>find</b> {projections:s} ' \
               '<b>for each</b> {result_type:s} ' \
               '<b>where</b> {filters:s}'.format(projections=projections,
                                                 result_type=result_type,
                                                 filters=filters)
    else:
        return '<b>find</b> {result_type:s} <b>where</b> {filters:s}'.format(
            result_type=result_type, filters=filters)


def fescape(value):
    """ escape a value to be included in html """
    return value and cgi.escape(value, quote=True) or ''


def shorten_value(value, max_value_len):
    """ provide a shorter version of a very long value for displaying
     in (html) results. Examples include long dataset or block names.
    """
    middle = (max_value_len-2)/2
    return fescape(value[:middle]) + \
        '<span class="not-fitting-value" style="font-size: 0.8em">...</span>' +\
        fescape(value[-middle:])


def result_to_dasql(result, frmt='text', shorten_html=True,
                    max_value_len=UI_MAX_DISPLAYED_VALUE_LEN):
    """
    returns proposed query as DASQL in there formats:

    - text, standard DASQL
    - html, colorified DASQL with long values shortened down (if shorten_html
        is specified)
    """
    patterns = DASQL_PATTERNS[frmt]

    def tmpl(name, params=None):
        """
        gets a pattern, formats it with params if any,
        and apply an escape function if needed
        """
        # a helper function to map values of dict
        # TODO: in Py2.7: {k: f(v) for k, v in my_dictionary.items()}
        map_dict_values = lambda f, my_dict: dict(
            (k, f(v)) for k, v in my_dict.iteritems())

        if frmt == 'html':
            # shorten value if it's longer than
            if isinstance(params, dict) and 'value' in params and shorten_html:
                value = params['value']
                if len(value) > max_value_len:
                    params['value'] = shorten_value(value, max_value_len)
                    params['field'] = fescape(params['field'])
            else:
                # for html, make sure to escape the inputs
                if isinstance(params, tuple) or isinstance(params, list):
                    params = tuple(fescape(param) for param in params)
                elif isinstance(params, dict):
                    params = map_dict_values(fescape, params)
                else:
                    params = params and fescape(params)

        pattern = patterns[name]

        if params is not None:
            return pattern % params
        return pattern

    if isinstance(result, dict):
        score = result['score']
        result_type = result['result_type']
        input_params = result['input_values']
        projections_filters = result['result_filters']
        trace = result['trace']
    else:
        (score, result_type, input_params, projections_filters, trace) = result

    # short entity names
    s_result_type = get_schema().entity_names.get(result_type, result_type)
    s_input_params = [(get_schema().entity_names.get(field, field), value) for
                      (field, value) in input_params]
    s_input_params.sort(key=lambda item: item[0])

    s_query = tmpl('RESULT_TYPE', s_result_type) + ' ' + \
        ' '.join(tmpl('INPUT_FIELD_AND_VALUE',
                      {'field': field, 'value': value})
                 for (field, value) in s_input_params)

    result_projections = [p for p in projections_filters
                          if not isinstance(p, tuple)]

    result_filters = [p for p in projections_filters
                      if isinstance(p, tuple)]

    if result_projections or result_filters:
        if DEBUG:
            print('selections before:', result_projections)
        result_projections = list(result_projections)

        # automatically add wildcard fields to selections (if any),
        # so they would be displayed in the results
        for field, value in input_params:
            if '*' in value and not field in result_projections:
                result_projections.append(field)

        # add formatted projections
        result_grep = [tmpl('PROJECTION', prj) for prj in result_projections]
        # add filters to grep
        s_result_filters = [tmpl('RESULT_FILTER_OP_VALUE',
                                 {'field': field, 'op': op, 'value': val})
                            for (field, op, val) in result_filters]
        result_grep.extend(s_result_filters)
        s_query += tmpl('GREP') + ', '.join(result_grep)

        if DEBUG:
            print('projections after:', result_projections)
            print('filters after:', result_filters)

    return {
        'result': s_query,
        'query': s_query,
        'trace': trace,
        'score': score,
        'entity': s_result_type,
        'das_ql_tuple': (s_result_type, s_input_params, result_projections,
                         result_filters, [])
    }
