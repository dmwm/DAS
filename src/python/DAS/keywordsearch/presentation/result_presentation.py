__author__ = 'vidma'


from DAS.keywordsearch.config  import DEBUG, UI_MAX_DISPLAYED_VALUE_LEN


#import DAS.keywordsearch.metadata.das_schema_adapter as integration_schema
#from DAS.keywordsearch.metadata.das_schema_adapter import entity_names

from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema


def DASQL_2_NL(dasql_tuple, html=True):
    #TODO: DASQL_2_NL
    """
    TODO: return natural language representation of a generated DAS query
     so to explain users what does it mean.
    """
    (result_type, short_input_params, result_projections, result_filters,
     result_operators) = dasql_tuple
    # TODO: support operators (sum, count) and post_filters
    # TODO: distinguish between grep filters and grep selections!!!

    result = result_type
    filters = ['%s=%s' % (f, v) for (f, v) in short_input_params]

    # sum(number of events IN dataset) where dataset=*Zmm*

    if result_filters:
        # TODO: add verbose name if any
        filters.extend(['%s %s %s' %
                        (getSchema().get_result_field_title(result_type, field, technical=True, html=True),
                         op, val) for (field, op, val) in result_filters])

    filters = ' <b>AND</b> '.join(filters)

    if result_projections:
        # TODO: what if entity is different than result_type? We shall probably output that as well...
        result_projections = [
            '%s' % getSchema().get_result_field_title(result_type, field, technical=True, html=True)
            for field in result_projections
        ]
        result_projections = ', '.join(result_projections)
        # TODO: use FOR EACH if selector is not the same, or includes a wildcard!
        return '<b>find</b> %(result_projections)s <b>for each</b> %(result_type)s <b>where</b> %(filters)s' % locals()

    return '<b>find</b> %(result)s <b>where</b> %(filters)s' % locals()




def result_to_DASQL(result, frmt='text', shorten_html = True,
                    max_value_len=UI_MAX_DISPLAYED_VALUE_LEN):
    """
    returns proposed query as DASQL in there formats:
    * text - standard DASQL
    * html - colorified DASQL with long values shortened down (if shorten_html
        is specified)
    """
    import cgi

    _patterns = {
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
                '<span class="q-field-name">%(field)s</span><span class="op" style="color: #f66;">=</span>%(value)s',
            'RESULT_FILTER_OP_VALUE':
                '<span class="q-post-filter-field-name">%(field)s<span class="q-op">%(op)s</span></span>%(value)s',
            'GREP': ' | <b>grep</b> ',
            'PROJECTION': '<span class="q-projection">%s</span>',
            },

    }
    patterns = _patterns[frmt]


    def tmpl(name, params = None):
        """
        gets a pattern, formats it with params if any,
        and apply an escape function if needed
        """

        fescape = lambda v: v and cgi.escape(v, quote=True) or ''
        def shorten_value(val):

            #return fescape(val[:max_value_len-2]) + \
            #                      '<span class="not-fitting-value" style="font-size: 0.8em">...</span>'

            middle = (max_value_len-2)/2
            return fescape(val[:middle]) + \
                     '<span class="not-fitting-value"' + \
                     ' style="font-size: 0.8em">...</span>' + \
                     fescape(val[-middle:])



        if frmt == 'html':

                    # shorten value if it's longer than
                    if isinstance(params, dict) and params.has_key('value') and shorten_html:
                        val = params['value']
                        if len(val) > max_value_len:
                            params['value'] = shorten_value(val)
                            params['field'] = fescape(params['field'])
                    else:
                        # for html, make sure to escape the inputs

                        if isinstance(params, tuple) or isinstance(params, list):
                            params = tuple(map(fescape, params))
                        elif isinstance(params, dict):
                            # a helper function to map values of dict
                            # TODO: in Py2.7: {k: f(v) for k, v in my_dictionary.items()}
                            map_dict_values = lambda f, my_dict: dict(map(lambda (k,v): (k, f(v)), my_dict.iteritems()))

                            params = map_dict_values(fescape, params)


                        else:
                            params = params and fescape(params)

        pattern = patterns[name]

        #print pattern, params, _params


        if params is not None:

            return  pattern % params
        return pattern

    missing_inputs = []

    if isinstance(result, dict):
        score = result['score']
        result_type = result['result_type']
        input_params = result['input_values']
        projections_filters = result['result_filters']
        trace = result['trace']
        #missing_inputs = result['missing_inputs']
    else:
        (score, result_type, input_params, projections_filters, trace) = result

    # short entity names
    s_result_type = getSchema().entity_names.get(result_type, result_type)
    s_input_params = [(getSchema().entity_names.get(field, field), value) for
                      (field, value) in input_params]
    s_input_params.sort(key=lambda item: item[0])

    s_query =tmpl('RESULT_TYPE', s_result_type) + ' ' + \
              ' '.join(
                        tmpl('INPUT_FIELD_AND_VALUE',
                             {'field': field, 'value': value})
                        for (field, value) in s_input_params)

    result_projections = [p for p in projections_filters
                          if not isinstance(p, tuple)]

    result_filters = [p for p in projections_filters
                      if isinstance(p, tuple)]


    if result_projections or result_filters:

        if DEBUG: print 'selections before:', result_projections
        result_projections = list(result_projections)

        # automatically add wildcard fields to selections (if any), so they would be displayed in the results
        for field, value in input_params:
            if '*' in value and not field in result_projections:
                result_projections.append(field)

        # add formated projections
        result_grep = map(lambda prj: tmpl('PROJECTION', prj),result_projections[:])
        # add filters to grep
        s_result_filters = [tmpl('RESULT_FILTER_OP_VALUE', {'field': field, 'op': op, 'value': val})
                            for (field, op, val) in result_filters]
        result_grep.extend(s_result_filters)
        # TODO: NL description

        s_query += tmpl('GREP') + ', '.join(result_grep)

        if DEBUG:
            print 'sprojections after:', result_projections
            print 'filters after:', result_filters


    das_ql_tuple = (s_result_type, s_input_params, result_projections, result_filters, [])
    result = {
        'result': s_query,
        'query': s_query,
        'trace': trace,
        'score': score,
        'entity': s_result_type,
        'das_ql_tuple': das_ql_tuple
    }
    return result
