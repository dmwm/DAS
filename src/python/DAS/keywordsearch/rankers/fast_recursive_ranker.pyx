#-*- coding: ISO-8859-1 -*-
# cython: boundscheck = False
# cython: profile = False

# http://docs.cython.org/src/reference/compilation.html#compiler-directives

# cython - libc
from libc.math cimport log, exp

# python standard libs
from collections import defaultdict
from heapq import heappush, heappushpop
from cherrypy import thread_data

# keyword search
from DAS.keywordsearch.config import get_setting, DEBUG, K_RESULTS_TO_STORE, \
    P_NOT_SPECIFIED_RES_TYPE, P_NOT_TAKEN, P_NOT_TAKEN_STOPWORD
from DAS.keywordsearch.metadata import das_ql
from DAS.keywordsearch.nlp import getstem, filter_stopwords
from DAS.keywordsearch.tokenizer import get_keyword_without_operator
from DAS.keywordsearch.tokenizer import get_operator_and_param
from DAS.keywordsearch.rankers.exceptions import TimeLimitExceeded

# Heuristics
# /DoubleMu/Run2012A-Zmmg-13Jul2012-v1xx/RAW-RECO --> dataset
cdef double p_value_as_lookup = 0.80
cdef bint TRACE = DEBUG


# TOOD: this is mainly for backward compatibility
cdef inline double logprob(double score):
   return log(score) if (score > 0.0) else score


# TODO: disabling debug ("trace" variable) would further improve performance


def cleanup_values_weights(values_ws):
    """

    """
    # TODO: move this to matchers to simplify code
    # cleanup values weights
    values_ws_ = defaultdict(list)
    for kw, maps in values_ws.items():
        for score, m in maps:
            if not isinstance(m, dict):
                m = {
                    'map_to': m,
                    # 'adjusted_keyword' # TODO: shall be renamed into _value
                }
            values_ws_[kw].append((score, m))
    return values_ws_


cdef class QueryContext:
    #char*
    # TODO: context in cpython
    #cdef cmap[string, vector[pair[double, string]]] schema_ws
    cdef dict schema_ws, values_ws, chunks
    cdef list kw_list
    cdef int n_kw
    cdef int n_kw_uniq_no_stopw
    cdef clock_t time_limit
    cdef bint time_limit_exceeded
    #cdef int N_kw_non_stopword
    # TODO: cdef list[bint] is_stopword


    def __cinit__(self, schema_ws, values_ws, kw_list, chunks, time_limit):
        self.schema_ws = dict(schema_ws)
        self.values_ws = dict(values_ws)
        self.kw_list = kw_list
        self.n_kw = len(kw_list)
        self.n_kw_uniq_no_stopw = len(filter_stopwords(set(kw_list)))
        self.chunks = dict(chunks)

        # calculate max cpu-time after which execution is not allowed
        cdef clock_t start, end
        start = clock()
        end = start + CLOCKS_PER_SEC * time_limit
        self.time_limit = end
        self.time_limit_exceeded = False


cdef class PartialSearchResult:
    """
    a helper class for storing results.
     each time calling reset() it is re-initialized with the earlier default object
     and can be later overridden.
    """
    cdef public str result_type # lookup, shall be str but get: unicode/str
    cdef public bint result_type_enumerated
    cdef public dict values
    cdef public set wildcards_set, params_set
    cdef public list greps
    cdef PartialSearchResult default
    cdef public set kw_used # TODO: shall it be set instead?
    cdef public double score
    cdef public list trace

    cdef public inline init(self, PartialSearchResult d):
        if d:
            self.result_type = d.result_type
            self.result_type_enumerated = d.result_type_enumerated
            self.greps = d.greps[:]
            self.kw_used = d.kw_used.copy()
            self.score = d.score
            self.trace = d.trace[:]
            self.values = d.values.copy()

            self.wildcards_set = d.wildcards_set.copy()
            self.params_set  = d.params_set.copy()
        else:
            self.result_type = ''
            self.result_type_enumerated = False
            self.values = {}
            self.greps = []
            self.kw_used = set()
            self.score = 0.0
            self.trace = []

            self.wildcards_set = set()
            self.params_set  = set()


    def __cinit__(self, PartialSearchResult d):
        # TODO: empty strngs in cython
        self.init(d)
        self.default = d

    def __str__(self):
        return str((self.result_type, self.values))

    # TODO: maybe multipe ops,e.g.:
    # addGrep
    # addInput
    # addValue
    cdef reset(self, act, new_kw, double delta_score, field=None,
                new_value=None, add_grep=None, list grep_req_kwds=None):
        cdef bint _DEBUG_DETAIL = True

        self.init(self.default)

        upd = field

        if field and (act =='lookup' or act == 'lookup-enum'):
            self.result_type = str(field)
            if act == 'lookup-enum':
                self.result_type_enumerated = True
            upd = field

        if act == 'val+result_type':
            self.result_type = str(field)
            self.result_type_enumerated = False


        if new_kw:
            self.kw_used.add(new_kw)


        if act == 'grep' and add_grep and grep_req_kwds:
            upd = str(add_grep)

            self.greps.append(add_grep)
            for kw in grep_req_kwds:
                self.kw_used.add(kw)


        if delta_score is not None:
            self.score += delta_score

        if field and new_value:
            # TODO: or we could modify the var only if needed
            self.values[field] = new_value
            self.params_set.add(field)
            if '*' in new_value:
                self.wildcards_set.add(field)
            upd = '%s=%s' % (field, new_value)

        # disable extensive trace in production as it's not shown to users...
        if TRACE:
            if _DEBUG_DETAIL:
                upd += "; params: %s;       kw_used: %s, no rt: %s, sc: %.2f" % \
                       (str(self.params_set),
                        str(self.kw_used),
                        str(self.result_type_enumerated),
                        self.score
                        )
            t = (new_kw, act, upd, '%.2f' % exp(delta_score))
            self.trace.append(t)

        return self



    #object *aggregations

# INITIALIZATION


#ctypedef cset[string] ApiParamSet
#ctypedef pair[ApiParamList, ApiParamList] ApiParamDefinition

# Construct the C-Style List of Api Parameter declarations


# TODO: for now using just plain python types
cdef class ApiParamDefinition:
    cdef readonly frozenset api_params_set, req_params
    cdef readonly str lookup

cpdef ApiParamDefinition create_api_param_definition(frozenset api_params_set, frozenset req_params, str lookup):
        cdef ApiParamDefinition inst = ApiParamDefinition.__new__(ApiParamDefinition)
        inst.api_params_set = api_params_set
        inst.req_params = req_params
        inst.lookup = lookup
        return inst

# GLOBALS
#cdef clist[char*] lookup_keys = schema.lookup_keys
#cdef vector[ApiParamDefinition] api_definitions = vector[ApiParamDefinition]()
cdef list api_definitions = list()
cdef list lookup_keys = list()


def initialize_ranker(schema):
    """ initialize the ranker: update cached data (cython) """
    global lookup_keys, api_definitions
    lookup_keys = schema.lookup_keys
    api_definitions = list()
    for api_params, req_params, lookup in schema.get_api_param_definitions():
        api_definitions.append(create_api_param_definition(
                                   api_params_set=frozenset(api_params),
                                   req_params=frozenset(req_params),
                                   lookup=str(lookup)))


#inline
cdef  bint are_wildcards_allowed(str entity,  set wildcards, set params):
        """
        Whether wildcards are allowed for given inputs

        currently only these simple rules allowed:
        site=W*
        dataset=W*
        dataset site=T1_CH_*
        dataset site=T1_CH_* dataset=/BprimeBprimeToBZBZinc_M-375_7TeV-madgraph/Summer11-START311_V2-v3/GEN
        file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*
        file block=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO#b45f3476-cfb6-11e1-bb62-00221959e69e
        file file=W* dataset=FULL
        file file=W* block=FULL

        # these are supported (probably) because of DAS-wrappers
        file dataset=*DoubleMuParked25ns*
        file dataset=*DoubleMuParked25ns* site=T2_RU_JINR
        """
        # TODO: shall we allow params not defined in the rules?
        cdef bint ok = True
        if not wildcards or not entity:
            return True

        # TODO: how to compare cset's?

        if entity == 'dataset' and wildcards == set(['dataset.name',]):
            pass
        elif entity == 'file' and wildcards == set(['file.name',]) and \
                ('dataset.name' in params or 'block.name' in params):
            pass
        elif entity == 'file' and wildcards == set(['site.name',]) and \
                ('dataset.name' in params or 'block.name' in params):
            pass
        elif entity == 'site' and wildcards == set(['site.name',]):
            pass
        elif entity == 'dataset' and wildcards == set(['site.name',]):
            pass
        elif entity == 'dataset' and wildcards == set(['dataset.name', 'site.name']):
            pass
        elif entity == 'dataset' and wildcards <= set (['dataset.name', 'release.name']):
            pass
        # these are supported (probably) because of DAS-wrapper
        elif entity == 'file' and (
                wildcards == set(['site.name', 'dataset.name']) or
                wildcards == set(['site.name', 'block.name']) or
                wildcards == set(['dataset.name']) or
                wildcards == set(['block.name'])
                ):
            pass
        else:
            ok = False
        return ok


# TODO: for now we use python types...
cdef bint validate_input_params_das_cpy(set params, str entity, bint final_step, set wildcards):
    """
    slightly cython-optimized version of schema.validate_input_params
    """
    # could still be added later...
    if final_step and not entity:
        return False

    # check if wildcards are allowed
    # TODO: this is just a quick hack to use lookup instead of PK
    # TODO: now entity is a lookup? but not always as matching returns PK ...
    lookup = entity and entity.split('.')[0]
    #cdef char*
    #long_entity = lookup and lookup.split(',')[0] + '.name'
    if final_step and wildcards and \
            not are_wildcards_allowed(lookup, wildcards, params):
        return False

    # given input parameters mapping (from keywords to input parameters)
    # there must exist an API, covering these input params
    cdef ApiParamDefinition api_item
    for api_item in api_definitions:
        if params.issubset(api_item.api_params_set) and \
                (not lookup or lookup == api_item.lookup):
            if not final_step:
                return True
            if params.issuperset(api_item.req_params):
                return True

    return False


def is_valid_result_py(params_set, result_type, final_step, wildcards):
    # TODO: now PartialSearchResult could be a direct argument...
    return validate_input_params_das_cpy(
        params_set, result_type, final_step, wildcards)


cdef inline bint is_valid_result(PartialSearchResult _r, bint final_step=False):
    # TODO: now PartialSearchResult could be a direct argument...
    return validate_input_params_das_cpy(_r.params_set,
                                 entity=_r.result_type,
                                 final_step=final_step,
                                 wildcards=_r.wildcards_set)

def perform_search(schema_ws, values_ws, kw_list, chunks, time_limit=3):
    values_ws = cleanup_values_weights(values_ws)

    cdef QueryContext c = QueryContext(schema_ws=schema_ws,
                                         values_ws=values_ws,
                                         kw_list=kw_list,
                                         chunks=chunks,
                                         time_limit=time_limit)

    cdef PartialSearchResult r = PartialSearchResult(None)
    run_search(c, r, i=0)
    if c.time_limit_exceeded:
        raise TimeLimitExceeded()



cdef void seach_for_filters(QueryContext c, PartialSearchResult _r, int i):
    if check_time_limit(c):
        return

    requested_entity_short = _r.result_type.split('.')[0]

    cdef PartialSearchResult r = PartialSearchResult(_r)

    cdef list chunks = c.chunks.get(requested_entity_short, [])
    cdef dict match
    cdef int index

    for index in xrange(i, len(chunks)):
        match = chunks[index]
        target_fieldname = match['field_name']

        # we are anyway including this into filter
        if target_fieldname == _r.result_type:
            continue

        req_kwds = match['tokens_required']
        # if all required keywords are still available
        if set(req_kwds).isdisjoint(_r.kw_used) and \
                (target_fieldname not in _r.greps):
            if DEBUG: print 'required fields available for:', match

            target = target_fieldname

            delta_score = match['score']

            if match['predicate']:
                pred = match['predicate']
                target = (target_fieldname, pred['op'], pred['param'])

            tokens = match['tokens_required_non_stopw'] # filter_stopwords(match['tokens_required'])
            if len(tokens) == 1:
                # TODO: these penalties shall be moved within entry points (?)
                delta_score += penalize_highly_possible_schema_terms_as_values(
                    tokens[0], None)
            else:

                penalties = [penalize_highly_possible_schema_terms_as_values(
                        kwd, None) for kwd in tokens]
                # for now, use average
                delta_score += len(penalties) and sum(penalties) / len(
                    penalties) or 0.0

            #_r_filters.append(target)

            # we count each token separately in scoring
            delta_score = len(tokens) * logprob(delta_score)
            r.reset('grep', new_kw=None, delta_score=delta_score, field=target_fieldname,
                    new_value=None, add_grep=target, grep_req_kwds=req_kwds)
            store_result(c, r)
            seach_for_filters(c, r, i+1)


cdef inline bint check_time_limit(QueryContext c):
    """
    check time limit
    """
    # TODO: we may throw python exception here, but it may be slighly slower
    if  clock() > c.time_limit:
        # time limit exceeded
        # TODO: would it help checking time bounds only each whatever # calls
        c.time_limit_exceeded = True
        return True
    return False


# TODO: one could do lazy log-score calculation
# TODO: nicier exception handling
cdef void run_search(QueryContext c, PartialSearchResult _r,
                int i): #
    """
    classify either as:
    - input name
    - input value

    greps:
    - grep filter
    - grep selector

    not supported yet: aggregation operators (min, max)
    """

    if check_time_limit(c):
        return

    #cdef char* lookup
    global lookup_keys


    # is this a valid to continue
    if i > 0 and not is_valid_result(_r, False):
        #print 'bad:', _r
        return

    cdef PartialSearchResult r = PartialSearchResult(_r)

    # check if this could be a valid final result
    if i >= c.n_kw:
        #if not _r.result_type:
        #    print 'lookup:', _r.result_type

        if  r.result_type and is_valid_result(r, final_step=True):
            store_result(c, r)
            seach_for_filters(c, r, 0)

        elif not _r.result_type:
            #print 'no result type... for:', r

            # handle the case when result type is NOT mapped

            # iterate over result types
            # TODO: this fact must be available afterwards for ranking...
            for lookup in lookup_keys:
                # TODO: support multi-value lookup
                r.reset('lookup-enum', '',
                        # rank multi-term lookup slightly lower
                        delta_score=-0.001 if ',' in lookup else 0.0,
                        field = lookup)
                if is_valid_result(r, final_step=True):
                    store_result(c, r)
                    # search for post filters and aggregations, if any
                    seach_for_filters(c, r, 0)

        return


    kw = c.kw_list[i]

    # TODO: what if no kwd mapps into lookup? loop all or try to find which are supported?

    # skip this KW
    # TODO: or I could add the score here
    run_search(c, _r, i+1)

    schema_ws = c.schema_ws.get(kw, [])
    values_ws = c.values_ws.get(kw, [])


    kw_val = kw.split('=')[-1] # without "=", if any...
    kw_field = get_keyword_without_operator(kw)

    op_param = get_operator_and_param(kw)



    for score, field in schema_ws:
        # part of (TODO:possibly multi-daskey) lookup (!)
        if not '=' in kw:
            r.reset('lookup', new_kw=kw, delta_score=log(score), field = field)
            run_search(c, r, i+1)

        # input name
        if not field in _r.values:
            r.values[field] = ''
        r.reset('inputn', new_kw=kw, delta_score=log(score), field=field)
        run_search(c, r, i+1)

        # TODO: it almost doesn't make sense iterating over schema_ws and when value_ws!!!

        # some kwds may be in  daskey=value form
        if op_param and op_param['op'] == '=':
            for vscore, mapping in values_ws:
                vfield = mapping['map_to']
                val = mapping.get('adjusted_keyword', kw_val)

                # TODO: reward matches which honor pre-specified daskey=val
                if vfield == field:
                    vscore *= 1.6
                else:
                    continue

                # TODO: are A=B currently account as two KWDS in penalize?!
                r.reset('val', new_kw=kw,
                        delta_score=log(score)+log(vscore),
                        field=vfield, new_value=val)
                run_search(c, r, i+1)


    # value
    if not op_param or op_param['op'] == '=':
        for score, mapping in values_ws:
            field = mapping['map_to']
            val = mapping.get('adjusted_keyword', kw_val)

            # if both field name and value is matched
            if _r.values.get(field, False) == '':
                score *= 1.15

            r.reset('val', new_kw=kw, delta_score=log(score), field=field, new_value=val)
            run_search(c, r, i+1)

            # TODO: isn't this expanding the search scope as I'm anyways adding all result_types afterwards!?
            # and TODO: this complicates the control flow...
            if not _r.result_type:
                r.reset('val+result_type', new_kw=kw,
                        # TODO: this makes more sense in beginning
                        delta_score=log(score)+log(p_value_as_lookup), field=field, new_value=val)
                run_search(c, r, i+1)






    # filters...


cdef void store_result(QueryContext c, PartialSearchResult r):
    # require some values to be mapped

    value_list  = [(k, v) for k, v in r.values.items()
                   if v != '' ]
    if not value_list:
        return


    cdef PartialSearchResult r_new = PartialSearchResult(r)

    cdef double _score = penalize_non_mapped_keywords_(c, r_new)

    #TODO: struct can be transformed into dict automatically
    result = {'score': _score,
              'result_type': r.result_type,
              'input_values': value_list,
              'result_filters': tuple(r.greps),
              'trace': tuple(r_new.trace + [('adjusted_score', _score),]),
              'missing_inputs': []}

    heap_tuple = (_score, result)

    if DEBUG:
        print 'adding a result:'
        print heap_tuple


    # store only K-best results (if needed)
    if not K_RESULTS_TO_STORE:
        thread_data.results.append(result)
    else:
        if len(thread_data.results) > K_RESULTS_TO_STORE:
            # this adds the item, and removes the smallest
            popped_out = heappushpop(thread_data.results, heap_tuple)
        else:
            heappush(thread_data.results, heap_tuple)



cdef inline double penalize_non_mapped_keywords_(QueryContext c, PartialSearchResult r,
                                  result_type=False):
    """
    penalizes keywords that have not been mapped.
    """
    keywords_used = r.kw_used
    keywords_used_no_stopw = filter_stopwords(r.kw_used)
    score = r.score

    keywords_list= c.kw_list,

    n_total_kw = c.n_kw
    n_kw_without_stopw = c.n_kw_uniq_no_stopw

    # TODO: is keywords_used only non-stopword?!
    # no,but sometimes a semi-stopword can be useful: where, when, .. in beginning of Q

    n_kw_not_used = max(n_kw_without_stopw - len(keywords_used_no_stopw), 0)
    cdef double dscore = logprob(P_NOT_TAKEN) * n_kw_not_used

    score += dscore

    # TODO: shall we map field>=value twice as in averaging approach? then not taking is penalized twice...
    # we add the score twice anyways...!!!
    # TODO: how about multi-keyword "qouted string"

    n_not_mapped_all = max(n_total_kw - len(keywords_used) - n_kw_not_used,0)
    r.trace += [
        str(locals()),
    ]
    score += logprob(P_NOT_TAKEN_STOPWORD) * n_not_mapped_all


    cdef bint r_type_specified = r.result_type and not r.result_type_enumerated
    if r_type_specified:
        pass
    elif r.greps:
        # grep (filter) is also sort of defining the result type, but not allways...
        # this has already +/- accounted for result_type (probability was multiplied)
        # we could add a high probability, so slightly favour explicit result types
        score += logprob(0.8)
    else:
        score += logprob(P_NOT_SPECIFIED_RES_TYPE)

    return score


def normalization_factor_by_query_len(keywords_list):
    """
    provides query score normalization factor (expected very good score)
     based on query length (excluding stopwords).
    keywords with operators get double score (as they are scored twice in the ranking)

    TODO: multi word phrases get multiplied score as well

    + 0.3 extra for entity_type and other boost features
    """
    return 1.0 # probabilistic ranking only


# TODO: this has to be implemented in a better way
# TODO: shall this go into entity_matcher directly?
cdef bint _penalize_highly_possible_schema_terms_as_values_enabled = \
    get_setting('DOWNRANK_TERMS_REFERRING_TO_SCHEMA')


def _penalise_subroutine_schematerms(keyword, schema_ws):
    _DEBUG = 0

    # e.g. configuration of dataset Zmmg-13Jul2012-v1 site=T1_* location is at T1_*
    # 4.37: dataset dataset=*Zmmg-13Jul2012-v1* site=T1_*
    # 4.37: dataset dataset=*dataset* site=T1_*
    # I should look at distribution and compare with other keywords
    f_avg_ = lambda items: len(items) and sum(items) / len(items) or None
    f_avg = lambda items: f_avg_(filter(None, items))
    f_avg_score = lambda interpretations: f_avg(
        map(lambda item: item[0], interpretations))
    # global average schema score
    avg_score = f_avg([f_avg_score(keyword_scores)
                       for keyword_scores in schema_ws.values()]) or 0.0
    # average score for this keyword
    keyword_schema_score = f_avg_score(schema_ws[keyword]) or 0.0
    if _DEBUG:
        print "avg schema score for '%s' is %.2f; avg schema = %.2f " % (
            keyword, keyword_schema_score, avg_score)
    if avg_score < keyword_schema_score:
        return 3 * min(-0.5, -(keyword_schema_score - avg_score))
    else:
        return 0.0




def _get_reserved_terms(stem=False):
    """
    terms that shall be down-ranked if contained in values or in grep-field names
    """
    # TODO: list of entities shall be taken from das_schema_adapter
    entities = ['dataset', 'run', 'block', 'file', 'site', 'config', 'time',
                'lumi']
    operators = das_ql.get_operator_synonyms()
    r = set(entities) | set(operators)

    if stem:
        r = map(lambda w: getstem(w), r)
    return set(r)

cdef set _reserved_terms = _get_reserved_terms(stem=False)
cdef set _reserved_terms_stemed = _get_reserved_terms(stem=True)


cdef double penalize_highly_possible_schema_terms_as_values(keyword, schema_ws):
    """
    it is important to avoid missclassifying dataset, run as values.
    these shall be allowed only if explicitly requested.
    """

    if not _penalize_highly_possible_schema_terms_as_values_enabled:
        return 0.0

    # TODO: this is just a quick workaround
    if keyword in _reserved_terms: #['dataset', 'run', 'block', 'file', 'site']:
        # TODO: each reserved term shall have a different weight, e.g. operators lower than entity?
        return logprob(-5.0)

    if DEBUG: print '_get_reserved_terms(stem=True):', _reserved_terms_stemed

    if not ' ' in keyword and getstem(keyword) in _reserved_terms_stemed: #['dataset', 'run', 'block', 'file', 'site']:
        # TODO: each reserved term shall have a different weight, e.g. operators lower than entity?
        return logprob(-3.0)

    if schema_ws:
        return _penalise_subroutine_schematerms(keyword, schema_ws)

    return 0.0


