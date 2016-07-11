#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0103,C0111,C0301,W0201
# disabled: invalid name,missing docstring, line too long,
# attribute defined not in __init__ -- the way how it's implemented

"""
Provide a layer of abstraction between Keyword Search
and the Data Integration System.
"""
from __future__ import print_function

import pprint
import re
from collections import namedtuple, defaultdict

from DAS.keywordsearch.config import DEBUG
from DAS.keywordsearch.metadata.das_output_fields_adapter \
    import get_outputs_field_list

FULL_DEBUG = False
ApiDef = namedtuple('ApiInputParamsEntry',
                    ['api_entity', 'api_params_set', 'req_params', 'api',
                     'lookup', 'wildcard_params'])


class DasSchemaAdapter(object):
    """
    provides an adapter between keyword search and Data integration system.
    """

    __instance = None
    _lookup_keys = set()

    def __new__(cls, dascore=None):
        """
        aka. parametrized mutable singleton.

        this will allow accessing adapter even without holding reference to it,
        still allowing to pass parameters to it (e.g. DASCore)

        when dascore is given the cached metadata is reinitialized (useful if
         some of data has to be reloaded)
        """
        if DasSchemaAdapter.__instance is None:
            if not dascore:
                raise Exception('Schema adapter was not initialized yet')
                #DasSchemaAdapter.__instance.init(DASCore(multitask=False))
            DasSchemaAdapter.__instance = object.__new__(cls)

        # allow overriding DASCore or reinitializing what is cached
        if dascore:
            DasSchemaAdapter.__instance.init(dascore)

        return DasSchemaAdapter.__instance

    def init(self, dascore=None):
        """
        initialization or re-initialization
        """
        self.entity_names, self._apis_by_their_input_contraints = \
            self._discover_apis_and_inputs(dascore)

        # compile the input constraints (regexps)
        regexps = self._apis_by_their_input_contraints.items()
        self.compiled_input_regexps = [
            (re.compile(regexp), regexp, apis)
            for regexp, apis in regexps]

        self._lookup_keys |= set(self.entity_names.values())
        self._fields_dict = get_outputs_field_list(dascore)

        if DEBUG:
            self.print_debug()

    @property
    def lookup_keys(self):
        return list(self._lookup_keys)

    _cms_synonyms = {
        'daskeys': {
            'site.name': ['location', ],
            'config.name': ['configuration', ]
        },
        'schema': {
            'site.se': ['storage element'],
            'run.delivered_lumi': ['delivered luminosity'],
        },
        'values': {
            'datatype.name': {
                'mc': 'Monte Carlo',

            }
        }
    }

    @property
    def cms_synonyms(self):
        return self._cms_synonyms

    @property
    def input_fields(self):
        return []

    @property
    def output_fields(self):
        return []

    @property
    def apis_by_their_input_contraints(self):
        return self._apis_by_their_input_contraints

    def _get_api_definition(self, api, sys, mappings):
        """
        returns definition of given API
        """
        entity_long = mappings.primary_mapkey(sys, api)
        entity_short = mappings.primary_key(sys, api)
        api_info = mappings.api_info(sys, api)
        # ensure lookup is in unique order
        lookup_key = ','.join(sorted(api_info['lookup'].split(',')))
        if ',' in lookup_key:
            self._lookup_keys |= set([lookup_key, ])
            if DEBUG:
                print('--------')
                print('Sys:', sys, 'api:', api)
                print('PK: ', entity_short, 'primary_mapkey=', entity_long)
                print('lookup=', lookup_key)

        # could contain some low level params like API version, default values..
        api_params_lowlev = api_info.get('params', {})
        das_map = api_info.get('das_map', [])
        # inputs are those in das_map having api_arg defined
        # other entries can be for instance PK, and are not passed as input
        api_inputs = [p for p in das_map
                      if p.get('api_arg', '')]

        api_inputs_wo_api_arg = [p for p in das_map[1:]
                                 if not p.get('api_arg', '')]
        # TODO: OK, but check if das_map[0] could also be param without api_arg
        api_inputs.extend(api_inputs_wo_api_arg)

        is_required = lambda p: 'api_arg' in p and \
            api_params_lowlev.get(p['api_arg'], '') == 'required'

        api_inputs_set = set(p['rec_key'] for p in api_inputs)
        req_inputs_set = set(p['rec_key']
                             for p in api_inputs
                             if is_required(p))
        api_def = ApiDef(entity_long, api_inputs_set,
                         req_inputs_set, api, lookup_key, set())

        if DEBUG and api_inputs_wo_api_arg:
            print('Sys:', sys, 'api:', api)
            print('lookup:', lookup_key)
            print('entity_long:', entity_long)
            print('req inputs:', req_inputs_set)

            print('das_map w/o api_arg:', api_inputs_wo_api_arg)

            print('other inputs:', api_inputs)
            print('das map: ', das_map)

            print('-----------')

        params_list = []
        for p in api_inputs:
            param_def = {
                'api': api,
                'system': sys,
                'key': p['das_key'],
                'entity_long': p['rec_key'],
                'constr': '',
                'regex_compiled': None,
                'lookup': lookup_key}
            # the patterns defining the inputs accepted are compiled-regexps
            if 'pattern' in p:
                regex = p['pattern']
                pat = regex.pattern
                # modify patterns to make file/dataset regexps more restrictive
                strict_regexps = ['dataset', 'file', 'reco_status', 'run']
                if api in strict_regexps and not ('^' in pat or '$' in pat):
                    pat = '^' + pat + '$'
                    regex = re.compile(pat)
                param_def['regex_compiled'] = regex
                param_def['constr'] = pat
            params_list.append(param_def)

        return api_def, params_list

    def _discover_apis_and_inputs(self, dascore):
        """
        builds a list of apis and input/output fields
        that are defined in service maps
        """
        self.api_input_params = []
        entity_names = {}
        apis_by_their_input_contraints = defaultdict(list)

        mappings = dascore.mapping

        # retrieve list of all daskeys
        # P.S. apis cover only those DAS keys that are result_types
        for das_key in dascore.das_keys():  # the short daskeys
            for entity_long in mappings.mapkeys(das_key):
                entity_names[entity_long] = das_key

        # list all system, api pairs
        api_list = ((sys, api) for sys in dascore.mapping.list_systems()
                    for api in mappings.list_apis(sys))

        for sys, api in api_list:
            entity_long = mappings.primary_mapkey(sys, api)
            entity_short = mappings.primary_key(sys, api)
            entity_names[entity_long or entity_short] = entity_short

            api_def, params_def = self._get_api_definition(api, sys, mappings)

            for p in params_def:
                param_constr = p['constr']
                apis_by_their_input_contraints[param_constr].append(p)

            # the same set of params could come from different systems,
            # but currently we dont care about this
            if not api_def in self.api_input_params:
                self.api_input_params.append(api_def)

        return entity_names, apis_by_their_input_contraints

    def check_result_field_match(self, fieldname):
        """
        checks for complete match to a result field
        """
        if not '.' in fieldname:
            return None
        entity, _ = fieldname.split('.', 1)  # i.e. maxsplit=1
        fields = self._fields_dict.get(entity, {})
        r = fields.get(fieldname)
        if r:
            return entity, r

    def list_result_fields(self, entity=None, inputs=None):
        """
        lists attributes available in all service outputs (aggregated)
        """
        return self._fields_dict

    def get_field_list_for_entity_by_pk(self, result_entity, pk):
        if self._fields_dict:
            return self._fields_dict[result_entity]

    @classmethod
    def are_wildcards_allowed(cls, entity,  wildcards, params):
        """
        Whether wildcards are allowed for given inputs

        currently only these simple rules allowed:
        site=W*
        dataset=W*
        dataset site=T1_CH_*
        dataset site=T1_CH_* dataset=/A/B/C
        file dataset=/DoubleMu/Run2012A-Zmmg-13Jul2012-v1/RAW-RECO site=T1_*
        file block=/A/B/C#D
        file file=W* dataset=FULL
        file file=W* block=FULL

        # these are supported (probably) because of DAS-wrappers
        file dataset=*DoubleMuParked25ns*
        file dataset=*DoubleMuParked25ns* site=T2_RU_JINR
        """
        ok = True
        if wildcards and entity:
            if entity == 'dataset.name' and wildcards == set(['dataset.name']):
                pass
            elif entity == 'file.name' and wildcards == set(['file.name']) and \
                    ('dataset.name' in params or 'block.name' in params):
                pass
            elif entity == 'file.name' and wildcards == set(['site.name']) and \
                    ('dataset.name' in params or 'block.name' in params):
                pass
            elif entity == 'site.name' and wildcards == set(['site.name']):
                pass
            elif entity == 'dataset.name' and wildcards == set(['site.name']):
                pass
            elif entity == 'dataset.name' and wildcards == set(['dataset.name',
                                                                'site.name']):
                pass
            # these are supported (probably) because of DAS-wrapper
            elif entity == 'file.name' and (
                    wildcards == set(['site.name', 'dataset.name']) or
                    wildcards == set(['site.name', 'block.name']) or
                    wildcards == set(['dataset.name']) or
                    wildcards == set(['block.name'])):
                pass
            else:
                ok = False
        return ok

    def validate_input_params(self, params, entity=None, final_step=False,
                              wildcards=None):
        """
        checks if DIS can answer query with given params.
        """
        params = set(params)
        wildcards = wildcards or set()

        # check if wildcards are allowed
        if final_step and wildcards and \
                not self.are_wildcards_allowed(entity, wildcards, params):
            return False

        # given input parameters mapping (from keywords to input parameters)
        # there must exist an API, covering these input params
        fitting_apis = (api for api in self.api_input_params
                        if params.issubset(api.api_params_set) and
                        (entity is None or entity == api.api_entity))

        covered_apis = (api for api in fitting_apis
                        if params.issuperset(api.req_params))

        to_match = covered_apis if final_step else fitting_apis
        return next(to_match, False)

    def get_api_param_definitions(self):
        """
        returns a list of API input requirements
        """
        return [(api.api_params_set, api.req_params, api.lookup)
                for api in self.api_input_params]

    def validate_input_params_lookupbased(self, params, entity=None,
                                          final_step=False, wildcards=None):
        """
        checks if DIS can answer query with given params.
        """
        params = set(params)
        wildcards = wildcards or set()

        # could still be added later...
        if final_step and not entity:
            return False

        # check if wildcards are allowed
        # TODO: this is a quick "hack" to use lookup instead of PK
        lookup = entity and entity.split('.')[0]
        long_entity = lookup and lookup.split(',')[0] + '.name'
        if final_step and wildcards and \
                not self.are_wildcards_allowed(long_entity, wildcards, params):
            return False

        # given input parameters mapping (from keywords to input parameters)
        # there must exist an API, covering these input params
        fitting_apis = (api for api in self.api_input_params
                        if params.issubset(api.api_params_set) and
                        (not lookup or lookup == api.lookup))

        covered_apis = (api for api in fitting_apis
                        if params.issuperset(api.req_params))

        to_match = covered_apis if final_step else fitting_apis

        return next(to_match, False)

    def entities_for_inputs(self, params):
        """
        lists entities that could be retrieved with given input params
        """

        entities = defaultdict(list)
        # the api shall accept the params and to cover all of them
        matching_apis = (api for api in self.api_input_params
                         if params.issubset(api.api_params_set)
                         and params.issuperset(api.req_params))
        for api in matching_apis:
            if api.api_entity is None:
                if DEBUG:
                    print('API RESULT TYPE IS NONE:', api)
                continue
            entities[api.api_entity].append(
                (api.api_entity, api.api_params_set, api.req_params))
        return entities

    def get_result_field_title(self, result_entity, field, technical=False,
                               html=True):
        """
        returns name (and optionally title) of output field
        """
        entity_fields = self._fields_dict[result_entity]
        title = entity_fields.get(field, {'title': ''})['title']

        if technical:
            if title:
                if html:
                    return '%(title)s <span class="nl-result-field">' \
                           '(i.e. %(field)s)</span>' % locals()
                else:
                    return '%(title)s (i.e. %(field)s)' % locals()
            else:
                return field

        if not title:
            title = field
        return title

    def print_debug(self):
        print("ALL APIS:")
        #(entity, params, required, api_name, lookup)
        print('\n'.join("%s(%s) --> %s" %
                        (api.api, ','.join(api.api_params_set), api.api_entity)
                        for api in self.api_input_params))

        print("APIS without required params:")
        #(entity, params, required, api_name, lookup)
        print('\n'.join("%s(%s) --> %s" %
                        (api.api, ','.join(api.api_params_set), api.api_entity)
                        for api in self.api_input_params
                        if not api.req_params))
        print('entity_names')
        pprint.pprint(self.entity_names)
        print('search_field_names')
        pprint.pprint(self._lookup_keys)
        #print 'ENTITY FIELDS (BY LOOKUP):'
        #pprint.pprint(dict(self._fields_dict))
        print('ENTITY FIELDS (BY LOOKUP MULTI ENTITY):')
        pprint.pprint(["{0}: {1}".format(lookup,
                                       self._fields_dict[lookup].keys())
                      for lookup in self._fields_dict.keys()
                      if ',' in lookup])


if __name__ == '__main__':
    from DAS.core.das_core import DASCore
    s = DasSchemaAdapter(DASCore(multitask=False))
    #pprint.pprint(s.list_result_fields())

    print('validate input params():', \
        s.validate_input_params(set(), entity='dataset.name'))
    print('validate input params(dataset.name):', s.validate_input_params(
        set(['dataset.name']), entity='dataset.name'))
    print('validate input params run(dataset.name):', s.validate_input_params(
        set(['dataset.name']), entity='run.run_number', final_step=True))

    # non related entity in input
    print('validate input params run(dataset.name):', s.validate_input_params(
        set(['dataset.name', 'jobsummary.name']), entity='run.run_number',
        final_step=True))

    print('trying to validate Q jobsummary date=20120101: ', \
        s.validate_input_params(set(['date']), entity='jobsummary',
                                final_step=True,
                                wildcards=None))

    print('trying to validate Q monitor date=20120101: ', \
        s.validate_input_params(set(['date']), entity='monitor',
                                final_step=True,
                                wildcards=None))

    #print 'lookup keys=', s.lookup_keys

    print('trying to validate Q: file dataset=/HT/Run2011B-v1/RAW ' \
          'run=176304 lumi=80 &&: ', \
        s.validate_input_params(
            set(['dataset.name', 'run.run_number', 'lumi.number']),
            entity='file.name', final_step=False,
            wildcards=None))

    print('trying to validate Q: file dataset=/HT/Run2011B-v1/RAW ' \
          'run=176304 lumi=80 &&: ', \
        s.validate_input_params(
            set(['dataset.name', 'run.run_number', 'lumi.number']),
            entity='file.name', final_step=True,
            wildcards=None))

    from DAS.extensions.fast_recursive_ranker import is_valid_result_py

    print('trying to validate Q (pyx): file dataset=/HT/Run2011B-v1/RAW ' \
          'run=176304 lumi=80 &&: ', \
        is_valid_result_py(
            set(['dataset.name', 'run.run_number', 'lumi.number']),
            'file', final_step=True, wildcards=None))


    print('trying to validate Q (pyx): file dataset=/HT/Run2011B-v1/RAW ' \
          'run=176304 lumi=80 &&: ', \
        is_valid_result_py(
            set(['dataset.name', 'run.run_number', 'lumi.number']),
            'file', final_step=True, wildcards=None))
