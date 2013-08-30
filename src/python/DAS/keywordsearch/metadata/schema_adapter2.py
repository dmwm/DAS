__author__ = 'vidma'

import pprint
from collections import namedtuple, defaultdict
from itertools import ifilter


from DAS.core.das_core import DASCore

from DAS.keywordsearch.config import EXCLUDE_RECORDS_WITH_ERRORS, DEBUG


# TODO: now multiple entities can be returned (compound lookups)


ApiDef = namedtuple('ApiInputParamsEntry',
                ['api_entity', 'api_params_set', 'req_params', 'api', 'lookup',
                 'wildcard_params'])


# TODO: is it fast to call constructor each time to get the instance?!!

class DasSchemaAdapter(object):
    """
    provides an adapter between keyword search and Data integration system.
    """

    __instance = None
    _lookup_keys = set()


    def __new__(cls, dascore=None):
        """
        this will allow accessing schema even without holding reference to it,
        still allowing to pass parameters to it (e.g. DASCore)
        """
        if DasSchemaAdapter.__instance is None:
            DasSchemaAdapter.__instance = object.__new__(cls)

            if not dascore:
                DasSchemaAdapter.__instance.init(DASCore())

        # allow overriding DASCore
        if dascore:
            DasSchemaAdapter.__instance.init(dascore)

        return DasSchemaAdapter.__instance


    def init(self, dascore=None):
        print 'DasSchemaAdapter.init(); DAS CORE:'
        pprint.pprint(dascore)


        self.discover_apis_and_fields(dascore)
        self._lookup_keys |= set(self.entity_names.values())


        self.print_debug()
        self._result_fields_by_entity = init_result_fields_list(dascore)




    _cms_synonyms = {
        'daskeys': {
            'site.name': ['location', ]
        },
        'schema': {
            'site.se': ['storage element'],
            'run.bfield': ['magnetic field'],
            'run.delivered_lumi': ['delivered luminosity'],
        },
        'values': {
            'datatype.name': {
                'mc': 'Monte Carlo',

            }
        }
    }

    @property
    def lookup_keys(self):
        return list(self._lookup_keys)

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
        #global apis_by_their_input_contraints
        return self._apis_by_their_input_contraints


    # Discover APIs
    def _get_api_definition(self, api, sys, mappings):
        entity_long = mappings.primary_mapkey(sys, api)
        entity_short = mappings.primary_key(sys, api)
        api_info = mappings.api_info(api)
        lookup_key = api_info['lookup']
        if ',' in lookup_key:
            print '--------'
            print 'Sys:', sys, 'api:', api
            print 'PK: ', entity_short, 'primary_mapkey=', entity_long
            print 'lookup=', lookup_key

            self._lookup_keys |= set([lookup_key, ])

        # could contain some low level params like API version, default values etc
        api_params_lowlev = api_info.get('params', {})
        das_map = api_info.get('das_map', [])
        # inputs are those in das_map having api_arg defined
        # other entries can be for instance PK, and are not passed as input
        api_inputs = [p for p in das_map
                      if p.get('api_arg', '')]

        api_inputs_wo_api_arg = [p for p in das_map[1:]
                                 if not p.get('api_arg', '')]

        #if 'date' in api_inputs_wo_api_arg:
        # TODO: this seem to be correct, but still need to check if das_map[0] could also be param without api_arg
        api_inputs.extend(api_inputs_wo_api_arg)

        is_required = lambda p: \
            'api_arg' in p and api_params_lowlev.get(p['api_arg'],'') == 'required'

        api_inputs_set = set(p['rec_key'] for p in api_inputs)
        req_inputs_set = set(p['rec_key']
                             for p in api_inputs
                             if is_required(p))
        api_def = ApiDef(entity_long, api_inputs_set,
                         req_inputs_set, api, lookup_key, set())



        if True and api_inputs_wo_api_arg:
            print 'Sys:', sys, 'api:', api
            print 'lookup:', lookup_key
            print 'entity_long:', entity_long
            print 'req inputs:', req_inputs_set

            print 'das_map w/o api_arg:', api_inputs_wo_api_arg

            print 'other inputs:', api_inputs
            print 'das map: ', das_map

            print '-----------'


        params_list = []
        for p in api_inputs:
            param_constr = p.get('pattern', '')
            param_def =  {
                 'api': api,
                 'system': sys,
                 'key': p['das_key'],
                 'entity_long': p['rec_key'],
                 'constr': param_constr,
                 'lookup': lookup_key}
            params_list.append(param_def)

        return api_def, params_list

        #entity_names[lookup_key] = lookup_key
        #pprint.pprint(api_info)
        #e.g. of api_info record
        # Sys: dbs api: run_lumi4dataset
        # primary_key = run ??? why?
        # {u'_id': ObjectId('51e544e6579d717cc687c426'),
        #  u'created': 1373979878.512316,
        #  u'das_map': [{u'api_arg': u'run',
        #                u'das_key': u'run',
        #                u'rec_key': u'run.run_number'},
        #               {u'api_arg': u'lumi',
        #                u'das_key': u'lumi',
        #                u'rec_key': u'lumi.number'},
        #               {u'api_arg': u'dataset',
        #                u'das_key': u'dataset',
        #                u'pattern': u'/[\\w-]+/[\\w-]+/[A-Z-]+',
        #                u'rec_key': u'dataset.name'}],
        #  u'expire': 3600,
        #  u'format': u'JSON',
        #  u'instances': [u'prod', u'dev', u'int'],
        #  u'lookup': u'run,lumi',
        #  u'params': {u'dataset': u'required', u'run': u'optional'},
        #  u'services': u'',
        #  u'system': u'dbs3',
        #  u'url': u'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/filelumis',
        #  u'urn': u'run_lumi4dataset',
        #  u'wild_card': u'*'}
        # api_info['lookup'] = run,lumi

    def discover_apis_and_fields(self, dascore):
        """
        builds list of apis and input/output fields that are defined in service maps
        """
        self.api_input_params = []
        apis_by_their_input_contraints = defaultdict(list)

        mappings = dascore.mapping

        # retrieve list of all daskeys
        # P.S. apis cover only those DAS keys that are result_types
        for das_key in dascore.das_keys(): # the short daskeys
            for entity_long in mappings.mapkeys(das_key):
                entity_names[entity_long] = das_key


        # list all system, api pairs
        api_list = ( (sys, api) for sys in dascore.mapping.list_systems()
                    for api in mappings.list_apis(sys))


        for sys, api in api_list:
            # TODO: primary_(map)key would return only first of lookup keys...
            entity_long = mappings.primary_mapkey(sys, api)
            entity_short = mappings.primary_key(sys, api)
            entity_names[entity_long or entity_short] = entity_short
            daskeys_io['output'][entity_long] = entity_short

            api_def, params_def = self._get_api_definition(api, sys, mappings)

            for p in params_def:
                param_constr = p['constr']
                apis_by_their_input_contraints[param_constr].append(p)
                daskeys_io['input'][p['entity_long']] = p['key']

            # the same set of params could come from different systems,
            # but currently we dont care about this
            if not api_def in self.api_input_params:
                self.api_input_params.append(api_def)


        self.entity_names = entity_names
        self._apis_by_their_input_contraints = apis_by_their_input_contraints


    def check_result_field_match(self, fieldname):
        """
        checks for complete match to a result field
        """
        if not '.' in fieldname:
            return None

        entity, field = fieldname.split('.', 1) #i.e. maxsplit=1
        fields = self._result_fields_by_entity.get(entity, {})
        r = fields.get(fieldname)
        if r:
            return entity, r


    def list_result_fields(self, entity=None, inputs=None):
        """
        lists attributes available in all service outputs (aggregated)
        """
        return self._result_fields_by_entity


    # TODO: rename to ?? list_result_fields_for_pk
    # TODO: rename result_entity to looup?
    def get_field_list_for_entity_by_pk(self, result_entity, pk):
        # TODO: make sure all the fields exists on record returned by given PK!!
        # TODO: specify a certain PK

        if self._result_fields_by_entity:
            return self._result_fields_by_entity[result_entity]


    def areWildcardsAllowed(self, entity,  wildcards, params):
        """
        Whether wildcards are allowed for given inputs

        currently only these simple rules allowed:
        site=W*
        dataset=W*
        dataset site=T1_CH_*
        dataset site=T1_CH_* dataset=/BprimeBprimeToBZBZinc_M-375_7TeV-madgraph/Summer11-START311_V2-v3/GEN
        file file=W* dataset=FULL
        file file=W* block=FULL
        """
        # TODO: shall we allow params not defined in the rules?
        ok = True
        if wildcards and entity:
            if entity == 'dataset.name' and wildcards == set(['dataset.name',]):
                pass
            elif entity == 'file.name' and wildcards == set(['file.name',]) and \
                    ('dataset.name' in params or 'block.name' in params):
                pass
            elif entity == 'site.name' and wildcards == set(['site.name',]):
                pass
            elif entity == 'dataset.name' and wildcards == set(['site.name',]):
                pass
            elif entity == 'dataset.name' and wildcards == set(['dataset.name', 'site.name']):
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
        wildcards = wildcards or set([])

        # check if wildcards are allowed
        if final_step and wildcards and \
                not self.areWildcardsAllowed(entity, wildcards, params):
            return False

        # given input parameters mapping (from keywords to input parameters)
        # there must exist an API, covering these input params
        fitting_apis = (api for api in self.api_input_params
                            if params.issubset(api.api_params_set) and \
                            (entity is None or entity == api.api_entity))

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
                print 'API RESULT TYPE IS NONE:', api
                continue
            entities[api.api_entity].append(
                (api.api_entity, api.api_params_set, api.req_params))
        return entities



    def get_result_field_title(self, result_entity, field, technical=False,
                               html=True):
        """

        """
        # TODO: defaultdict?
        # TODO: presentation shall NOT be here
        title = self._result_fields_by_entity[result_entity].get(field, {'title': ''})[
            'title']

        if technical:
            if title:
                if html:
                    return '%(title)s <span class="nl-result-field">(i.e. %(field)s)</span>' % locals()
                else:
                    return '%(title)s (i.e. %(field)s)' % locals()
            else:
                return field

        if not title:
            title = field
        return title

    def print_debug(self):
        print "ALL APIS:"
        #(entity, params, required, api_name, lookup)
        print '\n'.join(["%s(%s) --> %s" % \
                            (api.api, ','.join(api.api_params_set), api.api_entity)
                         for api in self.api_input_params
                         ])

        print "APIS without required params:"
        #(entity, params, required, api_name, lookup)
        print '\n'.join(["%s(%s) --> %s" % \
                            (api.api, ','.join(api.api_params_set), api.api_entity)
                         for api in self.api_input_params
                         if not api.req_params])
        print 'entity_names'
        pprint.pprint(self.entity_names)
        print 'search_field_names'
        pprint.pprint(self._lookup_keys)


# ---- TODO: For now, below is ugly -----
#apis_by_their_input_contraints = {}

entity_names = {} #  dataset.name -> values, user, user.email, user.name
# entities = {'input': [{user.email->user, ..}], 'output': [short_key], }
daskeys_io = {'input': {}, 'output': {}}

search_field_names = [] # i.e. das key
#input_output_params_by_api = {}

#TODO: entities of API results !!!

# a dict by entity of fields in service results
_result_fields_by_entity = {}

DEBUG = False

from itertools import chain


def flatten(listOfLists):
    "Flatten one level of nesting"
    return chain.from_iterable(listOfLists)



# TODO: DAS Specific fields in results of every entity that signify error, etc
# TODO: DAS conflict could be useful
das_specific_fields = ['*.error', '*.reason', 'qhash']
# TODO: shall this be excluded as well or not? 'das.conflict',

# TODO: what is the problem with WARNING KeyLearning:process_query_record got inconsistent system/urn/das_id length

def init_result_fields_list(dascore, _DEBUG=False):
    # TODO: take some titles from DAS integration schema if defined, e.g.
    # site.replica_fraction -->  File-replica presence

    if _DEBUG:
        print 'keylearning collection:', dascore.keylearning.col
        print 'keylearning members:'
        pprint.pprint([item for item in dascore.keylearning.list_members()])
        print 'result attributes (all):'

    fields_by_entity = {}
    for r in dascore.keylearning.list_members():
        #pprint(r)
        result_type = dascore.mapping.primary_key(r['system'], r['urn'])

        #(entity_long, out, in_required, api_) =
        #       input_output_params_by_api[tuple((r['system'], r['urn']))]
        #print (entity_long, out, in_required, api_), '-->',
        #        result_type, ':', ', '.join([m for m in r.get('members', [])])

        result_members = r.get('members', [])
        fields = [m for m in result_members
                  if m not in das_specific_fields
            and m.replace(result_type, '*') not in das_specific_fields
        ]

        contain_errors = [m for m in result_members
                          if m.endswith('.error')]

        if contain_errors:
            print 'WARNING: contain errors: ', result_type, '(' + ', '.join(
                r.get('keys', [])) + ')', ':', \
                ', '.join(fields)
            if EXCLUDE_RECORDS_WITH_ERRORS:
                continue

        # TODO: however not all of the "keys" are used
        if _DEBUG:
            print result_type, '(' + ', '.join(r.get('keys', [])) + ')', ':', \
                ', '.join(fields)

        if not fields_by_entity.has_key(result_type):
            fields_by_entity[result_type] = set()

        fields_by_entity[result_type] |= set(fields)

        # now some of the fields are not refering to the same entity, e.g.
        #  u'release': set([u'dataset.name', u'release.name']),
        # which is actually coming from APIs that return the parameters..



    # assign the titles from presentation cache
    titles_by_field = {}

    if _DEBUG:
        print 'presentation cache:'
        pprint.pprint(dascore.mapping.presentationcache)
    for entries_by_entity in dascore.mapping.presentationcache.itervalues():
        for dic in entries_by_entity:
            field_name = dic['das']
            field_title = dic['ui']
            titles_by_field[field_name] = field_title

    if _DEBUG:
        print 'fields by entity'
        pprint.pprint(fields_by_entity)

    # build the result (fields, their titles)
    results_by_entity = {}
    for entity, fields in fields_by_entity.iteritems():
        if not results_by_entity.has_key(entity):
            results_by_entity[entity] = {}
        for field in fields:
            results_by_entity[entity][field] = {
                'field': field,
                #'field_name': field.split('.')[-1], # last item after .
                # TODO: add synonyms: from predef, ent.val, mapping db
                'title': titles_by_field.get(field, ''),
                # or field.replace(entity+'.', '').replace('_', ' ').replace('.', ' ')).lower(),
                # titles which mapping we don't have explicitly defined are less important
                'importance': bool(titles_by_field.get(field, ''))

            }
            # TODO: also add fields that WERE NOT bootstraped but exist in service definitions

    if _DEBUG:
        print 'results by entity'
        pprint.pprint(results_by_entity)

    return results_by_entity


if __name__ == '__main__':
    # TODO: what if DB unavailable...?
    from DAS.core.das_core import DASCore

    s = DasSchemaAdapter()
    #pprint.pprint(s.list_result_fields())

    print 'validate input params():', s.validate_input_params(set(),
                                                              entity='dataset.name')
    print 'validate input params(dataset.name):', s.validate_input_params(
        set(['dataset.name']), entity='dataset.name')
    print 'validate input params run(dataset.name):', s.validate_input_params(
        set(['dataset.name']), entity='run.run_number', final_step=True)

    # non related entity in input
    print 'validate input params run(dataset.name):', s.validate_input_params(
        set(['dataset.name', 'jobsummary.name']), entity='run.run_number',
        final_step=True)



    print 'trying to validate Q jobsummary date=20120101: ',\
        s.validate_input_params(set(['date']), entity='jobsummary', final_step=True,
                              wildcards=None)


    print 'trying to validate Q monitor date=20120101: ',\
        s.validate_input_params(set(['date']), entity='monitor', final_step=True,
                              wildcards=None)

    print 'lookup keys=', s.lookup_keys