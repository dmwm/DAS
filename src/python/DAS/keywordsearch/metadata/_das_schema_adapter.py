"""
Some functions to access DAS schema:
- list of services & their params (inputs, outputs)
- list of entities and their fields

Note/TODO: quite unclean code
"""
__author__ = 'vidma'



import pprint
from nltk.corpus import wordnet

from DAS.keywordsearch.config import EXCLUDE_RECORDS_WITH_ERRORS, DEBUG


#DEBUG = True



entity_wordnet_synsets = {
    'site': wordnet.synset('site.n.01'),
    'status': wordnet.synset('condition.n.01'),
    'dataset': wordnet.synset('datum.n.01'), # an item of factual information derived from measurement or research (TODO: actually it is a set of these)
    'date': wordnet.synset('date.n.01'),
    'config': wordnet.synset('configuration.n.01'),
    'block': wordnet.synset('block.n.06'),
    'lumi': wordnet.synset('luminosity.n.01'),
    # TODO: do we have all synsets: group, user, role, release, ?datatype
    # child?, parent, relationship
    # TODO: others, not in wordnet: lumi, tier,
    # -- in full sentence WSD + NER would be useful, but not 100%
    # run, stream, primary_dataset,monitor,
    # ("Synset('block.n.06')",
    #'(computer science) a sector or group of sectors that function as the smallest data unit permitted',
    #["since blocks are often defined as a single sector, the terms `block' and `sector' are sometimes used interchangeably"]),
}

apis_by_their_input_contraints = {}


entity_names = {} #  dataset.name -> values, user, user.email, user.name

# entities = {'input': [{user.email->user, ..}], 'output': [short_key], }
daskeys_io = {'input':{}, 'output':{}}

search_field_names = [] # i.e. das key

api_input_params = []
input_output_params_by_api = {}

#TODO: entities of API results !!!

# a dict by entity of fields in service results
_result_fields_by_entity = {}


def get_io_daskeys():
    """ return which daskeys are available for input and which for output """
    unique = lambda l: list(set(l))
    return unique(daskeys_io['input'].values()), unique(daskeys_io['output'].values())

# Discover APIs
def discover_apis_and_fields(dascore):
    """
    builds list of apis and input/output fields that are defined in service maps
    """
    # TODO: rewrite this freshly

    mappings = dascore.mapping
    UGLY_DEBUG = False

    global system, apis, api, entity_long, entity_short, parameters, api_info, api_params, p, i, param, param_constraint
    for system in dascore.mapping.list_systems():
        print 'Sys:', system
        print ''
        apis = mappings.list_apis(system)

        # apis cover only those DAS keys that are result_types

        for das_key in dascore.das_keys(): # the short daskeys
            long_daskeys = dascore.mapping.mapkeys(das_key)

            for entity_long in long_daskeys:
                entity_names[entity_long]  = das_key


        for api in apis:
            entity_long = mappings.primary_mapkey(system, api)
            entity_short = mappings.primary_key(system, api)
            parameters = mappings.api2daskey(system=system, api=api)


            entity_names[entity_long or entity_short] = entity_short

            daskeys_io['output'][entity_long] = entity_short

            #print 'primary_key', mappings.primary_key(system, api)
            #print 'primary_mapkey', mappings.primary_mapkey(system, api)


            #pprint(api_info)

            # TODO: in schema mapping, the entity returned is not allways exactly that, e.g.
            # api: people_via_email(user) --> user.email
            #entity returned: user.email (short daskey: user)
            #api input constraints: [{u'map': u'user.email', u'key': u'user', u'pattern': u'[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]{2,4}'}]
            #
            #
            #api: people_via_name(user) --> user.name
            #entity returned: user.name (short daskey: user)
            #api input constraints: [{u'map': u'user.name', u'key': u'user', u'pattern': u''}]



            #print 'api info', mappings.api_info(api)
            api_info = mappings.api_info(api)


            # could contain some low level params like API version, default values etc
            api_int_params_list = api_info.get('params', {})

            # das map also contains the mapping of the result entity, so the list of params is intersection of the two
            api_params = [param for param in api_info.get('das_map', [])
                          if param.get('api_arg', param['das_key']) in set(api_int_params_list.keys())]

            # figure out what are the required params
            api_params_required = set([p_name
                                       for p_name, p_spec in api_int_params_list.items()
                                       if p_spec == 'required'])
            required_params = [param['rec_key']
                               for param in api_params
                               if param.has_key('api_arg') and param['api_arg'] in api_params_required]
            #print required_params

            if UGLY_DEBUG:
                print '--------------------------'
                #print 'api: %s(%s) --> %s' %(api, ','.join(parameters), entity_long)
                print 'api: %s(%s) --> %s' % (api, ','.join([p['rec_key'] for p in api_params]), entity_long)
                print 'entity returned: %s (short daskey: %s)' % (entity_long, entity_short)
                #print 'api input constraints:', api_info.get('daskeys', [])
                print 'required:', required_params, ' params:', api_params
                pprint.pprint(api_info)

            # TODO: "required"
            #print api_params

            for param in api_params:
                param_constraint = param.get('pattern', '')
                apis_by_their_input_contraints[param_constraint] = apis_by_their_input_contraints.get(param_constraint,
                    [])

                apis_by_their_input_contraints[param_constraint].append(
                    {'api': api, 'system': system, 'key': param['das_key'], 'entity_long': param['rec_key'] , 'constr': param_constraint})

                daskeys_io['input'][param['rec_key']] = param['das_key']


            # response entity, fields (entity.attr), required_params
            api_def = (entity_long, set([param['rec_key'] for param in api_params]), set(required_params), api)
            if not api_def in api_input_params:
                api_input_params.append(api_def)
            input_output_params_by_api[tuple((system, api))] = api_def

            #mappings.das2api()
            if DEBUG: print '\n'


            # we want both global (DAS) naming (smf.smf + das key) + TODO: naming at the service (with lower priority)


            # TODO: get the service result format





def entities_for_input_params(params):
    """
    returns the list of entities that could be returned with given parameter set
    """

    entities = {}
    for (api_entity, api_params_set, api_required_params_set, api) in api_input_params:
    #print set(params), api_params_set, ' api ent:', api_entity
        if params.issubset(api_params_set)\
        and params.issuperset( api_required_params_set):
            # yield (api_entity, api_params_set, api_required_params_set)
            #print 'ok', api_entity
            if api_entity is None:
                print 'API RESULT TYPE IS NONE:',(api_entity, api_params_set, api_required_params_set)
                continue

            entities[api_entity] = entities.get(api_entity, [])
            entities.get(api_entity, []).append( (api_entity, api_params_set, api_required_params_set) )
        #print entities

    return entities





def validate_input_params(params, entity=None, final_step=False):
    """
    input parameters mapping (from keywords to input parameters) is that there must exist an API
    """
    # TODO: this could also check if actual values are matching API input param requirements
    if final_step:
        # TODO: check that all required params are included
        #print 'validate_input_params_mapping(final):',params, entity
        # currently - a valid mapping shall contain attributes from some api and must satisfy it's minimum input requirements
        # TODO: at the moment we do not allow additional selection
        for (api_entity, api_params_set, api_required_params_set, api) in api_input_params:
            #print set(params), api_params_set, ' api ent:', api_entity
            if set(params).issubset(api_params_set)\
               and set(params).issuperset( api_required_params_set)\
            and (entity is None or entity == api_entity):
                return api_entity, api_params_set, api_required_params_set
    else:
        # requirement for any params mapping is that there must exist such a
        for (api_entity, api_params_set,  api_required_params_set, api) in api_input_params:
            if set(params).issubset(api_params_set) and (entity is None or entity == api_entity):
                return True

    return False






# TODO: DAS Specific fields in results of every entity that signify error, etc
# TODO: DAS conflict could be useful
das_specific_fields = ['*.error', '*.reason',  'qhash']
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
    for r  in dascore.keylearning.list_members():
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
            print 'WARNING: contain errors: ', result_type, '(' +  ', '.join(r.get('keys', [])) + ')', ':',\
            ', '.join(fields)
            if EXCLUDE_RECORDS_WITH_ERRORS:
                continue

        # TODO: however not all of the "keys" are used
        if _DEBUG:
            print result_type, '(' +  ', '.join(r.get('keys', [])) + ')', ':', \
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



def get_result_field_list_by_entity(dascore, entity, input_params, _DEBUG=False):
    """
    return list of fields available in entity, if inputs_params are available
    """

    # TODO: take some titles from DAS integration schema if defined, e.g.
    # site.replica_fraction -->  File-replica presence
    print 'get_result_field_list_by_entity:input_output_params_by_api:'
    pprint.pprint(input_output_params_by_api)

    input_param_set = set(input_params)

    if _DEBUG:
        print 'keylearning collection:', dascore.keylearning.col
        print 'result attributes (all):'

    result_fields = set()


    for r  in dascore.keylearning.list_members():
        #pprint(r)
        result_type = dascore.mapping.primary_key(r['system'], r['urn'])
        print "result_type %s, need entity: %s" % (result_type, entity)

        if result_type != entity:
            continue

        entity_long, out, in_required, api_ = input_output_params_by_api[tuple((r['system'], r['urn']))]


        #print in_required, input_param_set
        if not set(in_required).issubset(input_param_set):
            continue

        #print (entity_long, out, in_required, api_), '-->', result_type, ':', ', '.join([m for m in r.get('members', [])])

        result_members = r.get('members', [])
        fields = [m for m in result_members
                  if m not in das_specific_fields
            and m.replace(result_type, '*') not in das_specific_fields
        ]

        contain_errors = [m for m in result_members
                          if m.endswith('.error')]

        if contain_errors:
            print 'WARNING: contain errors: ', result_type, '(' +  ', '.join(r.get('keys', [])) + ')', ':', \
                ', '.join(fields)
            if EXCLUDE_RECORDS_WITH_ERRORS:
                continue


        result_fields |= set(fields)

        # now some of the fields are not refering to the same entity, e.g.
        #  u'release': set([u'dataset.name', u'release.name']),
        # which is actually coming from APIs that return the parameters..

    return list(result_fields)



def init(dascore):
    print 'DAS CORE IS'
    pprint.pprint(dascore)
    discover_apis_and_fields(dascore)
    search_field_names = set(entity_names.values())


    print "APIS without required params:"
    print '\n'.join(["%s(%s) --> %s" % (api, ','.join(params), entity)
                     for (entity, params, required, api_name) in api_input_params
                     if not required])

    print 'entity_names'
    pprint.pprint(entity_names)
    print 'search_field_names'
    pprint.pprint(search_field_names)



    global _result_fields_by_entity
    _result_fields_by_entity = init_result_fields_list(dascore)



def get_result_field_title(result_entity, field, technical=False, html=True):
    if technical:
        title = _result_fields_by_entity[result_entity].get(field, {'title': ''})['title']
        if title:
            if html:
                return '%(title)s <span class="nl-result-field">(i.e. %(field)s)</span>' % locals()
            else:
                return '%(title)s (i.e. %(field)s)' % locals()
        else: return '%(field)s' % locals()


    title = _result_fields_by_entity[result_entity].get(field, {'title': ''})['title']
    if not title:
        title = field
    return title


def get_field_list_for_entity_by_pk(result_entity, pk):
    # TODO: make sure all the fields exists on record returned by PK !!!
    # TODO: specify a certain PK

    if _result_fields_by_entity:
        return _result_fields_by_entity[result_entity]


def list_result_fields():
    """
    lists the attributes contained in entity results (aggregated from services)
    """

    if _result_fields_by_entity:
        return _result_fields_by_entity
    else:
        raise Exception('keyword search: das schema or field list not loaded')




if __name__ == '__main__':
    from DAS.core.das_core import DASCore
    dascore = DASCore()
    init(dascore)
    pprint.pprint(list_result_fields())




cms_synonyms = {
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
