__author__ = 'vidma'



EXCLUDE_RECORDS_WITH_ERRORS = False

import math
import pprint
import itertools

from cherrypy import request

from nltk.corpus import wordnet

from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr
import DAS.web.dbs_daemon



DEBUG = False


# TODO: if field has fairly static values, then given value is not in there, is shall be penalized
static_field_values = {
    'site': [],
    'tier': [],
    'status': [],
    'datatype': []
}

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


#print mappings.find_system(system)

# api: listFileProcQuality api2daskey: [u'file', u'dataset', u'file_quality'] apitag: None
#primary_key file
#primary_mapkey file.name
# api returns entities: [{u'map': u'file.name', u'key': u'file', u'pattern': u''}, {u'map': u'dataset.name', u'key': u'dataset', u'pattern': u''}, {u'map': u'file_quality', u'key': u'file_quality', u'pattern': u''}]

apis_by_their_input_contraints = {}

# TODO: Other possibility is just look up DB:
# find .match(value)
# but mongodb is slow and I'd prefer not to relay on it


entity_names = {} #  dataset.name -> values, user, user.email, user.name
search_field_names = [] # i.e. das key

api_input_params = []
input_output_params_by_api = {}

#TODO: entities of API results !!!

# a dict by entity of fields in service results
_result_fields_by_entity = {}



# Discover APIs
def discover_apis_and_fields(dascore):
    mappings = dascore.mapping
    UGLY_DEBUG = False

    global system, apis, api, entity_long, entity_short, parameters, api_info, api_params, p, i, param, param_constraint
    for system in dascore.mapping.list_systems():
        print 'Sys:', system
        print ''
        apis = mappings.list_apis(system)

        for api in apis:
            entity_long = mappings.primary_mapkey(system, api)
            entity_short = mappings.primary_key(system, api)
            parameters = mappings.api2daskey(system=system, api=api)


            entity_names[entity_long or entity_short] = entity_short

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

# TODO: move this to value matching?
def match_value_dataset(keyword):
    DAS.web.dbs_daemon.KEEP_EXISTING_RECORDS_ON_RESTART = 1
    #DAS.web.dbs_daemon.SKIP_UPDATES = 1


    if hasattr(request, 'dbsmngr'):
        dbsmgr = request.dbsmngr
    else:
        dbsmgr = request.dbsmngr = get_global_dbs_mngr()

    print 'DBS mngr:', dbsmgr

    dataset_score = None
    adj_keyword = keyword
    # dbsmgr.find returns a generator, to check if it's non empty we have to access it's entities
    # TODO: check for full and partial match
    # e.g. /DoubleMu/Run2012A-Zmmg-13Jul2012-v1 --> /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/*
    # DoubleMu -> *DoubleMu*
    # TODO: a dataset pattern could be even *Zmm* -- we need minimum length here!!

    if next(dbsmgr.find(pattern=keyword, limit=1), False):
        print 'Dataset matched by keyword %s' % keyword
        # TODO: if contains wildcards score shall be a bit lower
        if '*' in keyword and not '/' in keyword:
            dataset_score = 0.8
        elif '*' in keyword and '/' in keyword:
            dataset_score = 0.9
        elif not '*' in keyword and not '/' in keyword:
            if next(dbsmgr.find(pattern='*%s*' % keyword, limit=1), False):
                dataset_score = 0.7
                adj_keyword = '*%s*' % keyword
        else:
            dataset_score = 1.0

    print 'dataset.name', dataset_score, adj_keyword

    return 'dataset.name', dataset_score, adj_keyword




    # bootstrap queries:
    # sudo python setup.py install && python -u  src/python/DAS/analytics/standalone_task.py -c key_learning
    # sudo python setup.py install && python -u  src/python/DAS/keywordsearch/das_schema_adapter.py


"""
entity.field -- samples

some interesting mappings:
dataset.datatype --> datatype
"""

# TODO: DAS Specific fields in results of every entity that signify error, etc
# TODO: DAS conflict could be useful
das_specific_fields = ['*.error', '*.reason',  'qhash']
# 'das.conflict',

# TODO:
"""
file dataset=/HT/Run2011B-v1/RAW run=176304 lumi=80
returns only file.name
while file dataset=/HT/Run2011B-v1/RAW --> heaps of other stuff
"""

# TODO: what is the problem with WARNING KeyLearning:process_query_record got inconsistent system/urn/das_id length

def init_result_fields_list(dascore, same_entitty_prunning=False, _DEBUG=False):

    # TODO: take some titles from DAS integration schema if defined, e.g.
    # site.replica_fraction -->  File-replica presence

    if _DEBUG:
        print 'keylearning collection:', dascore.keylearning.col
        print 'result attributes (all):'

    fields_by_entity = {}
    for r  in dascore.keylearning.list_members():
        #pprint(r)
        result_type = dascore.mapping.primary_key(r['system'], r['urn'])
        (entity_long, out, in_required, api_) = input_output_params_by_api[tuple((r['system'], r['urn']))]

        #print (entity_long, out, in_required, api_), '-->', result_type, ':', ', '.join([m for m in r.get('members', [])])

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



    # assign the titles
    titles_by_field = {}

    if _DEBUG:
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



def init(dascore):
    print 'DAS CORE IS'
    pprint.pprint(dascore)
    discover_apis_and_fields(dascore)
    search_field_names = set(entity_names.values())


    print "APIS without required params:"
    print '\n'.join(["%s(%s) --> %s" % (api, ','.join(params), entity)
                     for (entity, params, required, api_name) in api_input_params
                     if not required])


    if False:
        # TODO: this is not used (?)
        # TODO 2: we could actually use a standard IR search engine for idf
        # inverted term frequency in the schema, to lower the importance of
        # very common term (e.g. name (dataset.name, file.name)
        idf = {
        }

        for entity_long in entity_names.keys():
            # TODO: stemming?
            # term = entity_long # dataset.name
            if '.' in entity_long:
                term = entity_long.split('.')[1]
                idf[term] = idf.get(entity_long, 0) + 1

        for term in search_field_names:
            # TODO: stemming?
            idf[term] = idf.get(entity_long, 0) + 1

        N = len(idf)
        for (term, frequency) in idf.items():
            idf[term] = math.log(float(N) / frequency)

        # TODO: normalize them
        min_idf = min(idf.values())
        for (term, idf_value) in idf.items():
            idf[term] = idf_value / min_idf

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


def list_result_fields(same_entitty_prunning=False, _DEBUG=False):
    #global _result_fields_by_entity
    if _result_fields_by_entity:
        return _result_fields_by_entity
    else:
        return {}
        raise Exception('keyword search: das schema not loaded')



if __name__ == '__main__':
    pprint.pprint(_result_fields_by_entity)


"""
operators

sort
avg
count
max
min
median
sum
"""
operators = {
    # TODO: is this needed explicitly?
    #'grep': [
    #    {'type': 'filter',
    #     'synonyms': ['filter', 'where']},
    #],

    'avg':
        {'type': 'aggregator',
         'synonyms': ['average', 'avg']},

    # TODO:'number of' is ambiguos with 'number of events' in dataset etc
    'count':
        {'type': 'aggregator',
         'synonyms': ['count', 'how many',  ]},
    'min':
        {'type': 'aggregator',
         'synonyms': ['minimum', 'smallest', 'min']},
    'max':
        {'type': 'aggregator',
         'synonyms': ['largest', 'max', 'maximum']},
    'sum':
        {'type': 'aggregator',
         'synonyms': ['total', 'total sum']},

    'median':
        {'type': 'aggregator',
         'synonyms': ['median']},

    # TODO: problem: selecting between max and sort!
    # TODO: e.g. largest dataset vs what is the largest size of dataset

    # TODO: for size fields, smallest/largest
    'sort %(field)s':
        {'synonyms':
            # TODO: entities that have size!! simpler approach just use expansion, e.g.
            # largest *Zmm* dataset -> largest dataset Zmm
            # TODO:  {'largest (dataset|file|block)': 'ENTITY.size'}
            ['order by', 'sort by', 'sort', ]},
    'sort -%field':
        {'synonyms':
             ['order by %(field)s descending', 'smallest [entity]']},

}

def flatten(listOfLists):
    "Flatten one level of nesting"
    return itertools.chain.from_iterable(listOfLists)

def get_operator_synonyms():
    return flatten([op.get('synonyms', []) for op in operators.values()])



cms_synonyms = {
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
