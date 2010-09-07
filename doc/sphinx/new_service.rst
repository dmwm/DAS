How to add new data-service
===========================
DAS has pluggable architecture, so adding a new CMS data-service
should be a relatively easy procedure. Here we discuss two different ways
to add a new service into DAS. 

Plug and play interface
-----------------------
This work is in progress. 

A new data-service can register with DAS by providing a file describing
the interface and available APIs. This configuration includes the data-service URL,
the data format provided, an optional expiration timestamp for
the data, the API name, necessary parameters and optional mapping onto
DAS keys.

A new DAS interface will allow this information to be added
via a simple configuration file. The data-service configuration
files should be presented in [YAML]_ data-format. 

An example configuration follows [#f1]_:

.. doctest::

    # SiteDB API mapping to DAS
    system : sitedb
    format : JSON

    # API record
    ---
    # URI description
    urn : CMSNametoAdmins
    url : "https://a.b.com/sitedb/api"
    params : {'name':''}
    expire : 3600 # optional DAS uses internal default value

    # DAS keys mapping defines mapping between query names, e.g. run,
    # and its actual representation in DAS record, e.g. run.number
    daskeys : [
        {'key':'site', 'map':'site.name', 'pattern':''},
        {'key':'admin', 'map':'email', 'pattern':''}
    ]

    # DAS search keys to API input parameter mapping
    das2api : [
            {'das_key':'site', 'api_param':'se', 'pattern':""}
    ]
    ---
    # next API
    ---
    # APIs notation mapping maps data-service output into
    # DAS syntax, e.g
    # {'site_name':'abc'} ==> {'site':{'name':'abc'}}
    notation : [
            {'notation':'site_name', 'map': 'site.name', 'api': ''}
    ]

The syntax consists of key:value pairs, where value can be in a 
form of string, list or dictionary. Hash sign (#) defines a 
comment, the three dashes (---) defines the record separator. 
Each record starts with definition of system and data format 
provided by data-service. 

.. doctest::

   # comment
   system: my_system_name
   format: XML

Those definitions will be applied to each API defined later in 
a map file. 
The API section followed after the record separator and should define: 
*urn*, *url*, *expire*, *params* and *daskeys*. 

.. doctest::

   # API section
   ---
   urn: api_alias
   url: "http://a.b.com/method"
   expire: 3600 # in seconds
   params: {} # dictionary of data-service input parameters
   daskeys: [{}, {}] # list of dictionaries for DAS key maps

- the *urn* is the API name or identifier (any name different from the 
  API name itself) and used solely inside of DAS
- the *url* defines the data-service URL
- the *params* are data-service input parameters
- the *daskeys* is a list of maps between data-service input parameters 
  and DAS internal key representation. For instance when we 
  say *site* we might mean site CMS name or site SE/CE name. 
  So the DAS key will be *site* while DAS internal key 
  representation may be *site.name* or *site.sename*. So, each entry in
  *daskeys* list is defined as the following dictionary:
  {'key':value, 'map':value, 'pattern':''}, where pattern is a
  regular expression which can be used to differentiate between different
  arguments where they have different structures. 
- the (optional) *das2api* map defines mapping between DAS internal 
  key and data-service input parameter. For instance, *site.name* 
  DAS key can be mapping into _name_ data-service input parameter. 

The next API record can be followed by the next record separator, e.g.

.. doctest::

   ---
   # API record 1
   urn: api_alias1
   url: "http://a.b.com/method1"
   expire: 3600 # in seconds
   params: {} # dictionary of data-service input parameters
   daskeys: [{}, {}] # list of dictionaries for DAS key maps
   ---
   # API record 2
   urn: api_alias2
   url: "http://a.b.com/method2"
   expire: 1800 # in seconds
   params: {} # dictionary of data-service input parameters
   daskeys: [{}, {}] # list of dictionaries for DAS key maps

At the end of DAS map there is an optional *notation* mapping, 
which defines data-service output mapping back into DAS internal 
key representation (including converting from flat to hierarchical
structures if necessary).

.. doctest::

   ---
   # APIs notation mapping maps data-service output into
   # DAS syntax, e.g
   # {'site_name':'abc'} ==> {'site':{'name':'abc'}}
   notation : [
           {'notation':'site_name', 'map': 'site.name', 'api': ''}
   ]

For instance, if your data service returns 
runNumber and in DAS we use run_number you'll define this mapping 
in *notation* section.

To summarize, the YAML map file provides

- system name
- underlying data format used by this service for its meta-data
- the list of apis records, each record contains the following:

  - urn name, DAS will use it as API name
  - url of data-service
  - expiration timestamp (how long its data can live in DAS)
  - input parameters, provide a dictionary
  - list of daskeys, where each key contains its name *key*, the
    mapping within a DAS record, *map*, and appropriate pattern
  - list of API to DAS notations (if any); different API can yield
    data in different notations, for instance, siteName and site_name.
    To accommodate these syntactic differences we use this mapping.

- notation mapping between data-service provider output and DAS

.. rubric:: Footnotes

.. [#f1] This example demonstrates flexibility of YAML data-format 
         and shows different representation styles.

Add new service via API
----------------------- 
You can manually add new service by extending 
:class:`DAS.services.abstract_service.DASAbstractService` and
overriding the *api* method.

To do so we need to create a new class
inherited from :class:`DAS.services.abstract_service.DASAbstractService`.

.. doctest::

    class MyDataService(DASAbstractService):
        """
        Helper class to provide access to MyData service
        """
        def __init__(self, config):
            DASAbstractService.__init__(self, 'mydata', config)
            self.map = self.dasmapping.servicemap(self.name)
            map_validator(self.map)
 
optionally the class can override .. function:: def api(self, query)
method of :class:`DAS.services.abstract_service.DASAbstractService`
Here is an example of such an implementation

.. doctest::

    def api(self, query):
        """My API implementation"""
        api     = self.map.keys()[0] # get API from internal map
        url     = self.map[api]['url']
        expire  = self.map[api]['expire']
        args    = dict(self.map[api]['params']) # get args from internal map
        time0   = time.time()
        dasrows = function(url, args) # get data and convert to DAS records
        ctime   = time.time() - time0
        self.write_to_cache(query, expire, url, api, args, dasrows, ctime)

The hypothetical function call should contact the data-service and fetch,
parse and yield data. Please note that we encourage the use of 
python generators [Gen]_ in function implementations.
