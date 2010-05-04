How to add new data-service
===========================
DAS supports pluggable architecture, so adding a new CMS data-service
should be a trivial procedure. Here we discuss two different ways
to add a new service into DAS. 

Plug and play interface
-----------------------
This work is in progress. 

A new data-service can register with DAS by providing its URI/API
configuration. This configuration includes the data-service URL,
the data format it provides, the optional expiration timestamp for
its data, the API name and its parameters and optional mapping into
DAS keys.

A new DAS interface will allow to add this information via simple 
configuration file. The data-service configuration
files should be presented in [YAML]_ data-format. Since DAS is written
in Python we use python YAML library. Here is an example of such configuration
[#f1]_

.. doctest::

    # SiteDB API mapping to DAS
    system : sitedb
    url : "https://a.b.com/sitedb/api"
    format : JSON
    ---
    api : 'CMSNametoAdmins'
    expire : 3600 # optional DAS uses internal default value
    params : {'name':''}
    record : {
        'daskeys' : [{'key':'site', 'map':'site.name', 'pattern':''},
                     {'key':'admin', 'map':'email', 'pattern':''}],
        'api2das' : [
                {'api_param':'name', 'das_key':'admin', 'pattern':""},
        ]
    }
    ---
    # next API

The file provides:

- system name
- url of data-service
- underlying data format used by this service for its meta-data
- the list of apis, each api

  - contains a name
  - its expiration timestamp (how long its data can live in DAS)
  - set of parameters
  - the record description, each record contains

    - list of daskeys, where each key contains mapping within a record and
      appropriate pattern
    - list of API to DAS notations (if any); different API can yield
      data in different notations, for instance, SSN and SocialSecurityNumber.
      To accomodate this syntatic differences we use this mapping.

.. rubric:: Footnotes

.. [#f1] This example demonstrates flexibility of YAML data-format and shows different representation styles.

Add new service via API
----------------------- 
You can manually add new service by extending 
:class:`DAS.services.abstract_service.DASAbstractService` and
overriding its *api* method.

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
Here is an example of such implementation

.. doctest::

    def api(self, query):
        """My API implementation"""
        api     = self.map.keys()[0] # get API from internal map
        args    = dict(self.map[api]['params']) # get args from internal map
        time0   = time.time()
        genrows = function(self.url, args)
        ctime   = time.time() - time0
        self.write_to_cache(query, api, self.url, args, genrows, ctime)

The hypotetical function call should contact data-service and retrieve,
parse and yield data. Please note that we encourage to use 
generator [Gen]_ in function implementation.

.. [Gen] http://www.dabeaz.com/generators/
.. [YAML] http://en.wikipedia.org/wiki/Yaml, http://pyyaml.org/wiki/PyYAMLDocumentation
