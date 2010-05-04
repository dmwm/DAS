How to add new data-service
===========================

**IMPORTANT:**
code is under re-factoring to use a pluggable interface via
registration service.

DAS supports pluggable architecture, so adding a new CMS data-service
should be a trivial procedure. To do so we need to create a new class
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

