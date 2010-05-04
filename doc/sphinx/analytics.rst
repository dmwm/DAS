DAS Analytics DB
================

DAS Analytics DB keeps information about user queries placed 
to DAS. Such information is used for pre-fetching strategies 
and further analysis of user queries. This slide show you a 
sketch of workflow between DAS cache request server and DAS 
analytics DB. Here is an example of DAS Analytics DB record:

.. doctest::

    {
     "dasquery": "find site where site=T1_CH_CERN", 
     "_id": "4ac66890e2194e3ffd000000", 
     "qhash": "6e682db2b7a43d737e4fa47d39a63701", 
     "mongoquery": {"fields": ["site", "das"], "spec": {"site.name": "T1_CH_CERN"}}
    }

.. doctest::

    {
     "api": {"params": {"name": "T1_CH_CERN"}, "name": "CMStoSiteName"}, 
     "_id": "4ac668a8e2194e4000000000", 
     "qhash": "6e682db2b7a43d737e4fa47d39a63701", 
     "system": "sitedb", 
     "counter": 2
    }

First query represents DAS-QL expression, 
its `MongoDB <http://www.mongodb.org>`_ query counterpart and query hash. 
The second record, represents information about API call placed on 
behave of DAS to concrete data-service.
