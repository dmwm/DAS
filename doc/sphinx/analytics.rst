DAS Analytics DB
================

DAS Analytics DB keeps information about user queries placed 
to DAS. Such information is used for pre-fetching strategies 
and further analysis of user queries. 

Analytics DB records
--------------------

query records
+++++++++++++

A query record is produced each time a user sends a query to DAS, 
either by the CLI or the web interface. The record is created when 
DASCore::adjust_query is called (providing add_to_analytics is enabled):

- dasquery, input DAS query
- dhash, md5 hash of dasquery
- mongoquery, DAS query using MongoDB syntax
- qhash, md5 hash of mongoquery
- time, timestamp query was made

It is possible for these records to be identical except for the timestamp.
The time parameter is wanted for analytics, but could alternatively be stored as an array attached to an otherwise unique record, if necessary in future.

Here is an example of query record

.. doctest::

    {u'_id': ObjectId('4b4f4caee2194e72ae000001'),
     u'dasquery': u'site = T1_CH_CERN',
     u'dhash': u'7f0a8d3f0e44f35b72f504fcb77482b7',
     u'mongoquery': u'{"fields": null, "spec": {"site.name": "T1_CH_CERN"}}',
     u'qhash': u'5e0dbc2a8e523e0ca401a42a8868f139',
     u'time': 123456789.0}

api records
+++++++++++

Non-persistent API records are used to keep track of API calls within DAS.
Each record contains:

- apicall, a dictionary of API parameters

  - api, name of the data-service API
  - api_params, a dictionary of API input parameters
  - expire, expiration timestamp for API call
  - system, name of data-service
  - url, the URL of the invoked data-service, please note it may or may not
    be a full URL to the relevant api (which may be part of the query parameters).

Here is an example of API record

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000003'),
     u'apicall': {u'api': u'CMStoSiteName',
                  u'api_params': {u'name': u'T1_CH_CERN'},
                  u'expire': 1263531376.068213,
                  u'system': u'sitedb',
                  u'url': u'https://cmsweb.cern.ch/sitedb/json/index/CMStoSiteName'}}

These records are used by DAS to check availability of meta-data provided by
this API call. So if user placed a request which can be covered by any API records
whose parameters are superset of input query, we look-up this by using API records.

These records are deleted from analytics DB once apicall.expire passes.

api counter records
+++++++++++++++++++

DAS keeps track of API calls. For that we have a separate persistent records
with the following keys

- api, a dictionary of API name and its input parameters
- counter, an incremental counter for this API call
- qhash, corresponding md5 mongoquery hash
- system, a data-service name

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000000'),
     u'api': {u'name': u'CMStoSiteName', u'params': {u'name': u'T1_CH_CERN'}},
     u'counter': 1,
     u'qhash': u'5e0dbc2a8e523e0ca401a42a8868f139',
     u'system': u'sitedb'}