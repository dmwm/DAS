DAS raw cache
=============
DAS raw cache holds all *raw* data-service outputs converted into DAS records.

DAS cache records
-----------------

DAS cache keeps data-services meta-data in a form of DAS records.
Each data-service output has been converted into DAS records according to
:ref:`DAS mapping <das_mapping>`.

.. _das_cache_data_record:

data records
++++++++++++

Each data record contains data-service meta-data. Its structure is unknown
a-priory to DAS. Since DAS operates with JSON almost any data structure
can be stored into DAS, e.g. dictionaries, lists, strings, numerals, etc.
The only fields DAS appends to it are:

- das, contains DAS expiration timestamp
- das_id, refers to query2apis data record
- primary_key, provide a primary key to the stored data

For example, here is a data record from `SiteDB <https://cmsweb.cern.ch/sitedb/>`_

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000002'),
     u'das': {u'expire': 1263531376.062233},
     u'das_id': u'4b4f4cb0e2194e72b2000001',
     u'primary_key': u'site.name',
     u'site': {u'name': u'T1_CH_CERN', u'sitename': u'CERN'}}

.. _das_cache_query2apis_record:

query2apis records
++++++++++++++++++

This type of DAS records contain information about underlying API calls
made by DAS upon provided user query. It contains the following keys

- das, a dictionary of DAS operations

  - api, a list of API calls made by DAS upon provided user query
  - ctime, a call time spent for every API call
  - expire, a shortest expiration time stamp among all API calls
  - lookup_keys, a DAS look-up key for provided user query
  - qhash, a md5 hash of input query (in MongoDB syntax)
  - status, a status request field
  - system, a corresponding list of data-service names
  - timestamp, a timestamp of last API call
  - url, a list of URLs for API calls
  - version, reserved for future use

- query, an input user query in a form of MongoDB syntax

Here is an example query2apis record for the following user input *site=T1_CH_CERN* 

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000001'),
     u'das': {u'api': [u'CMStoSiteName',
                       u'CMStoSiteName',
                       u'CMStoSAMName',
                       u'CMSNametoAdmins',
                       u'CMSNametoSE',
                       u'SEtoCMSName',
                       u'CMSNametoCE',
                       u'nodes',
                       u'blockReplicas'],
              u'ctime': [1.190140962600708,
                         1.190140962600708,
                         0.71966314315795898,
                         0.72777295112609863,
                         0.7784569263458252,
                         0.75019693374633789,
                         0.74393796920776367,
                         0.28762698173522949,
                         0.30852007865905762],
              u'expire': 1263489980.1307981,
              u'lookup_keys': [u'site.name'],
              u'qhash': u'5e0dbc2a8e523e0ca401a42a8868f139',
              u'status': u'ok',
              u'system': [u'sitedb', u'sitedb', u'sitedb', u'sitedb',
                          u'sitedb', u'sitedb', u'sitedb', u'phedex', u'phedex'],
              u'timestamp': 1263488176.062233,
              u'url': [u'https://cmsweb.cern.ch/sitedb/json/index/CMStoSiteName',
                       u'https://cmsweb.cern.ch/sitedb/json/index/CMStoSiteName',
                       u'https://cmsweb.cern.ch/sitedb/json/index/CMStoSAMName',
                       u'https://cmsweb.cern.ch/sitedb/json/index/CMSNametoAdmins',
                       u'https://cmsweb.cern.ch/sitedb/json/index/CMSNametoSE',
                       u'https://cmsweb.cern.ch/sitedb/json/index/SEtoCMSName',
                       u'https://cmsweb.cern.ch/sitedb/json/index/CMSNametoCE',
                       u'http://cmsweb.cern.ch/phedex/datasvc/xml/prod/nodes',
                       u'http://cmsweb.cern.ch/phedex/datasvc/xml/prod/blockReplicas'],
              u'version': u''},
     u'query': u'{"fields": null, "spec": {"site.name": "T1_CH_CERN"}}'}



