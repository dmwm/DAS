DAS Analytics DB
================

DAS Analytics DB keeps information about user queries placed 
to DAS. Such information is used for pre-fetching strategies 
and further analysis of user queries. 

Analytics DB records
--------------------

query record
++++++++++++

.. doctest::

    {u'_id': ObjectId('4b4f4caee2194e72ae000001'),
     u'dasquery': u'site = T1_CH_CERN',
     u'dhash': u'7f0a8d3f0e44f35b72f504fcb77482b7',
     u'mongoquery': u'{"fields": null, "spec": {"site.name": "T1_CH_CERN"}}',
     u'qhash': u'5e0dbc2a8e523e0ca401a42a8868f139'}

api record
++++++++++

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000003'),
     u'apicall': {u'api': u'CMStoSiteName',
                  u'api_params': {u'name': u'T1_CH_CERN'},
                  u'expire': 1263531376.068213,
                  u'sytsem': u'sitedb',
                  u'url': u'https://cmsweb.cern.ch/sitedb/json/index/CMStoSiteName'}}

api counter record
++++++++++++++++++

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000000'),
     u'api': {u'name': u'CMStoSiteName', u'params': {u'name': u'T1_CH_CERN'}},
     u'counter': 1,
     u'qhash': u'5e0dbc2a8e523e0ca401a42a8868f139',
     u'system': u'sitedb'}
