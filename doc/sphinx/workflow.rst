.. _das_workflow:

DAS workflow
============
DAS workflow consists of the following steps:
- parsing
- query DAS merge cache

  - query raw cache, if you don't check if you have unmerged records that are suitable

    - query services, get the records that aren't available * write new records to raw cache

  - perform aggregation

    - write aggregation results to merged cache

- query merged cache for the result
- present DAS records in Web UI (convert records if it is required)

.. figure:: _images/das_workflow.png 
   :align: center

Logging DB records
------------------

.. doctest::

    {"args": {"query": "site=T1_CH_CERN"}, 
     "qhash": "7c8ba62a07ff2820c217ae3b51686383", "ip": "127.0.0.1", 
     "hostname": "", "port": 65238, 
     "headers": {"Remote-Addr": "127.0.0.1", "Accept-Encoding": "identity", 
                 "Host": "localhost:8211", "Accept": "application/json", 
                 "User-Agent": "Python-urllib/2.6", "Connection": "close"}, 
     "timestamp": 1263488174.364929, 
     "path": "/status", "_id": "4b4f4caee2194e72ae000003", "method": "GET"}

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

DAS cache records
-----------------

data records
++++++++++++

.. doctest::

    {u'_id': ObjectId('4b4f4cb0e2194e72b2000002'),
     u'das': {u'expire': 1263531376.062233},
     u'das_id': u'4b4f4cb0e2194e72b2000001',
     u'primary_key': u'site.name',
     u'site': {u'name': u'T1_CH_CERN', u'sitename': u'CERN'}}

query2apis record
+++++++++++++++++

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

DAS merge records
-----------------

.. doctest::

    {u'_id': ObjectId('4b4f4cb4e2194e72b2000033'),
     u'das': {u'expire': 1263489980.1307981},
     u'das_id': [u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001',
                 u'4b4f4cb0e2194e72b2000001'],
     u'site': [{u'ce': u'ce126.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce201.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce131.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce103.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce128.cern.ch', u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'Josh.Bendavid@cern.ch', u'forename': u'Josh', 
                           u'surname': u'Bendavid', u'title': u'Data Manager'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'gowdy@cern.ch', u'forename': u'Stephen',
                           u'surname': u'Gowdy',u'title': u'T0 Operator'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'gowdy@cern.ch', u'forename': u'Stephen',
                           u'surname': u'Gowdy', u'title': u'Site Executive'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'gowdy@cern.ch', u'forename': u'Stephen',
                           u'surname': u'Gowdy', u'title': u'Data Manager'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'gowdy@cern.ch', u'forename': u'Stephen',
                           u'surname': u'Gowdy', u'title': u'Site Admin'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'dmason@fnal.gov', u'forename': u'David',
                           u'surname': u'Mason', u'title': u'Data Manager'},
                u'name': u'T1_CH_CERN'},
               {u'ce': u'ce132.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce130.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce127.cern.ch', u'name': u'T1_CH_CERN'},
               {u'name': u'T1_CH_CERN', u'samname': u'CERN-PROD'},
               {u'name': u'T1_CH_CERN', u'sitename': u'CERN'},
               {u'admin': {u'email': u'Victor.Zhiltsov@cern.ch', u'forename': u'Victor',
                           u'surname': u'Zhiltsov', u'title': u'Data Manager'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'Peter.Kreuzer@cern.ch', u'forename': u'Peter',
                           u'surname': u'Kreuzer', u'title': u'Site Admin'},
                u'name': u'T1_CH_CERN'},
               {u'ce': u'ce125.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce112.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce129.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce133.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce202.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce106.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce105.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce111.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce104.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce113.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce107.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce114.cern.ch', u'name': u'T1_CH_CERN'},
               {u'ce': u'ce124.cern.ch', u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'Peter.Kreuzer@cern.ch', u'forename': u'Peter',
                           u'surname': u'Kreuzer', u'title': u'Site Executive'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'Christoph.Paus@cern.ch', u'forename': u'Christoph',
                           u'surname': u'Paus', u'title': u'Data Manager'},
                u'name': u'T1_CH_CERN'},
               {u'admin': {u'email': u'ceballos@cern.ch', u'forename': u'Guillelmo',
                           u'surname': u'Gomez-Ceballos', u'title': u'Data Manager'},
                u'name': u'T1_CH_CERN'}]}

