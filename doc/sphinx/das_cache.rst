DAS raw cache
=============
DAS raw cache holds all *raw* data-service outputs converted into DAS notations.

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



