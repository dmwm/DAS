DAS merge cache
===============
DAS merge cache is used to keep merged (aggregated) meta-data information
from multiple data-service outputs. For example if service A and service B
returns documents

.. doctest::

   {'service':'A', 'foo':1, 'boo':[1,2,3]}
   {'service':'B', 'foo':1, 'data': {'test':1}}

the DAS will merge those documents based on common key, *foo* and
resulting merged (aggregated) document will be in the following form:

.. doctest::

   {'service':['A','B'], 'foo':1, 'boo':[1,2,3], 'data':{'test':1}}

But of course DAS provide more then just merging the document content.
Below you can find concrete example of merged CMS records.

DAS merge records
-----------------

DAS merge record represents aggregated results made by DAS upon
user input query. Each query contains

- das, an expiration timestamp, based on shortest expire timestamps of
  corresponding :ref:`data records <das_cache_data_record>`
- das_id, a list of corresponding _id's of :ref:`data records <das_cache_data_record>`
  used for this aggregation
- an aggregated data-service part, e.g. site.

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

