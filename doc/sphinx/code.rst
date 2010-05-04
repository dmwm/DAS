Documentation for the Code
**************************

.. toctree::
.. automodule:: DAS

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

DAS core classes
================

DAS Core
--------
.. automodule:: DAS.core.das_core
   :members:

DAS Cache
---------
.. automodule:: DAS.core.das_cache
   :members:

DAS Analytics DB
----------------
.. automodule:: DAS.core.das_analytics_db
   :members:

DAS Mapping DB
--------------
.. automodule:: DAS.core.das_mapping_db
   :members:

DAS mongocache
--------------
.. automodule:: DAS.core.das_mongocache
   :members:

DAS robot
---------
.. automodule:: DAS.core.das_robot
   :members:

DAS SON manipulator
-------------------
.. automodule:: DAS.core.das_son_manipulator
   :members:

DAS qlparser
------------
.. automodule:: DAS.core.qlparser
   :members:

DAS web modules
===============

DAS Cache Server
----------------
.. automodule:: DAS.web.DASCacheModel
   :members:

DAS Web Server
--------------
.. automodule:: DAS.web.DASSearch
   :members:

DAS web utils
-------------
.. automodule:: DAS.web.utils
   :members:

DAS services
============

DAS service
-----------
.. automodule:: DAS.services.abstract_service
   :members:

DAS map reader
--------------
.. automodule:: DAS.services.map_reader
   :members:

CMS services
------------
Each CMS data service in DAS represented by its class. In addition we use
two maps who are represneted in YAML format:

- API map to map API metrics into DAS. It contains the following items:

  - *api* which represents name of the API
  - *params* to list API input parameters together with regex expression patterns
    accpeted by parameters
  - *record* to represent DAS record. Each record has

    - *daskeys* a list of DAS keys it represent

      - *key* a DAS key
      - *map* a comma separated fields of DAS record, e.g. block.name
      - *pattern* a regex pattern for DAS key

    - *api2das* map to map API output into DAS records

      - *api_param* is an API input parameter
      - *das_key* a DAS key it represents, e.g. site
      - *pattern* a refex pattern for *api_param* 

- Notation map which represents API notations


Dashboard
+++++++++
.. automodule:: DAS.services.dashboard.dashboard_service
   :members:

DBS
+++
.. automodule:: DAS.services.dbs.dbs_service
   :members:

PhEDEx
++++++
.. automodule:: DAS.services.phedex.phedex_service
   :members:

RunSummary
++++++++++
.. automodule:: DAS.services.runsum.runsum_service
   :members:

Monitor
+++++++
.. automodule:: DAS.services.monitor.monitor_service
   :members:

SiteDB
++++++
.. automodule:: DAS.services.sitedb.sitedb_service
   :members:

