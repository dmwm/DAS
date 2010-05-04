DAS services
============

.. toctree::
   :maxdepth: 4

   services/abstract_service
   services/map_reader
   services/dashboard
   services/dbs
   services/phedex
   services/sitedb
   services/runsummary
   services/monitor

DAS operates with provided list of data-services. It can be
any type of service, e.g. web service, database, data-service.
Each service needs to be registered in DAS by providing
appropriate url, mapping and service handler class.

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

Please use these links: `API map <api_map>`_ and `API notation <notation>`_
for concrete examples.

