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
Each CMS data-service is represented by its map and, optionally, by its plugin class.
The data-service map contains description of the data-service, e.g. URL, URN, expire
timestamp as well as API and notations maps.

- the API map relates DAS keys and API input parameters. It contains the following items:

  - *api* represents name of the API
  - *params* is a list of API input parameters together with regex expression patterns
    accpeted by parameters
  - *record* represents DAS record. Each record has

    - *daskeys* is a list of DAS maps; each map relate user input das key
      and its DAS record representation

      - *key* a DAS key used in DAS queries, e.g. *block*
      - *map* a DAS record representation of the *key*, e.g. *block.name*
      - *pattern* a regex pattern for DAS key

    - *das2api* is a map between DAS key representation and API input parameter

      - *api_param* an API input parameter, e.g. *se*
      - *das_key* a DAS key it represents, e.g. *site.se*
      - *pattern* a refex pattern for *api_param* 

- Notation map represents a mapping between data-service output and DAS records.
  It is optional. 

Please use these links :ref:`API map <api_map>` and :ref:`API notation <notation>`
for concrete examples.

