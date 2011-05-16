DAS services
============

.. toctree::
   :maxdepth: 4

DAS operates using a provided list of data-services and their definitions.
The nature of these services is unimportant provided that they provide a
some form of API which DAS can call and aggregate the data returned.
Each service needs to be registered in DAS by providing
an appropriate configuration file and (optionally) a service handler class..

CMS services
------------
Each CMS data-service is represented by a mapping and, optionally, by a plugin class.
The data-service map contains description of the data-service, e.g. URL, URN, expiry
timeout as well as API and notations maps.

- the API map relates DAS keys and API input parameters. It contains the following items:

  - *api*, name of the API
  - *params*, a list of API input parameters together with regex patterns
    accepted to check the format of or identify ambiguous values. 
  - *record* represents DAS record. Each record has

    - *daskeys*, a list of DAS maps; each map relates keys in the user query to
      the appropriate DAS representation

      - *key*, a DAS key used in DAS queries, e.g. *block*
      - *map*, a DAS record representation of the *key*, e.g. *block.name*
      - *pattern*, a regex pattern for DAS key

    - *das2api*, is a map between DAS key representations and API input parameters

      - *api_param*, an API input parameter, e.g. *se*
      - *das_key*, a DAS key it represents, e.g. *site.se*
      - *pattern*, a regex pattern for *api_param* 

- Notation map represents a mapping between data-service output and DAS records.
  It is optional. 

Please use these links :ref:`API map <api_map>` and :ref:`API notation <notation>`
for concrete examples.

DAS abstract service
--------------------
.. automodule:: DAS.services.abstract_service
        :members:

DAS generic service
-------------------
.. automodule:: DAS.services.generic_service
        :members:

DAS map reader module
---------------------
.. automodule:: DAS.services.map_reader
        :members:

