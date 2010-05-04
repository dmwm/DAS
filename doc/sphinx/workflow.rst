.. _das_workflow:

DAS workflow
============
DAS workflow consists of the following steps:

- parse input query

  - look-up for a superset of query conditions in DAS merge cache

    - if not found

      - query raw cache

        - query services, get the records that aren't available 
        - write new records to raw cache

      - perform aggregation

        - write aggregation results to merged cache

    - get results from merged cache
- present DAS records in Web UI (convert records if it is required)

.. figure:: _images/das_workflow.png 
   :align: center


