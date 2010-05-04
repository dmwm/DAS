.. _das_requestflow:

DAS request flow
================
To better understand DAS request flow we provide a use case diagram.

.. figure:: _images/das_requestflow.png 
   :align: center

It shows the request from an user, who ask for data by providing a
query *site=AAA*. DAS resolves it into several requests, by using
incoming map

- *http://a.b.com/se=AAA*
- *http://c.com/site_name=AAA*

and retrieves the results, which are re-mapped into DAS records
according to outgoing map.

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


