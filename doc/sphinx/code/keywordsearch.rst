.. _kws-code-docs:

======================
Keyword Search Modules
======================

.. toctree::
   :maxdepth: 2

.. currentmodule:: DAS.keywordsearch

.. automodule:: DAS.keywordsearch
        :members:

.. contents::
    :local:

Overview
========
.. figure:: ../_images/kws_architecture.png
     :align: right

The basic keyword search workflow:

* The query is tokenized (including shallow parsing for key=val
  patterns and quotes)
* Then by a number of entity matchers the entry points are generated
  (matches of each keyword or the few nearby keywords into some of the schema or value terms which make part of a structured query)
* Exploring the various combinations of the entry points the candidates
  for query suggestions are evaluated and ranked
* The top results are presented to the users (generating a valid
  DAS query and a providing a readable query description)

.. automodule:: DAS.keywordsearch.search
        :members:

.. container:: clearer

    .. image:: ../_images/spacer.png


Adapters to access Metadata
===========================
.. automodule:: DAS.keywordsearch.metadata
        :members:

The schema adapter
------------------
.. automodule:: DAS.keywordsearch.metadata.schema_adapter2
        :members:

.. automodule:: DAS.keywordsearch.metadata.das_output_fields_adapter
        :members:

Input Values Tracker
--------------------
.. automodule:: DAS.keywordsearch.metadata.input_values_tracker
        :members:

DAS Query Language definitions
------------------------------
.. automodule:: DAS.keywordsearch.metadata.das_ql
        :members:


Tokenizing and parsing the query
================================
.. automodule:: DAS.keywordsearch.tokenizer
        :members:


Entity matchers
===============
.. automodule:: DAS.keywordsearch.entity_matchers
        :members:


Value matching
--------------

.. automodule:: DAS.keywordsearch.entity_matchers.value_matching
        :members:

.. automodule:: DAS.keywordsearch.entity_matchers.value_matching_dataset
        :members:

.. automodule:: DAS.keywordsearch.entity_matchers.string_dist_levenstein
        :members:


Name matching
-------------

.. automodule:: DAS.keywordsearch.entity_matchers.name_matching
        :members:

Name matching: multi-term chunks representing field names
---------------------------------------------------------
.. automodule:: DAS.keywordsearch.entity_matchers.kwd_chunks
        :members:

.. automodule:: DAS.keywordsearch.entity_matchers.kwd_chunks.chunk_matcher
        :members:

.. automodule:: DAS.keywordsearch.entity_matchers.kwd_chunks.ir_entity_attributes
        :members:



Generating and Ranking the Query suggestion
===========================================

A ranker implemented in Cython and built into a C extension
-----------------------------------------------------------

.. automodule:: DAS.keywordsearch.rankers
        :members:


the source code is in DAS.keywordsearch.rankers.fast_recursive_ranker which
is compiled into DAS.extensions.fast_recursive_ranker (with help of
DAS/keywordsearch/rankers/build_cython.py )

.. automodule:: DAS.extensions.fast_recursive_ranker
        :members:


Presenting the Results to the user
==================================
.. automodule:: DAS.keywordsearch.presentation
        :members:

.. automodule:: DAS.keywordsearch.presentation.result_presentation
        :members:
