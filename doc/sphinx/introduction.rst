Introduction
============

DAS stands for Data Aggregation System. It is general purpose
framework to unify data-services into a common layer to be
usable as Google-like interface by end-users. It is developed
for [CMS]_ High Energy Physics experiment at [LHC]_, CERN,
Geneva, Switzerland. Even though it is used so far by CMS
experiment physicists and production tools, its vision
is to have a general purpose architecture applicaple to other
domains.

It provides several features:

  1. a caching layer for data-services
  2. a common tool to look-up meta-data served by data providers (services)
  3. an aggregation layer for meta-data

The main advantage of DAS is uniform meta-data representation
and ability to look-up it via free text-based queries.
The DAS data-format is JSON. All meta-data queried and stored
into DAS are converted into this format according to provided
mapping and common set of notations for CMS data-services
participating in DAS.

All documentation here is a replica of [DAS]_ documentation
available at CERN twikies.

Dependencies
------------
DAS is written in python and relies on standard python modules.
The underlying back-end is `MongoDB <http://www.mongodb.org>`_,
which provides *schema-less* storage and generic query language.
Below you can see current set of dependencies for DAS:

.. figure::  _images/das_dependencies.png
   :align:   center

Here the wmcore-webtools is a CMS general purpose web framework
based on CherryPy framework, see [CPF]_. 
Such dependencies will be deprecated in a nearest future.

.. [CMS] http://cms.web.cern.ch/cms/
.. [LHC] http://en.wikipedia.org/wiki/Lhc
.. [DAS] https://twiki.cern.ch/twiki/bin/view/CMS/DMWMDataAggregationService
.. [CPF] http://www.cherrypy.org/
