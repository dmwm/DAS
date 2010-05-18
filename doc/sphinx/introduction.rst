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
Below you can see current set of dependencies for DAS within CMS
environment:

.. figure::  _images/das_dependencies.png
   :align:   center

The wmcore and wmcore-webtools is a CMS general purpose web framework
based on CherryPy framework. Please note that DAS can
run both within CMS WMCore web framework as well as in stand-alone
version based on CherryPy.

Below we list all dependencies clarifying their role for DAS

- *python*, DAS is written in python, see [Python]_;
- *cherrypy*, a generic python web framework, see [CPF]_;
- *yui* the Yahoo YUI Library for building richly interactive web applications,
  see [YUI]_;
- *elementtree* and its *cElementTree* counterpart is used as generic XML parser in DAS,
  both implementations are part of python 2.5 and above;
- *MongoDB* a document-oriented database, the DAS DB back-ends, see [Mongodb]_
  and [MongodbOverview]_;
- *pymongo* a MongoDB python driver, see [Pymongo]_;
- *yaml* a human-readable data serialization format, a python YAML library is 
  used for DAS maps and server configurations, see [YAML]_;
- *Cheetah* is python template framework, used for all DAS web tempaltes, see
  [Cheetah]_;
- *sphinx* a python documentation library servers all DAS documentation, 
  see [Sphinx]_;
- *ipython* is an interactive reach python shell (optional, used in some admin tools),
  see [IPython]_;
- *cjson* a C-version of JSON decoder/encoder for python (optinonal), see
  [CJSON]_;

