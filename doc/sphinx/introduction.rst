Introduction
============

DAS stands for *Data Aggregation System*. It provides a
a common layer above various data services, to allow end users
to query one (or more) of them with a more natural, 
search-engine-like interface. It is developed for the [CMS]_
High Energy Physics experiment on the [LHC]_, at CERN,
Geneva, Switzerland. Although it is currently only used for
the CMS experiment, it was designed to have a general-purpose
architecture which would be applicable to other domains.

It provides several features:

  1. a caching layer for underlying data-services
  2. a common meta-data look up tool for these services
  3. an aggregation layer for that meta-data

The main advantage of DAS is a uniform meta-data representation
and consequent ability to perform look-ups via free text-based queries.
The DAS internal data-format is JSON. All meta-data queried and stored
by DAS are converted into this format, according to a mapping between
the notation used by each data service and a set of common keys
used by DAS.

All documentation here is a replica of [DAS]_ documentation
available on the CERN Twiki.

Dependencies
------------
DAS is written in python and relies on standard python modules.
The design back-end is `MongoDB <http://www.mongodb.org>`_,
which provides *schema-less* storage and a generic query language.
Below you can see current set of dependencies for DAS within the CMS
environment:

.. figure::  _images/das_dependencies.png
   :align:   center

The *wmcore* and *wmcore-webtools* modules are a CMS general purpose 
web framework based on CherryPy. It is possible to run DAS both within
this framework and in an entirely standalone version using only
CherryPy.

Below we list all dependencies clarifying their role for DAS

- *python*, DAS is written in python (2.6), see [Python]_;
- *cherrypy*, a generic python web framework, see [CPF]_;
- *yui* the Yahoo YUI Library for building richly interactive web applications,
  see [YUI]_;
- *elementtree* and its *cElementTree* counterpart are used as generic XML parser in DAS,
  both implementations are now part of python (2.5 and above);
- *MongoDB*, a document-oriented database, the DAS DB back-ends, see [Mongodb]_
  and [MongodbOverview]_;
- *pymongo*, a MongoDB python driver, see [Pymongo]_;
- *yaml*, a human-readable data serialization format (and superset of JSON),
  a python YAML library is used for DAS maps and server configurations, see [YAML]_;
- *Cheetah*, a python template framework, used for all DAS web templates, see
  [Cheetah]_;
- *sphinx*, a python documentation library servers all DAS documentation, 
  see [Sphinx]_;
- *ipython*, an interactive python shell (optional, used in some admin tools),
  see [IPython]_;
- *cjson*, a C library providing a faster JSON decoder/encoder for python (optional), see
  [CJSON]_;

