Dependencies
------------
DAS is written in python and relies on standard python modules.
The design back-end is `MongoDB <http://www.mongodb.org>`_,
which provides *schema-less* storage and a generic query language.


DAS depends on the following software:

- MongoDB and pymongo module
- libcurl library
- YUI library (Yahoo UI, version 2)
- python modules:

  - yajl (Yet Another JSON library) or cjson (C-JSON module)
  - CherryPy
  - Cheetah
  - PLY
  - PyYAML
  - pycurl

To install MongoDB visit [Mongodb]_ (make its bin directory
available in your path).
To install libcurl library visit [CURL]_.
To install YUI library, visit Yahoo developer web site [YUI]_ and install
*version 2* of their yui library.

To install python dependencies it is easier to use standard python installer
*pip* (see above).


.. rubric:: Dependencies in the CMS Environment

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
  a python YAML library is used for DAS maps and server configurations, see [YAML]_,
  [PyYAML]_;
- *Cheetah*, a python template framework, used for all DAS web templates, see
  [Cheetah]_;
- *sphinx*, a python documentation library servers all DAS documentation, 
  see [Sphinx]_;
- *ipython*, an interactive python shell (optional, used in some admin tools),
  see [IPython]_;
- *cjson*, a C library providing a faster JSON decoder/encoder for python (optional), see
  [CJSON]_;
- *yajl*, a C library providing a faster JSON decoder/encoder for python (optional), see
  [YAJL]_;
- *pycurl* and *curl*, python module for libcurl library, see [PyCurl]_, [CURL]_;
- *PLY*, python Lexer and Yacc, see [PLY]_;


