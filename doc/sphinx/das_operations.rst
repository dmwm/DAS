DAS operations
==============

.. toctree::
   :maxdepth: 2

Running DAS services
--------------------

DAS consists of multi-threaded DAS web and keyword search KWS servers.
Please refer to :ref:`DAS CMS operations <cms_operations>` section for deployment instructions
and :ref:`DAS configuration <das_config>` section for description of configuration
parameters.

By CMS conventions DAS uses the following ports

- 8212 for DAS web server
- 8214 for DAS KWS server

In order to start each of server you need to setup your environment, see
:ref:`setup.sh <setup.sh>` file. For that use the following steps:

.. doctest::

    cd /data/projects/das/ # change accordingly
    source setup.sh

Setting up DAS maps
-------------------
Data-services are registered via the service mappings (DAS Maps) which define
the relationships between the services and how their inputs or outputs shall
be transformed.

To initialize or update the DAS maps (along with other metadata) call the
bin/das_update_database with location of maps. The CMS maps are located in
$DAS_ROOT/src/python/DAS/services/cms_maps directory.

 .. doctest::
    das_update_database $DAS_ROOT/src/python/DAS/services/cms_maps [production]


Apache redirect rules
---------------------

Here we outline Apache redirect rules which can be used to serve
DAS services. Please note we used localhost IP, 127.0.0.1 for 
reference, which should be substituted with actual hostname of
the node where DAS services will run.

Rewrite rules for apache configuration file, e.g. httpd.conf

.. doctest::

    # Rewrite rules
    # uncomment this line if you compile apache with dynamic modules
    #LoadModule rewrite_module modules/mod_rewrite.so
    # uncomment this line if you compile your modules within apache
    RewriteEngine on

    # DAS rewrite rules
    RewriteRule ^(/das(/.*)?)$  https://127.0.0.1$1 [R=301,L]

    Include conf/extra/httpd-ssl.conf

Rules for SSL rewrites:

.. doctest::

    RewriteRule ^/das(/.*)?$ http://127.0.0.1:8212/das/$1 [P,L]

MongoDB server
--------------

DAS uses `MongoDB <http://www.mongodb.org>`_ as its back-end for
Mapping, Analytics, Logging, Cache and Merge DBs.

RPM installation will supply its proper configuration 
file. You must start cache back-end before starting the DAS cache server. 

MongoDB server operates via standard UNIX init script:

.. doctest::

    $MONGO_ROOT/etc/profile.d/mongo_init.sh start|stop|status

The MongoDB database is located in $MONGO_ROOT/db, while logs are in 
$MONGO_ROOT/logs. 

DAS web server
--------------

DAS web server is multi-threaded server. It can be started using the 
following command

.. doctest::

    das_server start|stop|status|restart

The das_server should be in your path once you setup your CMS DAS
environment, see :ref:`setup.sh <setup.sh>`, otherwise please locate it under
$DAS_ROOT/bin/ area.

It consits of N threads used by DAS web server, plus M threads used by DAS core to
run data retrieval processes concurrently.
DAS configuration has the following parameters

.. doctest::

    config.web_server.thread_pool = 30
    config.web_server.socket_queue_size = 15
    config.web_server.number_of_workers = 8
    config.web_server.queue_limit = 20

The first two (thread_pool and socket_queue_size) are used by underlying CherryPy
server to control its internal thread pool and queue, while second pair (number_of_workers
and queue_limit) are used by DAS core to control number of worker threads used internally.
The DAS core multitasking can be turned off by using 

.. doctest::

    config.das.multitask = False

parameter.

DAS administration
------------------

DAS RPMs provide a set of tools for administration tasks. 
They are located at $DAS_ROOT/bin.

- das_server is a DAS server init script;
- das_cli is DAS stand-along CLI tool, it doesn't require neither cache or web DAS servers;
- das_code_quality.sh is a bash script to check DAS code quality. It is based on pylint
  tool, see [PYLINT]_.
- das_config is a tool to create DAS configuration file;
- das_js_fetch script fetches DAS maps from remote github repository
- das_js_validate script validates DAS maps
- das_js_import script imports DAS and KWS maps into MongoDB
- das_js_update script fetches, validate and import DAS and KWS maps into
  MongoDB
- das_maps_yml2json is a tool that generates json DB dumps from YML dasmaps.
- das_mapreduce is a tool to create map/reduce function for DAS;
- das_stress_test script provides ability to perform stress tests against DAS
  server
