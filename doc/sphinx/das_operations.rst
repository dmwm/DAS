DAS operations
==============

.. toctree::
   :maxdepth: 2

Running DAS services
--------------------

DAS comes with 2 services

- DAS cache server
- DAS web server

Both can either reside on a single node or can run on dedicated machines.
Please refer to :ref:`DAS CMS operations <cms_operations>` section for deployment instructions
and :ref:`DAS configuration <das_config>` section for description of configuration
parameters.

By CMS conventions DAS uses the following ports

- 8211 for DAS cache server
- 8212 for DAS web server

In order to start each of server you need to setup your environment, see
:ref:`setup.sh <setup.sh>` file. For that use the following steps:

.. doctest::

    cd /data/projects/das/ # change accordingly
    source setup.sh

Setting up DAS maps
-------------------
Each data-service is registered with DAS via its DAS map. All maps are located
in $DAS_ROOT/src/python/DAS/services/maps area. We provide a single admin script
to install available maps. It is located in $DAS_ROOT/bin/ area which should be
in your path once your setup your environment *source setup.sh*. Then you can simply
call *das_map* and it will install all the maps specified in your services configuration.

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

DAS cache server
----------------

The DAS cache is multi-threaded server. When it starts 
you'll see that it runs 2*N+1 python processes, where 
N refers to number of CPUs on a node. 1 thread dedicated to
request queue while other 2*N to DAS workers. 

DAS server can be start as following:

.. doctest::

    das_server cache start|stop|status|restart

The das_cacheserver should be in your path once you setup your CMS DAS
environment, see :ref:`setup.sh <setup.sh>`, otherwise please locate it under
$DAS_ROOT/bin/ area.

DAS web server
--------------

DAS web server can be started independently from DAS cache server using the 
following command

.. doctest::

    das_server web start|stop|status|restart

The das_web should be in your path once you setup your CMS DAS
environment, see :ref:`setup.sh <setup.sh>`, otherwise please locate it under
$DAS_ROOT/bin/ area.

DAS administration
------------------

DAS RPMs provide a set of tools for administration tasks. 
They are located at $DAS_ROOT/bin.

- das_cacheclient is a CLI interface to DAS, it sends request to DAS cache server;
- das_server is a DAS server init script;
- das_cli is DAS stand-along CLI tool, it doesn't require neither cache or web DAS servers;
- das_code_quality.sh is a bash script to check DAS code quality. It is based on pylint
  tool, see [PYLINT]_.
- das_config is a tool to create DAS configuration file;
- das_map is a tool to create DAS maps;
- das_mapreduce is a tool to create map/reduce function for DAS;

