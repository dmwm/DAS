DAS release notes
=================

Release V04 series
------------------
The most significant part of this release is new plug-and-play mechanism
to add new data-services. This is done via data-service map creation. Each
map is represented data-service URI (URL, input parameters, API, etc.).

- 0.4.12

  - das_lexer supports floats
  - Add ability for filter to select specific values, e.g.
    run=123 | grep PD=MinBias
    right now only equal condition is working, in future may
    extend into support of other operators
  - add CMSSW release indexer

- 0.4.11

  - adjust abstract data-service and mongocache to use DAS compliant
    header if it is supplied by DAS compliant API, e.g. Tier0.
  - added cmsswconfigs data-service
  - work on xml_parser to make it recursive. Now it can handle nested
    children.
  - Fix problem with multiple look-up keys/API, by using api:lookup_keys
    dict. This had impact on storage of this information within das part
    of the record. Adjust code to handle it properly
  - added map for Tier0 monitoring data-service
  - fix problem with id references for web interface
  - fix problem with None passed into spec during parsing step

- 0.4.10

  - added new mapping for Phedex APIs
  - work on aggregator to allow merged records to have reference to
    their parent records in DAS cache, name them as cache_id
  - improve DAS admin interface:

    - show and hide various tasks
    - DAS tasks (query db, clean db, das queries)
    - Add digest authentication to admin interface, based on
      cherrypy.tools.digest_auth

  - allow to use multiple aggregators at the same time, e.g.
    site=T1_* | count(site.id), sum(site.id), avg(site.id)
  - enable aggregators in DAS core
  - migrated from CVS to SVN/GIT
  - added AJAX interface for DAS query look-up in admin interface
  - bug fix in core to get status of similar queries
  - validate web pages against XHTML 1.0, using http://validator.w3.org/check

- V0.4.9

  - update admin interface (added query info)
  - integrate DAS lexer in to DAS parser
  - add new class DASLexer, which is based on [PLY]
  - remove >, <, >=, <= operators from a list of supported ones, since
    they don't make sense when we map input DAS query into underlying
    APIs. The API usually only support = and range operators. Those
    operators are supported by MongoDB back-end, but we need more
    information how to support them via DAS <-> API callback
  - work on DAS parser to improve error catching of unsupported
    keywords and operators
  - split apart query insertion into DAS cache from record insertion to
    ensure that every query is inserted. The separation is required since
    record insertion is a generator which may not run if result set is
    empty
  - synchronized expire timestamp in DAS cache/merge/analytics db's

- V0.4.8

  - fix pagination
  - display DAS key for all records on the web to avoid overlap w/
    records coming out from multiple data-providers (better visibility)
  - protect DASCacheMgr with queue_limit configurable via das.cfg
  - found that multiprocess is unrealiable (crash on MacOSX w/ python
    version from macports); some processes become zombies. Therefore
    switch to ThreadPool for DAS cache POST requests
  - added ThreadPool
  - work on DBS2 maps
  - make monitoring_worker function instead of have it inside of
    DASCacheMgr
  - re-factor DASCacheMgr, now it only contains a queue
  - switch to use <major>.<minor>.<release> notations for DAS version
  - switch to use dot notation in versions, the setup.py/ez_tools.py
    substitute underscore with dash while making a tar ball

- V04_00_07

  - re-factor DAS configuration system
  - switch to pymongo 1.5.2
  - switch to MongoDB 1.4
  - added admin web interface; it shows db info, DAS config, individual
    databases and provide ability to look-up records in any collection

- V04_00_06

  - added support for proximity results
  - resolve issue with single das keyword provided in an input query
  - dynamically load of DAS plugins using __import__ instead of eval(klass)
  - first appearance of analytics code
  - fix issue with data object look-up
  - switch to new DAS QL parser

- V04_00_05

  - re-wrote DAS QL parser
  - move to stand-alone web server (remove WebTools dependency)
  - adjust web UI

- V04_00_04

  - choose to use flat-namespace for DAS QL keys in DAS queries
  - added aggregator functions, such as sum/count, etc. as coroutines
  - added "grep" filer for DAS QL
  - extended dotdict class with _set/_get methods
  - re-wrote C-extension for dict_helper
  - added wild_card parameter into maps to handle data-service with
    specific wild_card characters, e.g. \*, %, etc.
  - added ability to handle data-service HTTPErrors. The error records
    are recorded into both DAS cache and DAS merge collection. They will
    be propagated to DAS web server where admin view can be created to
    view them

- V04_00_02, V04_00_03

  - bug fix releases

- V04_00_01

  - minor tweaks to make CMS rpms
  - modifications for init scripts to be able to run in
    stand-alone mode

- V04_00_00
  - incorporate all necessary changes for plug-and-play
  - modifications for stand-alone mode

Release V03 series
------------------

Major change in this release was a separation of DAS cache into 
independent cache and merge DB collection. The das.cache collection
stores *raw* API results, while das.merge keeps only merged records.

- V03_00_04

  - minor changes to documentation structure

- V03_00_03

  - added DAS doc server
  - added sphinx support as primary DAS documentation system

- V03_00_02

  - work on DAS cli tools

- V03_00_01

  - bug fixes

- V03_00_00

  - separate DAS cache into das.cache and das.merge collections


Release V02 series
------------------

This release series is based on MongoDB. After a long evaluation of
different technologies, we made a choice in favor of MongoDB.

- added support for map/reduce
- switch to pipes syntax in QL for aggregation function support
- switch DAS QL to free keyword based syntax

Release V01 series
------------------

Evalutaion series. During this release cycle we played with
the following technologies:

- Memcached
- CouchDB
- custom file-based cache

At that time DAS QL was based on DBS-QL syntax.
During this release series we added DAS cache/web servers;
made CLI interface.

