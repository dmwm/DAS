DAS release notes
=================

Release 0.5.X series
--------------------
This release series is targeted to DAS stability. We redesigned DAS-QL
parser to be based on PLY framework; re-write DAS analytics; add benchmarking tools;
performed stress tests and code audit DAS servers.

- 0.5.7


  - Fix dbport/dbhost vs uri bug for das expert interface
  - Created new self-contained unit test framework to test CMS data-services

    - add new DASTestDataService class which represents DAS test integration
      web server
    - provide unit test against DAS test data web service
    - add new configuration for DASTestDataService server
    - perform queries against local DAS test data service, all queries can be
      persistent and adjusted in unittest
    - add fake dbs/phedex/sitedb/ip/zip services into DASTestDataService

  - remove all handlers before initialization of DASLogger
  - add NullHandler
  - add collection parameter to DAS core get_from_cache method
  - add unit test for web.utils
  - add delete_db_collection to mapping/analytics classes
  - remove obsolete templates, e.g. das_admin, mapreduce.
  - sanitize DAS templates, #545
  - Fix issues with showing records while applying DAS filters, #853
  - Move opensearch into das_opensearch.tmpl
  - Fix dbs/presentation maps
  - Add size_format function
  - Updated performance plot
  - make presentation layer more friendly, fixes #848, #879, #880
  - add new configuration parameter status_update, which allow to tune up
    DAS web server AJAX status update message (in msec) 
  - re-factor DAS web server code (eliminate unnecessary AJAX calls;
    implement new pagination via server calls, rather JS; make 
    form and all view methods to be internal; added check_data method;
    redesign AJAX status method)
  - Make admin tool be transparent to Ipython
  - Add new functions/unit tests for date conversion, e.g. to_seconds, next_day,
    prev_day
  - fix date issue with dashboard/runregistry services, fixes #888. Now user will
    be able to retrieve information for a certain date

- 0.5.6

  - add usable analytics system; this consists of a daemon (analytics_controller)
    which schedules tasks (which might spawn other tasks), several worker processes
    which actually perform these tasks and a cherrypy server which provides
    some information and control of the analytics tasks
  - the initial set of tasks are
  
    - Test - prints spam and spawns more copies of itself, as might be expected
    - QueryRunner - duplicates DAS Robot, issues a fixed query at regular intervals
    - QueryMaintainer - given a query, looks up expiry times for all associated 
      records and reschedules itself shortly before expiry to force an update
    - ValueHotspot - identifies the most used values for a given key, and
      spawns QueryMaintainers to keep them in the cache until the next analysis
    - KeyHotspot - identifies the most used query keys, and spawns ValueHotspot
      instances to keep their most popular values maintained in the cache
      
  - provides a cli utility, das_analytics_task allowing one-off tasks to be run
    without starting the analytics server
  - fix apicall records in analytics_db so that for a given set of all parameters
    except expiry, there is only one record
  - fix genkey function to properly compare dictionaries with different insert
    histories but identical content
  - alter analyticsdb query records to store an array of call times rather than
    one record per query, with a configurable history time
  - append "/" to $base to avoid /das?query patterns
  - Updates for analytics server, add JSON methods, add help section to web page
  - Analytics CLI
  - Add ability to learn data-service output keys, fixes #424
  - Add new class DASQuery
  - Add analytics server pid into analytics configuration
  - Prepend python to all shell scripts to avoid permission problem
  - fix dbs blockpath map
  - add new presentation layouts for various services
  - increase ajaxStatus lookup time
  - fix issue with date, in the case when date was specified as a range, e.g.
    date last 24h, the merge records incorrectly record the date value
- 0.5.5

  - fix map-reduce parsing using DAS PLY
  - introduce das_mapreduces() function which look-up MR functions in das.mapreduce
    collection
  - fixes for Tier0,DBS3 services
  - fix core when no services is available, it returns an empty result set
  - fix DAS parser cache to properly store MongoDB queries. By default
    MongoDB does not allow usage of $ sign in dictionary keys, since it is used
    in MongoQL. To fix the issue we encode the query as dict of key/value/operator
    and decode it back upon retrieval.
  - fix DAS PLY to support value assignment in filters, e.g.
    block | grep site=T1
  - Fixes for Dashboard, RunRegistry services
  - Eliminate DAS_PYTHONPATH, automatically detect DAS code location
  - Drop off ez_setup in favor python distutils, re-wrote setup.py to use only
    distutils
  - add opensearch plugin
  - fix issue with DAS PLY shift/reduce conflict (issue with COMMA/list_for_filter)
  - add to DAS PLY special keys, date and system, to allow queries like
    run date last 24h, jobsummary date last 24h. Prevent queires like
    run last 24h since it leads to ambuguous conditions.
  - add support for GridFS; parse2gridfs generator pass docs whose size less then
    MongoDB limit (4MB) or store doc into GridFS. In later case the doc in DAS
    workflow is replaced with gridfs pointer (issue #611)
  - add new method to DAS cache server to get data from GridFS for provided file id
  - fix DAS son manipulator to support gridfs_id
  - fix das_config to explicitly use DAS_CONFIG environment
  - fix bug with expire timestamp update from analytics
  - add support for "test" and "clean" action in setup.py;
    remove das_test in favor standard python setup.py test
  - add weighted producer into queryspammer toolkit; this allows to mimic
    real time behavior of most popular queries and ability to invoke
    DAS robots for them (up-coming)
  - fix #52, now both min and max das aggregators return _id of the record
  - return None as db instances when MongoDB is down
  - add avg/median functions to result object; modified result object to hold 
    result and rec counter; add helper das function to associate with each 
    aggreagators, e.g. das_min
  - drop dbhost/dbport in favor of dburi, which can be a list of MongoDB uris
    (to be used for connection with MongoDB replica sets)
  - replace host/port to URI for MongoDB specs, this will allow to specify
    replication sets in DAS config
  - use bson.son import SON to be compatible with newer version of pymongo
  - use col.count() vs col.find().count(), since former is O(1) operation wrt O(N)
    in later case

- 0.5.3 - 0.5.4 series

  - Clean-up %post and do not package docs over there
  - All names in bin are adjusted to one schema: das_<task>.
  - All scripts in bin are changed to use /bin/sh or 
    /bin/bash and use ${1+"$@"} instead of "$@"
  - bin area has been clean-up, e.g. das_doc, dassh is removed, etc.
  - Remove runsum_keys in runsum_service.py since it is obsolete code
  - Fix issue w/ root.close() for runsum_service.py (parser function)
  - Remove session from plotfairy
  - Remove encode4admin
  - Add urllib.quote(param) for das_services.tmpl and das_tables.tmpl
  - fix #446
  - das_jsontable.tmpl is removed since it's obsolete and no one is using it.
  - Remove das_help.tmpl and /das/help since it is obsolete
  - Remove das_admin.py since it is obsolete
  - Reviewed decorator in web/tools.py and commented out unused decorators, 
    exposexml, exposeplist. I want to keep them around upon they become relevant for DAS long terms.
  - Fix issue with wrap2das methods and made them internal.
  - Add checkargs decorator to validate input parameters for das_web
  - Change socket_queue_size to 100
  - Set engine.autoreload_on=False, request.show_tracebacks=False.
    Verified that server runs in production mode by default.
  - Add parameters validation for das_web/das_expert.
  - fix #493, allow relocation of PLY parsertab.py
  - fix #494, allow usage of HTTP Expires if data-services provide that
  - change eval(x) into eval(x, { "__builtins__": None }, {}) for those cases
    when fail to use json.load(x). Some data-service are not fully compliant
    and the issue with them need to be resolved at their end.
  - Use singleton class for Connection to reduce number of ESTABLISHED connections
    seeing on server. For details see 
    http://groups.google.com/group/mongodb-user/browse_thread/thread/67d77a62059568d7#
    https://svnweb.cern.ch/trac/CMSDMWM/ticket/529
  - use isinstance instead of types.typeXXX
  - make generic cern_sso_auth.py to authenticate with CERN SSO system
  - make das_map to accept external map dir parameter which specify locations
    of DAS maps
  - fix queryspammer to handle generators; add weights
  - unify DAS configuration via das_option
  - Remove das docs from RPM, will run it stand-alone elsewhere
  - Move checkargs into DAS.web.utils; reuse this decorator for all DAS servers
    to sanitize input arguments; added new unit test for it
  - Introduce DAS server codes, they resides in DAS.web.das_codes
  - Change DAS server behavior to return HTTPError. The passed message contains
    DAS server error code.
  - fix #525, #542.
  - fix issue with counting of empty records, #455
  - Handle the case when MongoDB is down. Both DAS servers can
    handle now outage of MongoDB either at start-up or during their
    operations. Adjust code to use a single mongodb host/port across all
    databases, fix #566
  - Remove from all unit test hardcoded value for mongodb host/port,
    instead use those from DAS configuration file
  - Use calendar.timegm instead of time.mktime to correctly convert 
    timestamp into sec since epoch; protect expire timestamp overwrite 
    if exires timestamp is less then local time
  - Add empty_record=0 into DAS records, to allow consistent look-up
  - Added DAS_PYTHONROOT, DAS_TMPLROOT, DAS_IMAGESROOT, DAS_CSSROOT, DAS_JSROOT
    to allow DAS code relocation

- 0.5.0 till 0.5.2

  - based on Gordon series of patches the following changes has been
    implemented

    - new analytics package, which keeps track of all input queries
    - new DAS PLY parser/lexer to confirm DAS QL
    - added new queryspammer tool

  - added spammer into DAS cache client, to perform benchmarking of
    DAS cache server
  - added a few method to DAS cache server for perfomance measurements
    of bare CherryPy, CherryPy+MongoDB, CherryPy+MongoDB+DAS
  - remove white/back list in favor of explicit configuration of
    DAS services via DAS configuration systems (both das.cfg and das_cms.py)
  - added index on das.expire
  - fixed issue with SON manipulator (conversion to str for das_id, cache_id)
  - enable checks for DAS key value patterns
  - added URN's to query record
  - added empty records into DAS merge to prevent cases when no results
    aggregated for user request

    - empty records are filtered by web interface
    - values for empty records are adjusted to avoid presence of special $ key,
      e.g. we cannot store to MongoDB records with {'$in': [1,2]}

  - new das_bench tool
  - fixed regex expression for DAS QL pattern, see 
    http://groups.google.com/group/mongodb-user/browse_thread/thread/8507223a70de7d51
  - various speed-up enhancements (missing indexes, empty records, regex bug, etc.)
  - added new RunRegistry CMS data-service
  - updated DAS documentation (proof-reading, DAS QL section, etc.)
  - remove src/python/ply to avoid overlap with system defaul ply and added
    src/python/parser to keep parsertab.py around

Release 0.4.X series
--------------------
The most significant part of this release is new plug-and-play mechanism
to add new data-services. This is done via data-service map creation. Each
map is represented data-service URI (URL, input parameters, API, etc.).

- 0.4.13 till 0.4.18

  - adjustment to CMS environment and SLA requirements
  - ability to read both cfg and CMS python configuration files
  - replacement of Admin to Expert interface and new authentication scheme
    via DN (user certificates) passed by front-end
  - new mongodb admin.dns collection
  - add PID to cherrypy das_server configuration

- 0.4.12

  - added unique filter
  - change value of verbose/debug options in all cli tools to be 0, instead
    of None, since it's type suppose to be int
  - add new example section to web FAQ
  - re-define logger/logformat in debug mode; the logger is used
    StreamHandler in this mode, while logformat doesn't use time stamp.
    This is usefull for DAS CLI mode, when --verbose=1 flag is used.
  - add "word1 word2" pattern to t_WORD for das_lexer, it's going to
    be used by searching keywords in cmsswconfig service and can be
    potentially used elsewhere to support multiple keywords per
    single DAS key
  - fix bug with apicall which should preceed update_cache
  - add simple enc/dec schema for DAS admin authentication
  - add logger configuration into das.cfg
  - separate logger streams into das.log, das_web.log and das_cache.log
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

