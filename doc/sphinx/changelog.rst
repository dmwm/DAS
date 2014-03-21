Release notes
=============

.. rubric:: Release 2.X.Y series

This release series is targeted to DAS production stability and quality.

- 2.6.X

  - Perform DAS/MongoDB separation
  - Remove analytics
  - Re-factor DASMaps management scripts, now we'll use
    das_js_fetch, das_js_validate, das_js_import, das_js_update
  - Re-factor DASMaps creation scripts, now we'll use
    das_create_json_maps, das_create_kws_maps

    - drop from KWS maps _id/oid feilds

  - Simplify hints templates
  - Remove db_monitor thread due to DAS/MongoDB separation
  - Re-factor bin/das_server script to support DAS/MongoDB separation
  - Add support for child dataset=X release=Y site=Z query
  - Add support for dataset parent=X release=Y site=Z query

- 2.5.X

  - Fixed 4166, 4165, 4164, 4162, 4160, 4158, 4156, 4155, 4153, 4148, 4142
  - Re-factor ReqMgr config queries to fetch configs from ReqMgr and WMStats
  - Add dataset suggestion hints when no results are found in default DBS
  - Fixed query disappearance under high load
  - Set DAS_SERVER in DAS/__init__.py module which will be used in HTTP
    User-Agent header. The DAS_SERVER uses version which will be updated upon
    deployment to specific release name. Therefore the User-Agent string will
    change once we deploy new DAS release. It has the following pattern:
    das-server/<DAS release>::python/<python version>, e.g.
    das-server/2.4.8::python/2.7
  - Change DASMapping to be singleton
  - Switch to <dbs_namespace>/<dbs_instance> schema for DBS3 maps, e.g.
    prod/global, int/global, prod/phys01, int/phys01
  - Reorganize DAS map upload (new scripts: das_create_json_maps,
    das_update_database)
  - Add support for Travis CI
  - Improve error handling, add DASHTMLParser to parse HTML content coming from
    data-providers (e.g. when service is down)
  - Change behavior of DAS CLI to show bytes as is and upon user request its
    representation in certain base, e.g. --base=2 will show MiB notations, etc.
  - Support DBS3 instances with slashes, e.g. prod/global, int/global
  - Extend list of supported DBS3 instances, in addition to global and
    phys0[1-3], I added instances with slashes, e.g. int/global. If instances
    does not have slash it is considered to come from prod DBS URL.

- 2.4.X

  - Re-factor RequestManager code to use internal store instead of MongoDB one
  - Re-evaluate racing conditions:

    - the clean-up worker should use lock to safely wipe out expired records
    - the remove_expired method should only care about given dasquery records
    - add additional delta when check expiration timestamp, it is required to
      protect code from situation when records can be wiped out during request
      operation

  - Fixed issues: 4098, 4097, 4090, 4095, 4093, 4089, 4085, 4082, 4081, 4079, 4077
  - Wrapped dasply parser call into spawn function, this fix intermittent
    problem with dasply under high load
  - Added spawn_manager along with its unit test
  - Re-factor code to use individual DB connections instead of sharing one in
    various places. This work address DAS instability issue reported in #4024.
  - Added cookie support in DAS client
  - Added support of DAS client version, it is configured via
    config.web_server.check_clients option

    - check DAS client version in DAS server
    - issue warning status and associative message for the client about version
      mismatch

  - Added code to re-initiate DAS request in check_pid, issue #4060
  - Prepared server/client for distributed deployment on cmsweb

- 2.3.X

  - Fix issue 4077 (don't yield record from empty MCM response)
  - Work on DAS deployment procedure

    - two set of DAS maps (one for production URLs and another for testbed)

  - Separate DAS web server with KWS one

    - run KWS on dedicated port (8214) with single-threaded DASCore

  - Assign type for records in mapping db; DASMapping code re-factoring to
    reduce latency of the queries
  - Fix query default assignment (issue #4055)
  - Provide ability to look-up config files used by ReqMgr for dataset
    creation (issue #4045)

    - Add config look-up for given dataset which bypass MCM data-service

  - Add clean-up worker for DAS caches; fixed #4050
  - Additional work on stability of DAS server under high-load (issue #4024)

    - add exhaust option to all MongoDB find calls
    - add index for mapping db
    - add DAS clean-up daemon, issue #4050
    - reduce pagination usage
    - increase MongoDB pool size, turn on auto_start_request and safe option
      for MongoClient
    - step away from db connection singleton

  - Add outputdataset API for ReqMgr data-service (issue #4043)
  - Fix python3 warnings

- 2.2.X

  - Fixed MCM prepid issue, switch to produces rest API
  - Merge and integrated KWS search

- 2.1.X

  - Make das-headers mandatory in das_client.py and keep --das-headers option
    and das_headers input parameter in get_data API for backward compatibility
  - Replaced all has_key dict calls with key in dict statement (work towards
    python3 standard)
  - Add check_services call to DAS core to check status of services
  - Pass write_concern flag to MongoClient, by default it is off
  - Fixed #4032
  - Re-factor core/web code to propagate error record back to end-user and
    setup error status code in this case
  - Throw error records when urlfetch_getdata fails
  - Move set_misses into write_to_cache
  - Made adjustments to DBS3 data-service based on recent changes of DBS3 APIs

- 2.0.X

  - Add services attribute to das part of data record, it shows which DAS
    services were used, while system attribute used to show which CMS systems
    were used to produce data record(s)
  - Turn off dbs_phedex, it producing too much load, instead use individual
    services
  - Re-evaluate lifetime of records in DAS cache: the clean-up should be done
    either for qhash/das.expire pair (less then current tstamp) or for records
    which live in cache long enough, via das.exire<tstamp-rec_ttl
  - Introduce dasdb.record_ttl configuration parameter int das config
  - Fix issue4023
  - Changes to allow DAS run with DBS2/DBS3 in a mix mode
  - Extend download LFN link to download web page, issue 4022
  - Add Status link to DAS header and let users to see status of DAS queue
  - Re-factor DASMapping code, see ticket 4021
  - Add support for mcm dataset=/a/b/c query; first it looks-up information
    from ReqMgr to get its info for given dataset, then it parse ReqMgr info
    and extracts PrepID and passes it to MCM data-service.
  - Add MCM links on dataset summary page when information
    is provided by reqmgr data-service (MC datasets)
  - Add code to support MCM (PREP) data-service (issue 3449),
    user can look-up mcm info by using the following query: mcm prepid=<PREP-ID>
  - Remove timestamp attribute from passed dict to md5hash function, it is
    required due to dynamic nature of timestamp which leads to modification of
    the hash of the record
  - Add new stress tool, see bin/das_stress_tool
  - Round timestamp for map records as well as for dasheader due to
    inconsistent behavior of json parsers, see note in jsonwrapper module
  - Fix issue4017: add hash to all DAS map records; add verification of hash
    into DASMapping check_maps method
  - Fix issue4016: add aux-record called arecord; arecord contains count of
    corresponding map record, map record type and a system. Adjust DASMapping
    check_maps method to perform full check of DAS maps by comparing count
    field from aux-record with actual number of maps in DAS mapping DB
  - Apply common set of indexes for both cache/merge collection to properly
    get/merge records
  - Allow runs DBS3 API to yield individual records
  - Support block tier=GEN-SIM date between [20120223, 20120224] query via
    blocksummaries DBS3 API
  - Switch from block_names to block_name as input parameter for blocksummaries
    DBS3 API; handle correctly incorrect values for dates in DBS3
    blocksummaries API
  - Fix issues 4014, 4013, 4009
  - Add lumi4block_run and dataset4block DBS3 APIs
  - fix run input parameter for all DBS3 APIs
  - Add runsummaries API



.. rubric:: Release 1.X.Y series

- 1.12.X

  - Fix wildcards to provide more informative messages in text mode
  - Fix issues: 3997, 3975
  - Replace phedex_tier_pattern with phedex_node_pattern
  - Get rid of empty_record,  query. Instead, introduce das.record with
    different codes. Codes are defined in utils/utils.py record_codes function.
    Add mongodb index on codes; modified queries to look-up das/data-records
    using new das.record field
  - Fix issue with ply_query parameter
  - Add extra slash to avoid one round trip
  - Work on support new run parameter w/ DBS3 APIs, now DAS is capable to use
    run-range/run-list queries into DBS3
  - Use json.dumps to printout JSON dict to stdout

- 1.11.X

  - Add support for block,run,lumi dataset=/a/b/c queries
  - Add plistlib python module w/ None modifications to handle DAS XML output
  - Add list of attributes for config output
  - Add summary4block_run API
  - Highlight unknown global tags in web UI
  - Re-factor the code: add insert_query_records which scan input DAS query and
    insert query records into DAS cache, then it yields list of acknowledged
    data-services which used by call API for data retrieval
  - Extend incache API to work with query or data records by providing
    query_record flag with default value of False (check data records)
  - Take care of potential failure of PLY parser. Use few trials on given input
    and then give-up
  - Fix bug in task manager when I mix-up return type of spawn function which
    cause task fails under race conditions
  - Add support for summary dataset=/a/b/c query without run conditions
  - Add support for run range in DBS2 summary dataset/run query
  - Add expand_lumis helper function into das aggregators which flatten lumi
    lists, e.g. [[1,3], [5,7]] into [1,2,3,5,6,7]. This allows correctly count
    number of lumis in DAS records
  - Implement support for comp-ops queries, e.g.
    find run, lumi for given dataset and optional run range
    find file, lumi for given dataset and optional run range
    find file, lumi, run for given dataset and optional run range
    this work is done via new urlfetch_getdata module

- 1.10.X

  - Add urlfetch_pycurl module to fetch content from multiple urls
  - Use custom db_monitor which check MongoDB connection as well as periodically
    reload DAS maps
  - Add preliminary support for file block=/a/b/c#123 runs site
    query (must have urlfetch proxy)
  - Allow user to get DBS file into regardless of its status, ticket 3992
  - Add indexes for file.name,dataset.name.block.name and run.run_number in DAS
    cache collection to prevent error on sorting entities
  - Add support for block dataset run in/between [1,2] query, ticket 3974
  - Apply file.name index to allow MongoDB to sort the files, ticket 3988
    this is required in rare case when number of files is very large and
    MongoDB give up on sorting without the index. I may apply similar index on
    block as well since their number in dataset can be large as well.
  - Add constrain on block name for lumi block=/a/b/c#123 queries, ticket 3977
  - Add pyurlfetch client
  - Add proxy_getdata to request data from external urlproxy server, ticket
    3986; should be used to fetch data concurrently
  - Add support for file dataset=/a/b/c run in [1,2,3] site=T2_CH_CERN, ticket
    3982 (requires external urlproxy server, see 3986)
  - Split fakeDatasetSummary into fakeDatasetPattern and fakeDatasetSummary to
    support look-up of valid datasets for given pattern and any dataset info
    for givan dataset path; ticket 3990
  - Add draft code to accommodate file dataset=/a/b/c run in [1,2,3] site=X
    query (still under development)
  - Add url_proxy module which can work with pyurlfecth or Go proxy server
  - Add get_proxy, proxy_getdata and implementation (still experimental) of
    proxy usage within DBS3 module
  - Re-wrote update_query_record API; update ctime for query records
  - Separte insertion of query and data records
  - Remove analytics calls from abstract service, current analytics
    implementation require full re-design, it does not make any good so far
  - Add distinguishing message in ticket issue title for no apis/no results
    errors
  - Add fakeFiles4BlockRun API to cover file block=/a/b/c#123 run in [1,2,3]
    queries required by CMSSW Integration Builds (IB).
  - Fix file block=/a/b/c#123 query (DBS should contribute to it)
  - Add dataset pattern constratins for all DBS/DBS3 queries
  - Remove listLFNs since listFiles cover the use case to look-up file for a given dataset
  - Add filelumis4dataset API to support file,lumi dataset=/a/b/c queries
  - Add support for run IN [1,2,3] queries, this will be allowed in DBS/DBS3,
    CondDB, RunRegistry data-services
  - Upgrade to Prototype.js 1.7
  - Remove lumi API from CondDB mapping; add lumi API to RunRegistry mapping;
    clean-up RunRegistry code and remove v2 APIs, the v3 is default now
  - Re-factor Vidmantas code: move wild-card errors into separate template;
    sanitize template parameters; clean-up code
  - Add das_exceptions module, move all Wild-card excepion into this module
  - Imrove web UI links with box_attention for submitting DAS tickets, ticket
    #3969

- 1.9.X

  - Fix ticket #3967 (preserve DAS records order while removing duplicates)
  - Fix ticket #3966 (strip-off zero in das filters)
  - Add JS function to handle Event (hide DAS keys window) via ESC
  - Resolve double counting issue, ticket #3965
  - Add Show DAS keys description to web UI
  - Wrap combined_site4dataset API call into try/except block and show
    exception on web UI. This will help to catch transient missing values from
    combined data-service for site dataset=/a/b/c queries.
  - Add DASKEY EQUAL VALUE VALUE error condition to DAS PLY parser to cover the
    case when user cut-and-paste some value and it has empty space, e.g.
    dataset=/a/b/c om
  - Always use upper() for DBS status since it is stored in upper-case in DBS
    DB
  - Add function to print DAS summary records
  - Add DAS SERVER BUSY message to web server, ticket #3945
  - Read prim_key from mapping DB rather then lookup_keys in das_mongocache
    module (with fallback to lookup_keys)
  - Fix verbose printout for pycurl_manager module
  - Add support for summary dataset=/a/b/c run=123, ticket #3960
  - Re-factor das_client to be used in other python application; change return
    type from str to json in get_data API; add das-headers flag to explicitly
    ask for DAS headers, by default drop DAS headers
  - Re-factor dasmongocache code to support multiple APIs responses
    for single DAS key
  - Add api=das_core to dasheader when we first register query record
  - Extend DAS aggregator utility to support multiple APIs repsonse
    for single DAS key
  - Add db_monitor threads to DASMapping/DASMongocache classes
  - Switch from explicit show|hide links to dynamic show/hide which
    switch via ToggleTag JS function
  - Adjust web UI with Eric's suggestions to show service names in color
    boxes; remove DAS color map line in result output
  - Revert to base 10 in size_format
  - Add update_filters method to DASQuery class to allow upgrade its filters
    with spec keys; this is useful on web UI, when end-user specifies a filter
    and we need to show primary key of the record
  - Wrote check_filters function to test applied filters in a given query and
    invoke it within nresults method, ticket #3958
  - Collapse lumi list from DBS3, ticket #3954
  - Remove dbs url/instances from DAS configuration and read this information
    directly from DAS maps; fixed #3955

- 1.8.X

  - Add support of lumi block=/a/b/c#123 and block file=/path/f.root
    queries both in DBS and DBS3
  - Do not check field keys in a query, e.g. allow to get partial results
  - Fix plain web view when using DAS filters
  - Extend DAS support for file dataset=/a/b/c run between [1,2] queries
  - Keep links around even if data service reports the error
  - Catch error in combined data-service and report them to UI
  - Protect qxml_parser from stream errors
  - Convert regex strings into raw strings
  - Separate curl cache into get/post instances to avoid racing condition
    for cached curl objects
  - Convert das timestamp into presentation datetime format
  - Queue type can be specified via qtype parameter in web section of DAS
    configuration file
  - Extend task_manager to support PriorityQueue
  - Revert default to cjson instead of yajl module, since later contains a bug
    which incorrectly rounds off large numbers; there is also an outstanding
    issue with potential memory leak
  - Remove dataset summary look-up information for dataset pattern queries to
    match DBS2 behavior and reduce DAS/DBS latency, see 9254ae2..86138bd
  - Replace range with xrange since later returns generator rather than list
  - Add capability to dump DAS status stack by sending SIGQUIT signal to DAS
    server, e.g. upon the following call `kill -3 <PID>` DAS server will dump
    into its logs the current snapshot of all its threads
  - Apply Vidmantas wildcard patch to improve usage of dataset patterns
    on web UI
  - Fix Phedex checksum parsing
  - Switch to new PyMongo driver, version 2.4

    - change Connection to MongoClient
    - remove safe=True for all insert/update/remove operation on
      mongo db collection, since it is default with MongoClient

  - DAS CLI changes:
    
    - Add exit codes
    - Add --retry option which allows user to decide if s/he wants to
      proceed with request when DAS server is busy; retry follows log^5 function
    - Set init waiting time to 2 sec and max to 20 sec; use cycle for sleep
      time, e.g. when we reach the max drop to init waiting time and start
      cycle again.  This behavior reduce overall waiting time for end-users

  - Fix issue with DBS3 global instance look-up
  - Switch to HTML5 doctype
  - New schema for DAS maps

    - re-factor code to handle new schema
    - change all maps/cms_maps according to new schema
    - add new documentation for new schame, see mappings.rst

  - Add support to look-up INVALID files in DBS2/DBS3
  - Enable dbs_phedex combined engine
  - Add new thread module to deal with threads in DAS
  - Switch from low-level thread.start_new_thread to new DAS thread
    module, assign each thread a name
  - Properly handle MongoDB connection errors and print out nice
    output about their failure (thread name, time stamps, etc.)

- 1.7.X

  - Switch from PRODUCTION to VALID dataset access type in DBS3
  - Adjust das_core and das_mongocache to optionally use dasquery.hashes

    - hashes can be assigned at run-time for pattern queries, e.g.
      dataset=/*abc*
    - hashes can be used to look-up data once this field is filled up

  - Let DBSDaemon optionally write dataset hashes, this can be used to enhance
    dataset pattern look-up in DAS cache, see ticket #3932
  - Add hashes data member and property to DASQuery class
  - Work on DBS3 APIs
  - Fix issue with forward/backward calls in a browser which cause existing
    page to use ajaxCheckPid. I added reload call which enforces browser to
    load page content with actual data

    - revisit ajaxCheckPid and check_pid functions. Removed ahash, simplify
      check_pid, use reload at the end of the request/check_pid handshake

  - Add fakeDataset4Site DBS2 API to look-up datasets for a given site, ticket
    #3084

    - DBS3 will provide new API for that

  - Change DAS configuration to accept web_service.services who lists
    local DAS service, e.g. dbs_phedex, dbs_lumi
  - Modify dbs_phedex service to initialize via DAS maps
  - Add lumi_service into combined module
  - Introduced services mapping key
  - Adjust combined map file to use services mapping key
  - Switch to pycurl HTTP manager, which shows significant performance boost
  - Work on pycurl_manager to make it complaint with httplib counterpart

- 1.6.X

  - Add new logging flag to enable/disable logging DAS DB requests into logging
    db (new flag is dasdb.logging and its values either True or False)
  - Change pymongo.objectid to bson.objectid, pymongo.code to bson.code since
    pymongo structure has been changed (since 2.2.1 pymongo version)
  - Introduce new dataset populator tool which should fetch all DBS
    datasets and keep them alive in DAS cache (not yet enabled)
  - Move DAS into github.com/dmwm organization
  - Extend das_dateformat to accept full timestamp (isoformat); provide set of
    unit tests for das_dateformat; fix web UI to accept date in full isoformat
    (user will need to provide quotes around timestamp, e.g.
    '20120101 01:01:01'); fixes #3931
  - Set verbose mode only when parserdb option is enabled

- 1.5.X

  - Add SERVICES into global scope to allow cross service usage, e.g.
    site look-up for DBS dataset records
  - Add site look-up for user based datasets, ticket #3432
  - Revisit onhold daemon and cache requests flaw

      - Start onhold daemon within init call (ensure MongoDB connection)
      - Check DAS cache first for CLI requests regardless if pid presence in a request
      - Put requests on hold only if user exceeds its threshold and server is busy,
        otherwise pass it through

  - Set DAS times, ticket #3758
  - Convert RR times into DAS date format (isoformat)
  - Fix ticket #3796

- 1.4.X

  - Move code to github
  - Fix bug in testing for numbers, SiteDB now contains unicode entries
  - Add HTTP links into record UI representation
  - Call clean-up method upon request/cache web methods.
  - Add htlKeyDescription, gtKey into RunRegistry, ticket #3735
  - Improve no result message, ticket #3724
  - Update error message with HTTPError thrown by data-provider, ticket #3718
  - Fix das_client to proper handle DAS filters, ticket #3706
  - Change Error to External service error message, see ticket #3697
  - Skip reqmgr API call if user provide dataset pattern, ticket #3691
  - Enable cache threshold reading via SiteDB group authorization
  - Add support for block dataset=/bla run=123 query, ticket #3688
  - Fix tickets #3636, #3639

- 1.3.X

  - Add new method for SiteDB2 which returns api data from DAS cache
  - Add parse_dn function to get user info from user DN
  - Add new threshold function which parse user DN and return threshold
    (it consults sitedb and look-up user role, if role is DASSuperUser it
    assigns new threshold)
  - Add suport_hot_threshold config parameter to specify hot threshold for super users
  - Extend check_pid to use argument hash (resolve issue with
    compeing queries who can use different filters)
  - Do not rely on Referrer settings, ticket #3563
  - Fix tickets #3555, #3556
  - Fix plain view, ticket #3509
  - Fix xml/json/plain requests via direct URL call
  - Clean-up web server and checkargs
  - Add sort filer to web UI
  - Add sort filter, users will be able to use it as following
    file dataset=/a/b/c | sort file.size,
    file dataset=/a/b/c | sort file.size-
    The default order is ascending. To reverse it, user will need to add
    minus sign at the end of the sort key, e.g. file.size-
  - Re-factor code to support multiple filters. They now part of DASQuery
    object. All filters are stored as a dict, e.g. {'grep': <filter list>,
    'unique': 1, 'sort': 'file.size}
  - Add sitedb links for site/user DAS queries
  - Re-factor code which serves JS/CSS/YUI files; reduce number of client/server
    round-trips to load those files on a page
  - fix ddict internal loop bug
  - add representation of dict/list values for given key attributes, e.g.
    user will be able to select block.replica and see list of dicts on web page

- 1.2.X

  - Pass instance parameter into das_duplicates template, ticket #3338
  - Add qhash into data records (simplify their look-up in mongocache manager)
  - Simplify query submission for web interface (removed obsolete code from
    web server)
  - Fix issue with sum coroutines (handle None values)
  - Avoid unnecessary updates for DAS meta-records
  - Made das core status code more explicit
  - Remove ensure_index from parser.db since it's capped collection
  - Made QLManager being a singleton
  - Add safe=True for all inserts into das.cache/merge collection to avoid
    late records arrival in busy multithreaded environment
  - Add trailing slash for condDB URL (to avoid redirection)
  - Show data-service name in error message
  - Show dataset status field
  - Add support to pass array of values into DAS filter, ticket #3350
    but so far array needs to consist of single element (still need to fix PLY)
  - Update TFC API rules (fix its regex in phedex mapping)
  - Init site.name with node.name when appropriate
  - Fill admin info in new SiteDB when user look-up the site
  - Switch to new SiteDB
  - Switch to new REST RunRegistry API
  - Remove dbs instance from phedex subscription URL and only allow DBS global link, ticket #3284
  - Fix issue with invalid query while doing sort in tableview (ticket #3281)
    discard qhash from the tableview presentation layer
  - Implement onhold request queue. This will be used to slow down users
    who sequentially abuse DAS server. See ticket #3145 for details.
  - Add qhash into DASquery __str__
  - Fix issue with downloading config from gridfs, ticket 3245
  - Fix DBS run in query with wide run range, use gte/lte operators instead
  - Fix issue with recursive calls while retrieve dict keys
  - Eliminate duplicates in plain view, ticket 3222
  - Fix fakeFiles4DatasetRunLumis API call and check its required parameters
  - Fix plain view with filter usage, ticket #3216
  - Add support for dataset group=X site=T3_XX_XXXX or
    dataset group=X site=a.b.com queries via blockreplicas Phedex API, ticket #3209
  - Fix IP look-up for das_stats, ticket #3208
  - Provide match between various SiteDB2 APIs in order to build combined record
  - Remove ts field and its index from das.cache collection, it is only needed for das.merge
  - Work on integration with new SiteDB, ticket #2514
  - Switch to qhash look-up procedure, ticket #3153
  - Fix DBS summary info, ticket #3146
  - Do not reflect request headers, ticket #3147
  - Fix DBSDaemon to work with https for DBS3
  - Add ability to DAS CLI to show duplicates in records, ticket #3120
  - Parse Phedex checksum and split its value into adler32/checksum, ticket #3119, 3120
  - Remove from dataset look-up for a given file constrain to look-up
    only VALID datasets, when user provide a file I need to look-up
    dataset and provide its status, ticket #3123
  - Resolved issue with duplicates of competing, but similar queries at web UI.
  - Changed task manager to accept given pid for tasks.
  - Generated pid at web layer; check status of input query in a cache and
    find similar one (if found check status of similar request and generate
    results upon its completion); moved check_pid code from web server into
    its one template; adjusted ajaxCheckPid call to accept external method
    parameter (such that I can use different methods, e.g. check_pid and
    check_similar_pid)
  - Fixed several issues with handling StringIO (delivered by pycurl)

- 1.1.X

  - Extend not equal filter to support patterns, ticket #3078
  - Reduce number of DAS threads by half (the default values for workers was too high)
  - Name all TaskManagers to simplify their debugging
  - Configure number of TaskManager for DASCore/DASAbstractService via
    das configuration file
  - Fix issue with data look-up from different DBS instances (introduce
    instance in das part of the record), ticket #3058
  - Switch to generic DASQuery interface. A new class is used as a placeholder
    for all DAS queries. Code has been refactored to accept new DASQuery interface
  - Revisited analytics code based on Gordon submission: code-refactoring;
    new tasks (QueryMaitainer, QueryRunner, AnalyticsClenup, etc);
    code alignment with DAS core reorganization, ticket #1974
  - Fix issue with XML parser when data stream does not come from data-service,
    e.g. data-service through HTTP error and DAS data layer creates HTTP JSON record
  - Fix bug in db_monitor who should check if DB connection is alive and reset DB cursor, ticket #2986
  - Changes for new analytics (das_singleton, etc.)
  - Add new tool, das_stats.py, which dumps DAS statistics from DAS logdb
  - Add tooltip template and tooltips for block/dataset/replica presence; ticket #2946
  - Move creation of logdb from web server into mongocache (mongodb layer);
    created new DASLogdb class which will responsible for logdb;
    add insert/deletion records into logdb;
    change record in logdb to carry type (e.g. web, cache, merge) and
    date (in a form of yyyymmdd) for better querying
  - add gen_counter function to count number of records in generator
    and yield back records themselves
  - add support for != operator in DAS filters and precise match of
    value in array, via filter=[X] syntax, ticket #2884
  - match nresults with get_from_cache method, i.e. apply similar techniques
    for different types of DAS queries, w/ filters, aggregators, etc.
  - properly encode/decode DAS queries with value patterns
  - fix issue with system keyword
  - allow usage of combined dbs_phedex service regardless of DBS,
    now works with both DBS2 and DBS3
  - Fix unique filter usage in das client, 
    add additions to convert timestamp/size into human readable format, ticket #2792
  - Retire DASLogger in favor of new PrintManager
  - code re-factoring to address duplicates issue; ticket #2848
  - add dataset/block/replica presence, according to ticket #2858; made changes to maps

- 1.0.X

  - add support for release file=lfn query, ticket #2837
  - add creation_time/modification_time/created_by/modified_by into DBS maps, ticket #2843
  - fix duplicates when applying filters/aggregators to the query, tickets #2802, #2803
  - fix issue with MongoDB 2.x index lookup (error: cannot index parallel arrays).
  - test DAS with MongoDB 2.0.1
  - remove IP lookup in phedex plugin, ticket #2788
  - require 3 slashes for dataset/block pattern while using fileReplicas API, ticket #2789
  - switch DBS3 URL to official one on cmsweb; add dbs3 map into cms_maps
  - migrate from http to https for all Phedex URLs; ticket 2755
  - switch default format for DAS CLI; ticket 2734
  - add support for 'file dataset=/a/b/c run=1 lumi=80' queries both in DBS2/DBS3, ticket #2602
  - prohibit queries with ambiguos value for certain key, ticket #2657
  - protect filter look-up when DAS cache is filled with error record, ticket #2655
  - fix makepy to accept DBS instance; ticket #2646
  - fix data type conversion in C-extension, ticket #2594
  - fix duplicates shown in using DAS CLI, ticket #2593
  - add Phedex subscription link, fixes #2588
  - initial support for new SiteDB implementation
  - change the behavior of compare_spec to only compare specs with
    the same key content, otherwise it leads to wrong results when
    one query followed by another with additional key, e.g.
    file dataset=abc followed by file dataset=abc site=X. This lead
    compare_spec to identify later query as subset of former one, but
    cache has not had site in records, ticket #2497
  - add new data retrieval manager based on pycurl library;
    partial resolution for ticket #2480
  - fix plain format for das CLI while using aggregators, ticket 2447
  - add dataset name to block queries
  - add DAS timestamp to all records; add link to TC; fixes #2429, #2392
  - re-factor das web server, and put DAS records representation on web UI
    into separate layer. Create abstract representation class and current
    CMS representation. See ticket 1975.

.. rubric:: Release 0.9.X series

- 0.9.X

  - change RunRegistry URL
  - fix issue with showing DAS error records when data-service
    is down, see ticket #2230
  - add DBS prod local instances, ticket 2200
  - fix issue with empty record set, see tickets #2174, 2183, 2184
  - upon user request highlight in bold search values;
    dim off other links; adjust CSS and das_row template, ticket #2080
  - add support for key/cert in DAS map records, fixes #2068
  - move DotDict into stand-alone module, fixes #2047
  - fix block child/parent relationship, tickets 2066, 2067
  - integrate DAS with FileMover, add Download links to FM for file records,
    ticket #2060
  - add filter/aggragator builder, fixes #978
  - remove several run attributes from DBS2 output, since this information
    belong to CondDB and is not present in DBS3 output
  - add das_diff utility to check merged records for inconsistencies.
    This is done during merge step. The keys to compare are configurable
    via presentation map. So far I enable block/file/run keys and
    check for inconsistencies in size/nfiles/nevents in them
  - replace ajax XHR recursive calls with pattern matching and
    onSuccess/onException in ajaxCheckPid/check_pid bundle
  - walk through every exception in a code and use print_exc as a
    default method to print out exception message. Adjust all
    exception to PEP 3110 syntax
  - code clean-up
  - replace traceback with custom print_exc function which prints all
    exceptions in the following format: msg, timestamp, exp_type,
    exc_msg, file_location
  - remove extra cherrypy logging, clean-up DAS server logs

.. rubric:: Release 0.8.X series

- 0.8.X

  - resolve double requests issue, ticket #1881, see discussion on HN
    https://hypernews.cern.ch/HyperNews/CMS/get/webInterfaces/708.html
  - Adjust RequestManager to store timestamp and handle stale requests
  - Make DBSDaemon be aware of different DBS instances, ticket #1857
  - fix getdata to assign proper timestamp in case of mis-behaved data-services
    ticket #1841
  - add dbs_daemon configuration into DAS config, which handles DBS
    parameters for DBSDaemon (useful for testing DBS2/DBS3)
  - add TFC Phedex API
  - add HTTP Expires handling into getdata
  - made a new module utils/url_utils.py to keep url related functions in
    one place; remove duplicate getdata implementation in combined/dbs_phedex
    module
  - add dbs_daemon whose task to fetch all DBS dataset; this info
    is stored into separte collection and can be used for autocompletion mode
  - improve autocompletion
  - work on scalability of DAS web server, ticket #1791

.. rubric:: Release 0.7.X series

This release series is targeted to DAS usability. We collected users
requests in terms of DAS functionality and usability. All changes made
towards making DAS easy to use for end-users.

- 0.7.X

  - ticket #1727, issue with index/sort while geting records from the cache
  - revisit how to retrieve unique records from DAS cache
  - add DAS query builder into autocomplete
  - extend refex to support free-text based queries
  - add DBS status keyword to allow to select dataset with different statuses in
    DBS, the default status is VALID, ticket #1608
  - add datatype to select different type of data, e.g. MC, data, calib, etc.
  - if possible get IP address of SE and create appropriate link to ip service
  - calculate run duration from RR output
  - add conddb map into cms_maps
  - add initial support for search without DAS keywords
  - apply unique filter permanently for output results
  - add help cards to front web page to help users get use with DAS syntax
  - work on CondDB APIs
  - fix issue with IE
  - turn off multitask for analytics services
  - add query examples into front-page
  - get file present fraction for site view (users want to know if
    dataset is completed on a site or not)
  - fix PLY to accept y|n as a value, can be used to check openness of the block
  - add create_indexes into das_db module to allow consistenly create/ensure
    indexes in DAS code

.. rubric:: Release 0.6.X series

This release series is targeted towards DAS production version. We switched from
implicit to explicit data retrieval model; removed DAS cache server and re-design
DAS web server; add multitasking support.

- 0.6.5

  - handle auto-connection recovery for DBSPhedexService
  - fix site/se hyperlinks

- 0.6.4

  - create new DBSPhedexService to answer the dataset/site quesitions.
    it uses internal MongoDB to collect info from DBS3/Phedex data-services
    and map-reduce operation to extract desired info.

- 0.6.3

  - support system parameter in DAS queries, e.g.
    block block=/a/b/c#123 system=phedex
  - add condition_keys into DAS records, this will assure that look-up conditions
    will be applied properly. For instance, user1 requested dataset site=abc release=1
    and user2 requested dataset site=abc. The results of user1 should not be shown
    in user2 queries since it is superset of previous query. Therefore each cache
    look-up is supplemented by condition_keys
  - add suport for the following queries:
    dataset release=CMSSW_4_2_0 site=cmssrm.fnal.gov
    dataset release=CMSSW_4_2_0 site=T1_US_FNAL
  - add new combined DAS plugin to allow combined queries across different
    data services. For instance, user can request to find all datasets at
    given Tier site for a given release. To accomplish this request I need
    to query both DBS/Phedex. Provided plugin just do that.
  - add new method/tempalte to get file py snippets
  - re-factor code which provide table view for DAS web UI
  - add new phedex URN to lookup files for a given dataset/site
  - put instance as separate key into mongo query (it's ignored everywhere except DBS)
  - work on web UI (remove view code/yaml), put dbs instances, remember
    user settings for view/instance on a page
  - add physics group to DBS2 queries
  - add support to look-up of sites for a given dataset/block
  - allow to use pattern in filters, e.g. block.replica.site=*T1*
  - add filters values into short record view
  - add links to Release, Children, Parents, Configs into dataset record info
  - add support to look-up release for a given dataset
  - add support to look-up cofiguration files for given dataset
  - add fakeConfig, fakeRelease4Dataset APIs in DBS2
  - add support for CondDB
  - add hyperlinks to DAS record content (support only name, se, run_number), ticket #1313
  - adjust das configuration to use single server (remove cache_server bits)
  - switch to single server, ticket #1125

    - remove web/das_web.py, web/das_cache.py

  - switch to MongoDB 1.8.0

- 0.6.2

  - das config supports new parameters queue_limit, number_of_workers)
  - add server busy feature (check queue size vs nworkers, reject requests above
    threashold), ticket #1315
  - show results of agg. functions for key.size in human readable format, e.g. GB
  - simplify DASCacheMgr
  - fix unique filter #1290
  - add missing fakeRun4File API to allow look-up run for a given file, fixes #1285
  - remove 'in' from supported list of operator, users advised to use
    'between' operator
  - DBS3 support added, ticket #949
  - fix #1278
  - fix #1032; re-structure the code to create individual per data-srv
    query records instead of a single one. Now, each request creates
    1 das query record plus one query record per data-srv. This allows
    to assign different expire timestamp for data-srv's and achieve
    desired scalability for data-service API calls.
  - re-wrote task_manager using threads, due to problems with multiprocessing
    modules
  - re-wrote cache method for DAS web servers to use new task_manager
  - adjust das_client to use new type of PID returned by task_manager upon
    request. The PID is a hash of passed args plus time stamp
  - bump to new version to easy distinguish code evolution

- 0.6.1

  - replace gevent with multiprocessing module
  - add task_manager which uses multiprocessing module and provides
    the same API as gevent

- 0.6.0

  - code refactoring to move from implicit data look-up to
    explicit one. The 0.5.X series retieved all data from multiple sources 
    based on query constrains, e.g. dataset=/a/b/c query cause to get 
    datasets, files, block which match the constraint. While new code
    makes precise matching between query and API and retrieve only selected
    data, in a case above it will retrieve only dataset, but not files.
    To get files users must explicitly specify it in a query, e.g.
    file dataset=/a/b/c
  - constrain PLY to reject ambiguos queries with more then one
    condition, without specifying selection key, e.g.
    dataset=/a/b/c site=T1 is not allowed anymore and proper exception will be
    thrown. User must specify what they want to select, dataset, block, site. 
  - protect aggregator functions from NULL results
  - new multiprocessing pool class
  - use gevent (if present, see http://www.gevent.org/) to handle data retrieval concurently
  - switch to YAJL JSON parser
  - add error_expire to control how long expire records live in cache, fixes #1240
  - fix monitor plugin to handle connection errors

.. rubric:: Release 0.5.X series

This release series is targeted to DAS stability. We redesigned DAS-QL
parser to be based on PLY framework; re-write DAS analytics; add benchmarking tools;
performed stress tests and code audit DAS servers.

- 0.5.11

  - change RunRegistry API
  - fix showing result string in web UI when using aggregators
  - bug fix for das_client with sparse records
  - add new das_web_srv, a single DAS web server (not enabled though)
  - fix das_top template to use TRACE rather then savannah

- 0.5.10

  - add DAS cache server time into the web page, fixes #941
  - remove obsolete yuijson code from DAS web server
  - use DASLogger in workers (instead of DummyLogger) when verbosity level is on.
    This allows to get proper printouts in debug mode.
  - fix bug in compare_specs, where it was not capable to identify
    that str value can be equal to unicode value (add unittest for that).
  - classified logger messages, move a lot of info into debug
  - change adjust_params in abstract interface to accept API as well
  - adjust DBS2 plugin to use adjust_params for specific APIs, e.g. listPrimaryDatasets,
    to accept other parameters, fix #934 
  - add new DAS keyword, parent, and allow parent look-up for dataset/file via
    appropriate DBS2 APIs
  - extend usage of records DAS keyword to the following cases

    - look-up all records in DAS cache and apply conditions, e.g.
      records | grep file.size>1, file.size<10
    - look-up all records in DAS cache regardless of their content (good/bad records),
      do not apply das.empty_record condition to passed empty spec

  - Fix filter->spec overwrite, ticket #958
  - Add cache_cleaner into cache server, its task is periodically clean-up
    expired records in das.cache, das.merge, analytics.db
  - Fix bug in expire_timestamp
  - Remove loose query condition which leads to pattern look-up (ticket #960)
  - Fix but in das_ply to handle correctly date

    - add new date regex
    - split t_DATE into t_DATE, t_DATE_STR

  - add support for fake queries in DBS plugin to fake non-existing DBS API
    via DBS-QL
  - remove details from DSB listFiles
  - add adjust_params to phedex plugin
  - adjust parameters in phedex map, blockReplicas can be invoked with passed dataset
  - update cms_maps with fake DBS2 APIs 
  - add DAS_DB_KEYWORDS (records, queries, popular)
  - add abstract support to query DAS (popular) queries, a concrete implementation
    will be added later
  - fix #998
  - fix SiteDB maps
  - fix host parameter in das_cache_client
  - remove sys.exit in das_admin to allow combination of multiple options together
  - fix compare_specs to address a bug when query with value A is considered as
    similar to next query with value A*
  - fix get_status to wait for completion of DAS core workflow
  - fix merge insert problem when records exceed MongoDB BSON limit (4MB), put
    those records into GridFS
  - fix nresults to return correct number of found results when applying a filter,
    e.g. monitor | grep monitor.node=T3_US_UCLA
  - replace listProcessedDatasets with fakeDatasetSummary, since it's better suits
    dataset queries. DBS3 will provide proper API to look-up dataset out of provided
    dataset path, release, tier, primary_dataset.
  - fix listLFNs to supply file as primary key
  - comment out pass_api call to prevent from non-merge situation, must revisit the code

    - fix issue with missing merge step when das record disapper from cache

  - bug fix to prevent from null string in number of events
  - increase expire time stamp for dashboard, due to problem described in 1032 ticket. 
    I need to revisit code and make das record/service rather then combined one to 
    utilize cache better. Meanwhile align expire timestamp wrt to DBS/Phedex
  - add DBS support to look-up file via provided run (so far using fake API)
  - use fakseDataset4Run instead of fakeFile4Run, since it's much faster. Users 
    will be able to find dataset for a given run and then find files for a
    given dataset
  - fix issue with JSON'ifying HTTP error dict
  - replace DAS error placement from savannah to TRAC
  - add new special keyword, instance, to allow query results from local
    DBS instances. The keyword itself it neutral and can be applied to any
    system. Add new abstract method url_instance in abstract_service which
    can be used by sub-systems to add actual logic how to adjust sub-system
    URL to specific instance needs.
  - replace connection_monitor with dascore_monitor to better handle connection/DASCore
    absense due to loosing connection to MongoDB
  - propagate parser error to user, adjust both DAS cache/web servers
  - fix queries with date clause, ticket #1112
  - add filter view to show filtered data in plain/text, ticket #959
  - add first implementation of tabular representation, ticket #979, based on YUI
    DataSource table with dynamic JSON/AJAX table feeder
  - add jsonstreamer
  - add cache method to web server (part of future merge between cache/web servers)
  - add das_client which talks to web server; on a web server side made
    usage of multiprocessing module to handle client requests. Each request
    spawns a new process.
  - visualize record's system by colors on web UI, ticket #977
  - add child/parent look-up for dataset/files
  - work on DAS PLY/web UI to make errors messages more clear, especially adjust to
    handle DBS-QL queries
  - added dbsql_vs_dasql template which guides how to construct DAS QL expressions
    for given DBS QL ones
  - fix concurrency problem/query race conditions in DAS core
  - remove fakeListFile4Site from DBS maps since DBS3 does not cover this use case
  - modified das_client to allow other tools use it as API
  - fix DBS/phedex maps to match dashes/underscores in SE patterns
  - add adjust_params into SiteDB to allow to use patterns in a way SiteDB does it
    (no asterisks)
  - disable expert interface
  - update analytics in DAS core when we found a match

- 0.5.9

  - fix issue with <,> operators and numeric valus in filters
  - add tier into DBS listProcessedDatasets API as input parameter, so user
    can query as "dataset primary_dataset=ZJetToEE_Pt* tier=*GEN*"
  - DBS2 API provides typos in their output, e.g. primary_datatset, processed_datatset,
    add those typos into DAS map to make those attributes queriable.
  - Add lumi into DBS map, as well as its presentation UI keys

- 0.5.8

  - Finish work to make presentation layer more interactive, ticket #880

    - create hyperlinks for primary DAS keys
    - round numbers for number of events, etc.
    - present file/block size in GB notations

  - add new "link" key into presentation to indicate that given key
    should be used for hyperlinks
  - add reverse look-up from presentation key into DAS key
  - add cache for presentation keys in DAS mapping class
  - update DAS chep paper, it is accepted as CMS Note CR-2010/230
  - fix issue with similar queries, e.g. dataset=/a/b/c is the same as
    dataset dataset=/a/b/c
  - improve presentation layer and add links

      - replace link from boolean to a list of record in presentation YML file
      - the link key in presentation now refers to list of records, where each
        record is a dict of name/query. The name is shown on a web UI under the Links:,
        whiel query represents DAS query to get this value, for example
        {"name":"Files", "query":"file dataset=%s"}

  - fix issue with counting results in a cache
  - make dataset query look-up close to DD view, fixes #821
  - add YAJL (Yet Another JSON Library) as experimental JSON module, see
    http://lloyd.github.com/yajl/ and its python binding. 
  - add keylearning and autocompletion, ticket #50
  - add parse_filter, parse_filters functions to parse input list of filters,
    they used by core/mongocache to yield/count results when filters are passed
    DAS-QL. This addresses several Oli use cases when multiple filters will
    be passed to DAS query, e.g.
    file dataset=/a/b/c | grep file.size>1, file.size<100
  - add special DAS key records, which can be used to look-up records regarless
    of condition/filter content, e.g. the DAS query site=T1_CH_CERN only shows
    site records, while other info can be pulled to DAS. So to look-up all records
    for given condition user can use records site=T1_CH_CERN
  - remove obsolete code from das_parser.py

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

.. rubric:: Release 0.4.X series

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

.. rubric:: Release V03 series

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


.. rubric:: Release V02 series

This release series is based on MongoDB. After a long evaluation of
different technologies, we made a choice in favor of MongoDB.

- added support for map/reduce
- switch to pipes syntax in QL for aggregation function support
- switch DAS QL to free keyword based syntax

.. rubric:: Release V01 series

Evalutaion series. During this release cycle we played with
the following technologies:

- Memcached
- CouchDB
- custom file-based cache

At that time DAS QL was based on DBS-QL syntax.
During this release series we added DAS cache/web servers;
made CLI interface.

