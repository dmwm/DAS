.. _das_config:

DAS configuration file
======================

DAS configuration consists of a single file, $DAS_ROOT/etc/das.cfg. Its structure is
shown below:

.. doctest::

    [das]                    # DAS core configuration
    verbose = 0              # verbosity level, 0 means lowest
    parserdir = /tmp         # DAS PLY parser cache directory
    multitask = True         # enable multitasking for DAS core (threading)
    core_workers = 10        # number of DAS core workers who contact data-providers
    api_workers = 2          # number of API workers who run simultaneously
    thread_weights = 'dbs:3','phedex:3' # thread weight for given services
    error_expire = 300       # expiration time for error records (in seconds)
    emptyset_expire = 5      # expiration time for empty records (in seconds)
    services = dbs,phedex    # list of participated data-providers

    [cacherequests]
    Admin = 50               # number of queries for admin user role
    Unlimited = 10000        # number of queries for unlimited user role
    ProductionAccess = 5000  # number of user for production user role

    [web_server]             # DAS web server configruation parameters
    thread_pool = 30         # number of threads for CherryPy
    socket_queue_size = 15   # queue size for requests while server is busy
    host = 0.0.0.0           # host IP, the 0.0.0.0 means visible everywhere
    log_screen = True        # print log to stdout
    url_base = /das          # DAS server url base
    port = 8212              # DAS server port
    pid = /tmp/logs/dsw.pid  # DAS server pid file
    status_update = 2500     #
    web_workers = 10         # Number of DAS web server workers who handle user requests
    queue_limit = 200        # DAS server queue limit
    adjust_input = True      # Adjust user input (boolean)
    dbs_daemon = True        # Run DBSDaemon (boolean)
    dbs_daemon_interval = 300# interval for DBSDaemon update in sec
    dbs_daemon_expire = 3600 # expiration timestamp for DBSDaemon records
    hot_threshold = 100      # a hot threshold for powerful users
    onhold_daemon = True     # Run onhold daemon for queries which put on hold after hot threshold

    [dbs]                    # DBS server configuration
    dbs_instances = prod,dev # DBS instances
    dbs_global_instance = prod # name of the global DBS instance
    dbs_global_url = http://a.b.c # DBS data-provider URL

    [mongodb]                # MongoDB configuration parameters
    dburi = mongodb://localhost:8230 # MongoDB URI
    bulkupdate_size = 5000   # size of bulk insert/update operations
    dbname = das             # MongoDB database name
    lifetime = 86400         # default lifetime (in seconds) for DAS records

    [dasdb]                  # DAS DB cache parameters
    dbname = das             # name of DAS cache database
    cachecollection = cache  # name of cache collection
    mergecollection = merge  # name of merge collection
    mrcollection = mapreduce # name of mapreduce collection

    [loggingdb]
    capped_size = 104857600
    collname = db
    dbname = logging

    [analyticsdb]            # AnalyticsDB configuration parameters
    dbname = analytics       # name of analytics database
    collname = db            # name of analytics collection
    history = 5184000        # expiration time for records in an/ collection (in seconds)

    [mappingdb]              # MappingDB configuration parameters
    dbname = mapping         # name of mapping database
    collname = db            # name of mapping collection

    [parserdb]               # parserdb configuration parameters
    dbname = parser          # parser database name
    enable = True            # use it in DAS or not (boolean)
    collname = db            # collection name
    sizecap = 5242880        # size of capped collection

    [dbs_phedex]             # dbs_phedex configuration parameters
    expiration = 3600        # expiration time stamp
    urls = http://dbs,https://phedex # DBS and Phedex URLs

For up-to-date configuration parameters please see `utils/das_config.py`
