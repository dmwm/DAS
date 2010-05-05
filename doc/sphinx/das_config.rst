.. _das_config:

DAS configurations
==================

.. toctree::
   :maxdepth: 2

DAS configuration file
----------------------

DAS configuration consists of a single file, $DAS_ROOT/etc/das.cfg. Its structure is
shown below:

.. doctest::

    [cache_server]           # DAS Cache server configuration parameters
    thread_pool = 30         # number of threads for CherryPy
    socket_queue_size = 15   # queue size for requests while server is busy
    n_worker_threads = 4     # number of threads for DAS workers
    host = 0.0.0.0           # host IP, the 0.0.0.0 means visible everywhere
    log_screen = True        # print log to stdout
    port = 8211              # server port

    [web_server]             # DAS web server configruation parameters
    thread_pool = 30         # number of threads for CherryPy
    socket_queue_size = 15   # queue size for requests while server is busy
    host = 0.0.0.0           # host IP, the 0.0.0.0 means visible everywhere
    log_screen = True        # print log to stdout
    port = 8212              # server port

    [mongodb]                # MongoDB configuration parameters
    dbhost = localhost       # MongoDB host name
    dbport = 27017           # MongoDB port number
    dbname = das             # MongoDB database name
    bulkupdate_size = 5000   # size of bulk insert/update operations
    attempt = 3              # number of attempts to connect to MongoDB
    capped_size = 104857600  # size of capped collection (logs), in bytes
    lifetime = 86400         # default lifetime (in seconds) for DAS records

    [analytics_db]           # AnalyticsDB configuration parameters
    dbport = 27017           # MongoDB port number
    dbhost = localhost       # MongoDB host name
    dbname = analytics       # MongoDB database name
    attempt = 3              # number of attempts to connect to MongoDB

    [mapping_db]             # MappingDB configuration parameters
    dbport = 27017           # MongoDB port number
    dbhost = localhost       # MongoDB host name
    dbname = mapping         # MongoDB database name
    attempt = 3              # number of attempts to connect to MongoDB

    [das]                    # DAS core configuration
    logformat = %(levelname)s %(message)s # DAS logger format
    rawcache = DASMongocache # class name for raw cache
    verbose = 0              # verbosity level, 0 means lowest

    [security]               # DAS security configuration parameters
    mount_point = /das/auth  # mount point for authentication
    group = das              # group name
    enabled = True           # enable/disable security module
    oid_server = http://a.ch # hostname of OpenID server  
    site =                   # site configuration name
    role =                   # role configuration name
    store_path = /tmp        # path for cookie storage
    session_name = SecModule # name of the session


