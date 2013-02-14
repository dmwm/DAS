DAS server
==========
DAS server is multi-threaded application which runs within CherryPy web
framework. It consists of the following main componens:

- DAS web server
- DAS core engine, which by itself consist of

  - DAS abstract data-service
  - Data-provider plugins (data-service implementations)

- DAS analytics daemons
- DAS command-line client
- Data-provider daemons, e.g. DBS daemon, etc.
- Various monitors, e.g. MongoDB connection monitor, etc.
- MongoDB

Upon user request, the front-end validates user input and pass it to DAS web
server. It decompose user query into series of requests to underlying core
engine, who by itself invokes multiple APIs to fetch data from data-provider
and place them into MongoDB. Later this data are serverd back to the user and
stay in cache for period of time determined by data-providers, see Figure:

.. figure:: _images/das_server.png

Thread structure of DAS server
------------------------------

Below we outline a typical layout of DAS server threads:

- 1 main thread
- 1 HTTP server thread (CherryPy)
- 1 TimeoutMonitor thread (CherryPy)
- 1 dbs_phedex_monitor thread (dbs_phedex combined service)
- 1 dbs_phedex worker thread (dbs_phedex combined service)
- 1 lumi_service thread (lumi service)
- :math:`N_{CP}` CherryPy threads
- :math:`N_{DAS}` worker threads, they are allocated as following:

  - :math:`N_{web}` threads for web workers
  - :math:`N_{core}` threads for DAS core workers
  - :math:`N_{api}` threads for DAS service APIs

In addition DAS configuration uses das.thread_weights parameter to weight
certain API threads. It is defined as a list of srv:weight pairs where
each service gets :math:`N_{api}\timesweight` number of threads.

Therefore the total number of threads is quite hight (range in first hundred)
and it is determined by the following formula

.. math::

    N_{threads} = N_{main} + N_{CP} + N_{DAS}

    N_{DAS} = N_{web} + N_{core} + N_{api}

:math:`N_{main}` equals to sum of main, timeout, http, dbs_phedex and lumi threads
:math:`N_{CP}` is defined in DAS configuration file, typical value is 30
:math:`N_{web}` is defined in DAS config file, see web_server.web_workers
:math:`N_{core}` is defined in DAS config file, see das.core_workers
:math:`N_{api}` is defined in DAS config file, see das.api_workers

For example, usig the following configuration parameters

.. doctest::

    [das]
    multitask = True         # enable multitasking for DAS core (threading)
    core_workers = 10        # number of DAS core workers who contact data-providers
    api_workers = 2          # number of API workers who run simultaneously
    thread_weights = 'dbs:3','phedex:3' # thread weight for given services
    das.services = dbs,phedex,dashboard,monitor,runregistry,sitedb2,tier0,conddb,google_maps,postalcode,ip_service,combined,reqmgr

    [web_server]             # DAS web server configruation parameters
    thread_pool = 30         # number of threads for CherryPy
    web_workers = 10         # Number of DAS web server workers who handle user requests
    dbs_daemon = True        # Run DBSDaemon (boolean)
    onhold_daemon = True     # Run onhold daemon for queries which put on hold after hot threshold

we get the DAS server running with 151 threads

.. math::

    N_{main}=7, \; N_{CP}=30, \; N_{DAS}=114

where :math:`N_{DAS}` has the following breakdown

.. math::

    N_{web}=20, \; N_{core}=60, \; N_{api}= 34

here we calculated :math:`N_{api}` as following: we have 13 services, each of
them uses 2 API workers (as specified in das configuration), but dbs and phedex
data-services are weighed with weight 3, therefore the total number of dbs and
phedex workers is 6, respectively. To sum up the numbers we have: 11 services
with 2 API workers plus 6 workers for dbs and 6 workers for phedex.

Debugging DAS server
--------------------

There is nice way to get a snapshot of current activity of DAS server by
sending SIGUSR1 signal to DAS server, e.g. upon executing `kill -SIGUSR1 <PID>`
command you'll get the following output in DAS log

.. doctest::

    # Thread: DASAbstractService:dbs:PluginTaskManager(4706848768)
    File: "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/threading.py", line 524, in __bootstrap
      self.__bootstrap_inner()
    File: "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/threading.py", line 551, in __bootstrap_inner
      self.run()
    File: "/Users/vk/CMS/GIT/github/DAS/src/python/DAS/utils/task_manager.py", line 39, in run
      task = self._tasks.get()
    File: "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/Queue.py", line 168, in get
      self.not_empty.wait()
    File: "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/threading.py", line 243, in wait
      waiter.acquire()
    ....
    .... and similar output for all other DAS threads
    ....
