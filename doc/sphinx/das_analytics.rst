.. _das_analytcs:

DAS analytics
=============
DAS analytics subsystem provides a task scheduler for running analytic
or maintenance tasks at regular intervals on the DAS server.

analytics_controller
--------------------

The main class of the analytics system is analytics_controller. This is invoked
with ``das_analytics [options] [config file(s)]``. All options can be
specified either in a configuration file (executed as python source, so global variables
in the file become configuration options) or specified on the command line (``--help``
will list the available options). Options on the command line take precedence. Jobs
can only be specified in a command line. As currently implemented the server blocks
rather than daemonises (this appears to cause conflicts with multiprocessing) but
this may be changed in future.

The analytics_controller owns a task scheduler and webserver, and provides logging
facilities for all analytics components.

scheduler
---------

The scheduler owns a pool of worker processes, and submits jobs from an internal
queue to the pool at their submission time. The jobs then run and on completion
will either be automatically rescheduled based on their requested interval, can
request a custom resubmission time and can also create new child tasks. Jobs that
fail are automatically retried a small number of times before being abandoned. A
minimum submission interval is enforced to try and prevent badly configured jobs
performing a DoS attack.

webserver
---------

The analytics webserver is run by a thread owned by the controller, and provides
a couple of pages showing the currently scheduled jobs, the results of previously
completed instances, log output from the server. Some limited control functionality
is provided, such as inserting new jobs, removing existing ones from the queue
(although jobs cannot be stopped once they are running in a worker process), 
and rescheduling jobs for earlier or later.

The webserver maintains internal fixed length queues of log entries and job results.
When the queue is filled, old entries are dropped. This might be better handled with
a capped mongo collection in future than python deques.

It is intended that task results be able to specify a custom display template
for their output to be displayed in, but currently only the raw dictionary is shown.

tasks
-----

An analytics task is a python class which will be run at regular intervals by the
analytics server. Tasks receive a set of standard arguments at init in addition to
those specified when the job was configured, then are called with no arguments to run.
They should then return a dictionary containing any information they wish to make
visible on the webserver, along with special arguments such as a requested resubmission
time or new tasks they wish to spawn.

Tasks are specified in a python configuration file like::

  Task(name, classname, interval, **kwargs)

where *kwargs* include any task-specific arguments necessary, *classname* is the name
of a class (and the containing file) in DAS.analytics.tasks and *name* is a display
name for the task.

Additionally, you can supply special kwargs *only_once*, *only_before*,
*max_runs* to limit the task running.

When the task is called, it receives a kwargs dictionary containing

:logger:  a logger with methods info, debug, warning, error, critical as per the
          logging package (but not identical, since it is a custom class that passes log
          messages through a pipe back to the controller process)
:DAS:  a DASCore instance (currently one-per-process is created, in future this
       may change to a single global instance to avoid connection overhead, rationale
       for one-per-job is that a separate logger can be supplued in each case)
:taskid:  uuid of this task
:name:  name of this task
:index:  run number of this task
:interval:  interval of the task
   
plus any *kwargs* added when the task was defined.

The following special values are understood in the return dictionary:

:resubmit:  set to False to disable resubmission
:next:  set to a (GMT) time to be resubmitted then instead of after the
        normal interval
:new_tasks:  set to a list of dictionaries to create those tasks. Generally,
             child tasks should be created with 'only_before' set to the next anticipated
             run of this task, since no interaction between parents and children in currently
             supported

standalone tasks
----------------

Tasks can be run independently of the analytics scheduler with the script
*das_analytics_task*. This will run the task as normal and report if new tasks
were requested or resubmission altered, but obviously no new tasks will
actually be spawned. Useful for testing or running one-off jobs. Example, to
run the test task::

    das_analytics_task -c Test -o "{'message': 'hello world'}"

implemented tasks
-----------------

Test
~~~~

:message:  Some text to print

Designed to test the scheduler, this will print a message each time it runs then randomly
prevent resubmission, submit extra copies of itself, raise exceptions, etc etc.

QueryRunner
~~~~~~~~~~~

:query:  A query in text or mongo dictionary format (passed to DASCore::call)

This is a simple query issuer that runs at a fixed interval, making a call to DAS.
This will guarantee the data is in the cache at call time, but not that it is renewed
(so it may expire 30 seconds later).

QueryMaintainer
~~~~~~~~~~~~~~~

:query:  A query in mongo *storage* format (ie, ``spec: [{'key':..., 'value':...}]``).
         This should be a simple, single-argument query with no modifiers, eg
         ``{'fields': None, 'spec': [{'key':'dataset.name', 'value':'xyz'}]}``
:preempt:  Time before data expiry it should be renewed. Default is 5 minutes. If the
           analytics server is busy (all workers filled), jobs may not run at submission
           time.
           
QueryMaintainer performs a (currently non-atomic) update of the requested query each
time it runs, looks up the earliest expiry time of the resulting data, and reschedules
itself to run next *preempt* seconds before this time.

The update is currently performed by calling *remove_from_cache* and then *call*, but this
should be replaced by an atomic update when possible.

It may also be worth producing an analogous class for individual API calls, since some
data services will have different expiry times and replacing everything after the first
expiry may not be optimum behaviour.

HotspotBase
~~~~~~~~~~~

:fraction:  proportion of items to maintain, interpreted different depending on *mode*
:mode:  item selection metric, currently supports
      
        :calls:  select the items representing the top *fraction* of all calls made
        :keys:  select the top *fraction* of all items, sorted by number of calls made
        :fixed:  select the first *fraction* items, sorted by number of calls made
                 (in this mode *fraction* should be > 1)
                  
:period:  period for consideration, default 1 month
:allowed_gap:  length of gap in summary documents that can be ignored (should be
               << *period* and < *interval*)
               
HotspotBase is a base class that should not be instantiated directly. It provides
a framework for analysis where short periods of time are analysed, to produce some
sort of item->count mapping, and then these summary documents are averaged over a 
much longer period to determine the most popular items in this period according to
some metric, which can then be used to inform pre-fetch strategies.

Users should implement::

    generate_task(self, item, count, epoch_start, epoch_end)
        
        Return a new task dictionary for the selected item, eg
        a QueryMaintainer task if queries are the items being selected
        
    make_one_summary(self, start, finish)
    
        Return an item->count mapping for the start and end times specified
    
and may also implement::

    report(self)
    
        Return a dictionary of extra keys to go in the return value
    
    preselect_items(self, items)
    
        Remove unwanted keys (eg, those containing wildcards if you want to
        not consider them), then return items
    
    mutate_items(self, items)
    
        Perform any merging of selected keys, eg determining which queries
        are supersets of each other, then return items

ValueHotspot
~~~~~~~~~~~~

:key:  A DAS key that we want to examine the values of, eg *dataset.name*
:preempt:  Passed to spawned *QueryMaintainer* tasks
:allow_wildcarding:  Whether to include values containing wildcards
:find_supersets:  Attempt to find superset queries of selected queries **experimental**

ValueHotspot considers the values given to a particular DAS key (no-condition queries are
ignored). Each time it runs, it performs a moving average over the past *period*
(default 1 month, from *HotspotBase*) to find the most requested *fraction* of values.

For each of these values, a QueryMaintainer task is spawned, with *only_before* set to
the next run of the *ValueHotspot* instance.

Hence, the most popular queries for a given DAS key are kept in the cache.

KeyHotspot
~~~~~~~~~~

:child_interval:  Interval for spawned tasks
:child_preempt:  Passed to spawned tasks as *preempt*

KeyHotspot is intended for very infrequent running (eg daily or less), to determine
which DAS keys are currently being most used.

For each of the most used keys, a ValueHotspot instance is spawned to run until the next
run of this instance, which will in turn spawn QueryMaintainers to keep the most
popular values for that query in the cache.
