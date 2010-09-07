DAS logging DB
==============
The DAS logging DB holds information about all requests made to DAS. All records are stored in a
`capped collection <http://www.mongodb.org/display/DOCS/Capped+Collections>`_ of MongoDB.
Capped collections are fixed sized collections that have a very high performance auto-LRU 
age-out feature (age out is based on insertion order) and  maintain insertion order for 
the objects in the collection.

Logging DB records
------------------

.. doctest::

    {"args": {"query": "site=T1_CH_CERN"}, 
     "qhash": "7c8ba62a07ff2820c217ae3b51686383", "ip": "127.0.0.1", 
     "hostname": "", "port": 65238, 
     "headers": {"Remote-Addr": "127.0.0.1", "Accept-Encoding": "identity", 
                 "Host": "localhost:8211", "Accept": "application/json", 
                 "User-Agent": "Python-urllib/2.6", "Connection": "close"}, 
     "timestamp": 1263488174.364929, 
     "path": "/status", "_id": "4b4f4caee2194e72ae000003", "method": "GET"}

