DAS records
***********

.. toctree::
   :maxdepth: 2

   data_objects.rst

The DAS records represents meta-data in [JSON]_ data format. It is very lightweight
to represent almost any data structure via values, dictionaries, lists. By itself,
the JSON is native JavaScript data format and it is a dictionary data type in python.
The DAS records is just collection of meta-data supplied by data-providers which
participate in DAS. DAS wraps each meta-data with auxilary information such as
internal id, reference das id to other DAS records, and DAS header. The DAS header
contains information about underlying API calls made to data-provider. For example:

.. doctest::

    {"query": "{\"fields\": null, \"spec\": {\"block.name\": \"/abc\"}}", 
     "_id": "4b6c8919e2194e1669000002", 
     "das": {"status": "ok", "qhash": "aa8bcf183d916ea3befbdfbcbf40940a", 
             "ctime": [0.55365610122680664, 0.54806804656982422], 
             "url": ["http://a.v.com/api", "http://c.d.com/api"], 
             "timestamp": 1265404185.2611251, 
             "lookup_keys": ["block.name", "file.name", "block.name"], 
             "system": ["phedex3", "dbs"], 
             "api": ["blockReplicas", "listBlocks"], 
             "expire": 1265407785.2611251}
    }

DAS handles different type of :ref:`data-objects <data_objects>`
