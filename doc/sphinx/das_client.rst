DAS CLI tool
============
The DAS Command Line Interface (CLI) tool can be downloaded directly from
DAS server. It is python-based tool and does not require any additional
dependencies, althought a python version of 2.6 and above is required.
Its usage is very simple

.. doctest::

    Usage: das_client.py [options]

    Options:
      -h, --help            show this help message and exit
      -v VERBOSE, --verbose=VERBOSE
                            verbose output
      --query=QUERY         specify query for your request
      --host=HOST           specify host name of DAS cache server, default
                            https://cmsweb.cern.ch
      --idx=IDX             start index for returned result set, aka pagination,
                            use w/ limit
      --limit=LIMIT         number of returned results (results per page)
      --format=format       specify return data format (json or plain), default
                            json

The query parameter specifies an input `DAS query <das_queries>`, while the format parameter
can be used to get results either in JSON or plain (suitable for cut and paste)
data format. Here is an example of using das_client tool to retrieve details of
specific dataset

.. doctest::

    python das_client.py --query="ip=137.138.141.145 | grep ip.City, ip.CountryCode" --format=plain
    Geneva CH

And here is a sample of data output in json data format

.. doctest::

    {'args': {'idx': '0',
              'input': 'ip=137.138.141.145',
              'limit': '10',
              'pid': '53071'},
     'data': [{'_id': '4d6a6e98f823c6cf49000005',
               'cache_id': ['4d6a6e98f823c6cf49000004'],
               'das': {'empty_record': 0,
                       'expire': 1298820790.1766081,
                       'primary_key': 'ip.address',
                       'system': ['ip_service']},
               'das_id': ['4d6a6e97f823c6cf49000001'],
               'ip': {'City': 'Geneva',
                      'CountryCode': 'CH',
                      'CountryName': 'Switzerland',
                      'Dstoffset': '0',
                      'Gmtoffset': '3600',
                      'Isdst': '0',
                      'Latitude': '46.2',
                      'Longitude': '6.1667',
                      'RegionCode': '07',
                      'RegionName': 'Geneve',
                      'Status': 'OK',
                      'TimezoneName': 'Europe/Zurich',
                      'ZipPostalCode': '',
                      'address': '137.138.141.145'}}],
     'headers': {'Accept': 'application/json',
                 'Accept-Encoding': 'identity',
                 'Connection': 'close',
                 'Host': 'localhost:8212',
                 'Remote-Addr': '127.0.0.1',
                 'User-Agent': 'Python-urllib/2.6'},
     'hostname': '',
     'ip': '127.0.0.1',
     'method': 'GET',
     'mongo_query': {'fields': ['ip'], 'spec': {'ip.address': '137.138.141.145'}},
     'nresults': 1,
     'path': '/cache',
     'port': 50475,
     'status': 'ok',
     'timestamp': 1298820779.4300001}

Using DAS CLI tool in other applications
++++++++++++++++++++++++++++++++++++++++

It is possible to plug DAS CLI tool into other python applications. This can be
done as following

.. doctest::

   from das_client import get_data

   # invoke DAS CLI call for given host/query
   data = get_data(host, query, idx, limit, debug)

