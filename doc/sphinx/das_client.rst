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

Please note, that aforementioned code snippet requires to load `das_client.py`
which is distributed within CMSSW. Due to CMSSW install policies the version of
`das_client.py` may be quite old. If you need up-to-date `das_client.py`
functionality you can follow this recipe. The code below download
`das_client.py` directly from cmsweb site, compile it and use it in your
application:

.. code::

    import os
    import json
    import urllib2
    import httplib
    import tempfile

    class HTTPSClientHdlr(urllib2.HTTPSHandler):
        """
        Simple HTTPS client authentication class based on provided
        key/ca information
        """
        def __init__(self, key=None, cert=None, level=0):
            if  level:
                urllib2.HTTPSHandler.__init__(self, debuglevel=1)
            else:
                urllib2.HTTPSHandler.__init__(self)
            self.key = key
            self.cert = cert

        def https_open(self, req):
            """Open request method"""
            #Rather than pass in a reference to a connection class, we pass in
            # a reference to a function which, for all intents and purposes,
            # will behave as a constructor
            return self.do_open(self.get_connection, req)

        def get_connection(self, host, timeout=300):
            """Connection method"""
            if  self.key:
                return httplib.HTTPSConnection(host, key_file=self.key,
                                                    cert_file=self.cert)
            return httplib.HTTPSConnection(host)

    class DASClient(object):
        """DASClient object"""
        def __init__(self, debug=0):
            super(DASClient, self).__init__()
            self.debug = debug
            self.get_data = self.load_das_client()

        def get_das_client(self, debug=0):
            "Download das_client code from cmsweb"
            url  = 'https://cmsweb.cern.ch/das/cli'
            ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
            cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
            req  = urllib2.Request(url=url, headers={})
            if  ckey and cert:
                hdlr = HTTPSClientHdlr(ckey, cert, debug)
            else:
                hdlr = urllib2.HTTPHandler(debuglevel=debug)
            opener = urllib2.build_opener(hdlr)
            fdesc = opener.open(req)
            cli = fdesc.read()
            fdesc.close()
            return cli

        def load_das_client(self):
            "Load DAS client module"
            cli = self.get_das_client()
            # compile python code as exec statement
            obj   = compile(cli, '<string>', 'exec')
            # define execution namespace
            namespace = {}
            # execute compiled python code in given namespace
            exec obj in namespace
            # return get_data object from namespace
            return namespace['get_data']

        def call(self, query, idx=0, limit=0, debug=0):
            "Query DAS data-service"
            host = 'https://cmsweb.cern.ch'
            data = self.get_data(host, query, idx, limit, debug)
            return json.loads(data)

    if __name__ == '__main__':
        das      = DASClient()
        query    = "/ZMM*/*/*"
        result   = das.call(query)
        if  result['status'] == 'ok':
            nres = result['nresults']
            data = result['data']
            print "Query=%s, #results=%s" % (query, nres)
            print data

