DAS Command Line Interface (CLI) tool
=====================================

The DAS Command Line Interface (CLI) tool can be downloaded directly from
DAS server. It is python-based tool and does not require any additional
dependencies, althought a python version of 2.6 and above is required.
Its usage is very simple

.. doctest::

    Usage: das_client.py [options]
    For more help please visit https://cmsweb.cern.ch/das/faq

    Options:
      -h, --help            show this help message and exit
      -v VERBOSE, --verbose=VERBOSE
                            verbose output
      --query=QUERY         specify query for your request
      --host=HOST           host name of DAS cache server, default is
                            https://cmsweb.cern.ch
      --idx=IDX             start index for returned result set, aka pagination,
                            use w/ limit (default is 0)
      --limit=LIMIT         number of returned results (default is 10), use
                            --limit=0 to show all results
      --format=FORMAT       specify return data format (json or plain), default
                            plain.
      --threshold=THRESHOLD
                            query waiting threshold in sec, default is 5 minutes
      --key=CKEY            specify private key file name
      --cert=CERT           specify private certificate file name
      --retry=RETRY         specify number of retries upon busy DAS server message
      --das-headers         show DAS headers in JSON format
      --base=BASE           specify power base for size_format, default is 10 (can
                            be 2)

The query parameter specifies an input `DAS query <das_queries>`, while the format parameter
can be used to get results either in JSON or plain (suitable for cut and paste)
data format. Here is an example of using das_client tool to retrieve information about
dataset pattern

.. doctest::

    python das_client.py --query="dataset=/ZMM*/*/*"

    Showing 1-10 out of 2 results, for more results use --idx/--limit options

    /ZMM_14TeV/Summer12-DESIGN42_V17_SLHCTk-v1/GEN-SIM
    /ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM

And here is the same output using JSON data format, the auxilary DAS headers are
also requested:

.. doctest::

    python das_client.py --query="dataset=/ZMM*/*/*" --format=JSON --das-headers


    {'apilist': ['das_core', 'fakeDatasetPattern'],
     'ctime': 0.0015709400177,
     'data': [{'_id': '523dcd7f0ec3dc12198a44c5',
               'cache_id': ['523dcd7f0ec3dc12198a44c3'],
               'das': {'api': ['fakeDatasetPattern'],
                       'condition_keys': ['dataset.name'],
                       'expire': 1379782315.848377,
                       'instance': 'cms_dbs_prod_global',
                       'primary_key': 'dataset.name',
                       'record': 1,
                       'services': [{'dbs': ['dbs']}],
                       'system': ['dbs'],
                       'ts': 1379782015.863179},
               'das_id': ['523dcd7d0ec3dc12198a4498'],
               'dataset': [{'created_by': '/DC=ch/DC=cern/OU=computers/CN=wmagent/vocms216.cern.ch',
                            'creation_time': '2012-02-24 01:40:40',
                            'datatype': 'mc',
                            'modification_time': '2012-02-29 21:25:52',
                            'modified_by': '/DC=org/DC=doegrids/OU=People/CN=Alan Malta Rodrigues 4861',
                            'name': '/ZMM_14TeV/Summer12-DESIGN42_V17_SLHCTk-v1/GEN-SIM',
                            'status': 'VALID',
                            'tag': 'DESIGN42_V17::All'}],
               'qhash': 'e5ced95dd57a5cfe1a3126a22a85a301'},
              {'_id': '523dcd7f0ec3dc12198a44c6',
               'cache_id': ['523dcd7f0ec3dc12198a44c4'],
               'das': {'api': ['fakeDatasetPattern'],
                       'condition_keys': ['dataset.name'],
                       'expire': 1379782315.848377,
                       'instance': 'cms_dbs_prod_global',
                       'primary_key': 'dataset.name',
                       'record': 1,
                       'services': [{'dbs': ['dbs']}],
                       'system': ['dbs'],
                       'ts': 1379782015.863179},
               'das_id': ['523dcd7d0ec3dc12198a4498'],
               'dataset': [{'created_by': 'cmsprod@cmsprod01.hep.wisc.edu',
                            'creation_time': '2011-12-29 17:47:25',
                            'datatype': 'mc',
                            'modification_time': '2012-01-05 17:40:17',
                            'modified_by': '/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118',
                            'name': '/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM',
                            'status': 'VALID',
                            'tag': 'DESIGN42_V11::All'}],
               'qhash': 'e5ced95dd57a5cfe1a3126a22a85a301'}],
     'incache': True,
     'mongo_query': {'fields': ['dataset'],
                     'instance': 'cms_dbs_prod_global',
                     'spec': {'dataset.name': '/ZMM*/*/*'}},
     'nresults': 2,
     'status': 'ok',
     'timestamp': 1379782017.68}

Using DAS CLI tool from other applications
++++++++++++++++++++++++++++++++++++++++++

It is possible to plug DAS CLI tool into other python applications. This can be
done as following

.. doctest::

   from das_client import get_data

   # invoke DAS CLI call for given host/query
   # host: hostname of DAS server, e.g. https://cmsweb.cern.ch
   # query: DAS query, e.g. dataset=/ZMM*/*/*
   # idx: start index for pagination, e.g. 0
   # limit: end index for pagination, e.g. 10, put 0 to get all results
   # debug: True/False flag to get more debugging information
   # threshold: 300 sec, is a default threshold to wait for DAS response
   # ckey=None, cert=None are parameters which you can used to pass around
   # your GRID credentials
   # das_headers: True/False flag to get DAS headers, default is True

   # please note that prior 1.9.X release the return type is str
   # while from 1.9.X and on the return type is JSON

   data = get_data(host, query, idx, limit, debug, threshold=300, ckey=None,
   cert=None, das_headers=True)

Please note, that aforementioned code snippet requires to load `das_client.py`
which is distributed within CMSSW. Due to CMSSW install policies the version of
`das_client.py` may be quite old. If you need up-to-date `das_client.py`
functionality you can follow this recipe. The code below download
`das_client.py` directly from cmsweb site, compile it and use it in your
application:

.. doctest::

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
            if  isinstance(data, basestring):
                return json.loads(data)
            return data

    if __name__ == '__main__':
        das      = DASClient()
        query    = "/ZMM*/*/*"
        result   = das.call(query)
        if  result['status'] == 'ok':
            nres = result['nresults']
            data = result['data']
            print "Query=%s, #results=%s" % (query, nres)
            print data

Here we provide a simple example of how to use das_client to find dataset
summary information.

.. doctest::

    # PLEASE NOTE: to use this example download das_client.py from
    # cmsweb.cern.ch/das/cli

    # system modules
    import os
    import sys
    import json

    from das_client import get_data

    def drop_das_fields(row):
        "Drop DAS specific headers in given row"
        for key in ['das', 'das_id', 'cache_id', 'qhash']:
            if  row.has_key(key):
                del row[key]

    def get_info(query):
        "Helper function to get information for given query"
        host    = 'https://cmsweb.cern.ch'
        idx     = 0
        limit   = 0
        debug   = False
        data    = get_data(host, query, idx, limit, debug)
        if  isinstance(data, basestring):
            dasjson = json.loads(data)
        else:
            dasjson = data
        status  = dasjson.get('status')
        if  status == 'ok':
            data = dasjson.get('data')
            return data

    def get_datasets(query):
        "Helper function to get list of datasets for given query pattern"
        for row in get_info(query):
            for dataset in row['dataset']:
                yield dataset['name']

    def get_summary(query):
        """
        Helper function to get dataset summary information either for a single
        dataset or dataset pattern
        """
        if  query.find('*') == -1:
            print "\n### query", query
            data = get_info(query)
            for row in data:
                drop_das_fields(row)
                print row
        else:
            for dataset in get_datasets(query):
                query = "dataset=%s" % dataset
                data = get_info(query)
                print "\n### dataset", dataset
                for row in data:
                    drop_das_fields(row)
                    print row

    if __name__ == '__main__':
        # query dataset pattern
        query = "dataset=/ZMM*/*/*"
        # query specific dataset in certain DBS instance
        query = "dataset=/8TeV_T2tt_2j_semilepts_200_75_FSim526_Summer12_minus_v2/alkaloge-MG154_START52_V9_v2/USER instance=cms_dbs_ph_analysis_02"
        get_summary(query)
