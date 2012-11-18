.. _das_mapping:

DAS Mapping DB
==============
DAS uses Query Language (QL) to look-up data from data-providers as well as its
own cache. The data provided by various data services can come in variety of
form and data formats, while DAS cache stores data records in
`JSON <http://en.wikipedia.org/wiki/Json>`_ data format.
Therefore we need to define certain mappings between DAS QL and
data-provider API calls as well as DAS QL and data records in DAS cache.
To serve this goal DAS relies on its Mapping DB which holds information
about all the data-service APIs which are used by DAS, and the necessary
mappings between DAS and API records::

.. figure:: _images/das_mappings.png

Each mapping holds the following entries:

- The daskeys mapping is used to transform keys entered by a user into 
  the keys that DAS uses internally to identify data in the cache.
- The das2api mapping converts between internal DAS keys and the names
  required by the data service query.
- The notation map transforms the keys in the data service response
  into the appropriate DAS equivalents.

DAS also uses presentation map to translate DAS records into human readable
form, e.g. to translate *file.nevents* into *Number of events*

The DAS maps use `YAML <http://en.wikipedia.org/wiki/Yaml>`_
data-format. Each file may contain several data-service API mappings, as well
as auxilary information about data-provider, e.g. data format, expiration
timestamp, etc. For example here is a simple mapping file for google map APIs

.. _api_map:
.. _notation:
.. doctest::

    system : google_maps
    format : JSON
    ---
    urn : google_geo_maps
    url : "http://maps.google.com/maps/geo"
    expire : 30
    params : { "q" : "required", "output": "json" }
    daskeys : [
        {"key":"city","map":"city.name","pattern":""},
    ]
    das2api : [
        {"das_key":"city.name","api_param":"q","pattern":""},
    ]
    ---
    urn : google_geo_maps_zip
    url : "http://maps.google.com/maps/geo"
    expire : 30
    params : { "q" : "required", "output": "json" }
    daskeys : [
        {"key":"zip","map":"zip.code","pattern":""},
    ]
    das2api : [
        {"das_key":"zip.code","api_param":"q","pattern":""},
    ]
    ---
    notations : [
        {"notation":"zip.name", "map":"zip.code", "api":""},
        {"notation":"name", "map":"code", "api":"google_geo_maps_zip"},
    ]

As you can see it defines the data-provider name, ``google_maps`` (DAS call it
system), the data format ``JSON`` used by this data-provider as well as three
maps, separated by tripple dashes.  The first one defines mapping for API which
provides information about geo location for a given city key, the second
defines geo location for a given zip key and mapping for notations used by DAS
workflow. In particulat, DAS will map ``zip.name`` into ``zip.code`` for any
api, and ``name`` into ``code`` for ``google_geo_maps_zip`` api (the meaning of
these translation will become clear when we will discuss concrete example
below).

As you may noticed, every mapping (the code between tripple dashes) has
repeated strucute. It defines *urn, url, expire, params, daskeys, das2api*
values. The *urn* stands for uniform resource name, this alias is used by DAS
to distinguish APIs and their usage pattern, the *url* is canonical URL for API
in question, the *params* defines a dictionary of input parameters accepted by
API, the *daskeys* is mapping from DAS keys into DAS data records, and finally,
*das2api* is mapping between DAS data and API records.

To accommodate different use cases of API usage the ``params`` structure may
contain three types of parameter values: the **default**, **required** and
**optional** values. The default value will be passed to API *as is*, the
required value must be substituted by DAS workflow (it will be taken from the
query provided by DAS user, if it will not be provide the API call will be
discarded from DAS workflow) and the optional value which can be skipped by API
call.

Example
-------
In this section we show a concrete example of mappings used by DAS workflow for
one of the data-services. Let's take the following DAS queries:

.. doctest::

   file file=X
   file file=X status=VALID

These queries will correspond to the following DAS record structure::

   {"file" : {"name": "X", "size":1, "nevents": 10, ...}}

The dots just indicate that structure can be more comlpex.

The ``file`` DAS key is mapped into ``file.name`` key.attribute value within
DAS record, here the period divides key from attribute in aforementioned
dictionary. Therefore ``file.name`` value is ``X``, ``file.size`` value is 1,
etc.

Here is an example of one of the DAS mapping records which can serve discussed
DAS queries (please note that it may be several data-services which may
provide the data for given DAS query).

.. doctest::

    urn: files
    url : "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/files/"
    expire : 900
    params : {
            "logical_file_name":"required",
            "detail":"True",
            "status": "optional",
    }
    daskeys : [
            {"key": "file", "map":"file.name", "pattern":""},
            {"key": "status", "map":"status.name", "pattern":""},
    ]
    das2api : [
            {"das_key":"file.name", "api_param":"logical_file_name", "pattern":""},
            {"das_key": "status.name", "api_param":"status", "pattern":""},
    ]

This record defines ``files`` API with given URL and expire timestamp. It
specifies the input parameters (``params``), in particular,
``logical_file_name`` is required by this API, the ``detail`` has default value
True and ``status`` is an optional input parameter. The daskeys mapping defines
mapping between DAS keys used by end-user and DAS record keys. For example

.. doctest::

   file file=X

will be mapped into the following API call::

   https://cmsweb.cern.ch/dbs/prod/global/DBSReader/files?logical_file_name=X&detail=True

while::

   file file=X status=VALID

will be mapped into::

   https://cmsweb.cern.ch/dbs/prod/global/DBSReader/files?logical_file_name=X&detail=True&status=VALID

In both case, the data-provider will return back the following data-record, e.g.::

   {"logical_file_name: "X", "size": 1, ...}

therefore we need another mapping from API data record into expected DAS record
structure (as we discussed above)::

   {"file": {"name": "X", "size": 1, ...}}

To perform such translation DAS workflow consults ``das2api`` maps which defines
them, e.g. ``logical_file_name`` maps into ``file.name``, etc.

Sometimes, different data-services provides data records who have different
notations, e.g. ``fileName``, ``file_name``, etc. To accommodate this differences
DAS consults notation map to perform transation from one into another
notation.

Finally, to translate DAS records into human readable form we need another
mapping, the presentation one. It defines what should be presented at DAS
UI level for a given DAS record. For example, we may want to display "File name"
at DAS UI, instead of showing ``file.name``. To perform this translation DAS uses
presentation map.


