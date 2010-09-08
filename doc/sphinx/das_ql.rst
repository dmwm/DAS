DAS Query Language
==================

DAS Query Language (DAS-QL) provides intuitive, easy to use text based queries
to DAS services. Its syntax is represneted in the following diagram

.. figure::  _images/das_ql.png

DAS query starts with list of known keys (look-up part of the query) 
defined by DAS data-services and
followed by optional filter(s) and aggregator function(s). A wild-card
patterns are supported via asterisk character. For example

.. doctest::

    file dataset=/a/b/c | grep file.size, dataset.name
    file dataset=/a/b/c | sum(file.size)
    file dataset=/a/b*

The grep filter accepts both DAS key and key.attribute fields. The key 
attributes are deduced from the DAS records. For instance, if file record
contain *size, id, md5 fields*, all of them can be used as attributed of
the *file* key, e.g. *file.md5*. But the attributes cannot appear in look-up
part of the query.
