.. _data_objects:

DAS data objects
================

DAS needs to deal with variety of different data object representations.
For instance, data-provides can send back to DAS a meta-data in different
data formats, such as XML, JSON, etc. The document structure is not known
to DAS a-priory. Therefore it needs to treat them as data objects. Here
we define what it means for DAS and provide examples of DAS data objects
or DAS records.

There are basically two types of data objects: flat and hierarchical ones.

- Flat objects represents meta-data in a flat structure, for instance

.. doctest::

   {"dataset":"abc", size:1, "location":"CERN"}

- Hierarchical objects represents meta-data in hierarchical structure, for
  instance

.. doctest::

   {"dataset": {"name":"abc", size:1, "location":"CERN"}}

The former one has disadvantage of not being able to tell what this object
represents. This is not the case of last one. It is clear in that case
that data object represents *dataset* object. While everything comes with
cost. The hierarchical data objects can be very nested which introduce
load on parsing them. In DAS we store objects in hierarchical structure, but
try to minimize its nest-ness. Therefore it allows to talk about key/attributes
of the object in a natural way. Also this simplify their aggregation. For instance,
if two data-providers ship information about files and file objects contain
*name* attribute, it will be trivial to merge objects based on *name* value.
