.. _data_objects:

DAS data objects
================

DAS needs to deal with a variety of different data object representations.
Data from providers may have both different formats (eg XML, JSON), and
different ways of storing hierarchical information. The structure of response
data is not known to DAS a-priori. Therefore it needs to treat them as data objects. Here
we define what it means for DAS and provide examples of DAS data objects
or DAS records.

There are basically two types of data objects: flat and hierarchical ones.

- Flat

.. doctest::

   {"dataset":"abc", size:1, "location":"CERN"}

- Hierarchical

.. doctest::

   {"dataset": {"name":"abc", size:1, "location":"CERN"}}

The first has the disadvantage of not being able to easily tell what this object
represents, whereas this is not the case for the latter. It is clear in that case
that the object principally represents a *dataset*. However, all good things come
with a cost; the hierarchical structures are much more expensive to parse,
both in python and MongoDB. In DAS we store objects in hierarchical structures, but
try to minimize the nesting depth. This allows us to talk about the key/attributes
of the object in a more natural way, and simplifies their aggregation. For instance,
if two different data-providers serve information about files and file objects containing
the *name* attribute, it will be trivial to merge the objects based on the *name* value.
