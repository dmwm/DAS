Introduction
============

DAS stands for *Data Aggregation System*. It provides a
a common layer above various data services, to allow end users
to query one (or more) of them with a more natural, 
search-engine-like interface. It is developed for the [CMS]_
High Energy Physics experiment on the [LHC]_, at CERN,
Geneva, Switzerland. Although it is currently only used for
the CMS experiment, it was designed to have a general-purpose
architecture which would be applicable to other domains.

It provides several features:

  1. a caching layer for underlying data-services
  2. a common meta-data look up tool for these services
  3. an aggregation layer for that meta-data

The main advantage of DAS is a uniform meta-data representation
and consequent ability to perform look-ups via free text-based queries.
The DAS internal data-format is JSON. All meta-data queried and stored
by DAS are converted into this format, according to a mapping between
the notation used by each data service and a set of common keys
used by DAS.

