DAS release notes
=================

Release V04 series
------------------
The most significant part of this release is new plug-and-play mechanism
to add new data-services. This is done via data-service map creation. Each
map is represented data-service URI (URL, input parameters, API, etc.).

- V04_00_01
  - minor tweaks to make CMS rpms
  - modifications for init scripts to be able to run in
    stand-alone mode

- V04_00_00
  - incorporate all necessary changes for plug-and-play
  - modifications for stand-alone mode

Release V03 series
------------------

Major change in this release was a separation of DAS cache into 
independent cache and merge DB collection. The das.cache collection
stores *raw* API results, while das.merge keeps only merged records.

- V03_00_04

  - minor changes to documentation structure

- V03_00_03

  - added DAS doc server
  - added sphinx support as primary DAS documentation system

- V03_00_02

  - work on DAS cli tools

- V03_00_01

  - bug fixes

- V03_00_00

  - separate DAS cache into das.cache and das.merge collections


Release V02 series
------------------

This release series is based on MongoDB. After a long evaluation of
different technologies, we made a choice in favor of MongoDB.

- added support for map/reduce
- switch to pipes syntax in QL for aggregation function support
- switch DAS QL to free keyword based syntax

Release V01 series
------------------

Evalutaion series. During this release cycle we played with
the following technologies:

- Memcached
- CouchDB
- custom file-based cache

At that time DAS QL was based on DBS-QL syntax.
During this release series we added DAS cache/web servers;
made CLI interface.

