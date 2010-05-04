DAS benchmark
=============

We profile using vocms67 machine who has the following specs:

- 8 core, Intel Xeon CPU @ 2.33GHz, cache size 6144 KB
- 16 GB of RAM
- 2.6.18-164.11.1.el5 #1 SMP Wed Jan 20 12:36:24 CET 2010 x86_64 x86_64 x86_64 GNU/Linux

The DAS benchmarking is done using the following query

.. doctest::

   das_cli --query="block" --no-output

Latest results are shown below:

.. doctest::

    ...
    INFO   0x9e91ea8> DASMongocache::update_cache, ['dbs'] yield 387137 rows
    ...
    INFO   0x9e91ea8> DASMongocache::update_cache, ['phedex'] yield 189901 rows
    ...
    INFO   0x9e91ea8> DASMongocache::merge_records, merging 577038 records

    DAS execution time (phedex) 106.446726799 sec
    DAS execution time (dbs) 72.2084658146 sec
    DAS execution time (merge) 62.8879590034 sec
    DAS execution time 241.767010927 sec, Wed, 20 Jan 2010 15:54:33 GMT

Here is top contributors into elapsed time

.. doctest::

    das_cli --query="block" --verbose=1 --profile --no-output

    Mon Jan 18 22:43:27 2010    profile.dat

         54420138 function calls (54256630 primitive calls) in 247.423 CPU seconds

   Ordered by: internal time

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
   560649   78.018    0.000   78.018    0.000 {method 'recv' of '_socket.socket' objects}
      992   23.301    0.023   23.301    0.023 {pymongo._cbson._insert_message}
  1627587   19.484    0.000   19.484    0.000 {DAS.extensions.das_speed_utils._dict_handler}
      467   17.295    0.037   21.042    0.045 {pymongo._cbson._to_dicts}
    23773   16.945    0.001   16.945    0.001 {built-in method feed}
   576626   12.101    0.000   77.974    0.000 /data/projects/das/COMP/DAS/src/python/DAS/utils/utils.py:709(xml_parser)
  1627587    9.383    0.000   28.867    0.000 /data/projects/das/COMP/DAS/src/python/DAS/utils/utils.py:694(dict_helper)
   969267    7.716    0.000   10.656    0.000 /data/projects/das/slc5_amd64_gcc434/external/py2-pymongo/1.3/lib/python2.6/site-packages/pymongo/objectid.py:77(__generate)
   392636    4.499    0.000   47.851    0.000 /data/projects/das/COMP/DAS/src/python/DAS/utils/utils.py:798(aggregator)
        1    3.877    3.877   72.942   72.942 /data/projects/das/COMP/DAS/src/python/DAS/core/das_mongocache.py:522(merge_records)
  1153242    3.644    0.000    5.443    0.000 /data/projects/das/COMP/DAS/src/python/DAS/utils/utils.py:52(dict_value)
   576626    3.042    0.000   89.345    0.000 /data/projects/das/COMP/DAS/src/python/DAS/core/das_mongocache.py:586(update_records)
   576715    3.002    0.000    8.917    0.000 /data/projects/das/slc5_amd64_gcc434/external/py2-pymongo/1.3/lib/python2.6/site-packages/pymongo/database.py:183(_fix_outgoing)
   969267    2.798    0.000   17.809    0.000 /data/projects/das/slc5_amd64_gcc434/external/py2-pymongo/1.3/lib/python2.6/site-packages/pymongo/database.py:170(_fix_incoming)
   ......
