#!/bin/sh

# Run this by:
# bash $DAS_ROOT/keywordsearch/bootstrap.sh
# ~/Desktop/DAS/DAS_code/DAS/

QUERIES_FILE="./bootstrap_queries.txt"
QUERIES_FILE="/home/vidma/Desktop/DAS/DAS_code/DAS/src/python/DAS/keywordsearch/bootstrap_queries_1r.txt"

# Clean up:


mongo --port 8230 keylearning --eval "db.db.remove(); db.db.find();"
mongo --port 8230 das --eval "db.cache.remove(); db.merge.remove();"

while read query; do
    #n=`expr $n + 1`
    # run a query

    echo "Running Query: ${query}"
    das_cli --no-output -q "$query"
    # while the results are still in raw cache, update the registry about service result structure
    python -u  $DAS_ROOT/analytics/standalone_task.py -c key_learning

    echo "updating the service registry"
    #$DAS_ROOT/keywordsearch/das_schema_adapter.py
    #
done < $QUERIES_FILE