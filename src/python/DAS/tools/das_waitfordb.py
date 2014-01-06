#!/usr/bin/env python
"""
A tiny module allowing to block limited amount of time until DB is ready.
This moght be useful in scripting environment, e.g. to run tasks
just after installation.
"""
import contextlib
import sys
import cStringIO
import time
from DAS.keywordsearch.entity_matchers.kwd_chunks import ir_entity_attributes


@contextlib.contextmanager
def nostdout():
    """ prevent from outputing anything to stdout """
    save_stdout = sys.stdout
    sys.stdout = cStringIO.StringIO()
    yield
    sys.stdout = save_stdout


# silence the DAS messages
with nostdout():
    from DAS.utils.das_config import das_readconfig
    from DAS.utils.das_db import db_connection, is_db_alive
    from DAS.core.das_core import DASCore
    from DAS.core.das_mapping_db import DASMapping


def check_mappings_readiness():
    """
    return whether DASMaps are initialized
    """
    print('db alive. checking it\'s state...')
    try:
        dasmapping = DASMapping(das_readconfig())
        if dasmapping.check_maps():
            DASCore(multitask=False)
            return True
    except Exception as exc:
        print exc
    print 'no DAS mappings present...'
    return False


def on_db_available():
    """
    this callback waits for correct DASMaps
    """
    print 'DB is available'
    if check_mappings_readiness():
        print('Mappings seem to be fine...')
        #time.sleep(10)
        sys.exit(0)


def db_monitor(uri, func, sleep=5, max_retries=None):
    """
    Check status of MongoDB connection. Invoke provided function upon
    successfull connection.
    """
    conn = None
    retries = 0
    while True:
        # db is dead
        if not (conn and is_db_alive(uri)):
            try:
                conn = db_connection(uri)
            except Exception as exc:
                print exc

        if conn and is_db_alive(uri):
            print "### established connection %s" % conn
            func()

        # limit the number of retries if needed
        retries += 1
        if max_retries is not None and retries > max_retries:
            break

        time.sleep(sleep)


def waitfordb(max_time, callback=on_db_available):
    """
    waits until DB is ready as well as until DAS mappings are created/updated.
    """
    config = das_readconfig()
    dburi = config['mongodb']['dburi']
    sleep_time = 5
    db_monitor(dburi, callback,
               sleep=sleep_time, max_retries=max_time // sleep_time)

    print 'DB is not available and the timeout has passed'
    sys.exit(-1)


def is_bootstrap_needed():
    """
    whether bootstrap is mandatory:
    - not initialized output fields (keylearning db)
    - no values known
    """
    with nostdout():

        from DAS.keywordsearch.metadata.input_values_tracker \
            import need_value_bootstrap
        from DAS.keywordsearch.metadata import schema_adapter_factory

        def need_res_fields_bootsrap():
            """ return whether the list of of entity attributes is available
            if not these are needed to be bootstrapped
            """
            dascore = DASCore(multitask=False)
            schema_adapter = schema_adapter_factory.get_schema(dascore)
            try:
                field_list = schema_adapter.list_result_fields()
                if not field_list:
                    return True
                ir_entity_attributes.SimpleIREntityAttributeMatcher(field_list)
            except Exception as exc:
                print exc
                return True
            return False

        needed = (need_value_bootstrap() or need_res_fields_bootsrap())
        return 1 if needed else 0


if __name__ == '__main__':
    is_bootstrap_needed()
    waitfordb(120)
