__author__ = 'vidma'

import sys, time
from DAS.utils.das_config import das_readconfig
from DAS.utils.das_db import db_connection, is_db_alive


def on_success():
    print 'DB is available'
    sys.exit(0)


def db_monitor(uri, func, sleep=5, max_retries=None):
    """
    Check status of MongoDB connection. Invoke provided function upon
    successfull connection.
    """
    conn = db_connection(uri)
    retries = 0
    while True:
        if  not conn or not is_db_alive(uri):
            try:
                conn = db_connection(uri)
                if conn:
                    func()

                if  conn:
                    print "### db_monitor re-established connection %s" % conn
                else:
                    print "### db_monitor, lost connection"

            # sys.exit(0) is not caught by Exception
            #  (but BaseException would catch it)
            except Exception, e:
                print e

        # limit the number of retries if needed
        retries += 1
        if max_retries is not None and retries > max_retries:
            break

        time.sleep(sleep)




def waitfordb(max_time):
    config = das_readconfig()
    dburi = config['mongodb']['dburi']
    sleep_time=5
    db_monitor(dburi, on_success,
               sleep=sleep_time, max_retries=max_time // sleep_time)

    print 'DB is not available and the timeout has passed'
    sys.exit(-1)


if __name__ == '__main__':
    waitfordb(60)