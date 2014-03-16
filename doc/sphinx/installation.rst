Installation
============

Quick installation with pip and virtualenv
------------------------------------------
Basically on a Debian-like system you may just copy&paste all
the commands below into a bash.

.. code-block:: bash

    # get or install virtualenv, see http://www.virtualenv.org/en/latest/virtualenv.html#installation
    # easiest is through apt-get or "pip install virtualenv" if available
    sudo apt-get install python-virtualenv

    # create virtual env
    virtualenv dasenv # will use dir "dasenv"

    # activate the virtualenv -- has to be run in every new shell
    source dasenv/bin/activate

    # get DAS source code
    git clone git://github.com/dmwm/DAS.git
    cd DAS

    # install dependencies
    export PYCURL_SSL_LIBRARY=gnutls  # depending on your distro try: openssl or gnutls
    pip install -r requirements_freezed.txt
    python -m nltk.downloader -e words stopwords wordnet

    # you also need to connect to or install a MongoDB server, e.g.
    sudo apt-get install mongodb-server
    # set MongoDB port to 8230 in /etc/mongodb.conf or change dasconfig
    # sed -i -e 's/8230/27017/g'  etc/das.cfg
    # sed -i -e 's/8230/27017/g'  bin/das_db_import


    # install DAS
    python setup.py install
    source ./init_env.sh

    # initialize database with (default) service mappings
    das_create_json_maps src/python/DAS/services/maps
    das_update_database src/python/DAS/services/maps/das_maps_dbs_prod.js

    # download YUI (the location is set in init_env.sh)
    (curl -o yui.zip -L http://yuilibrary.com/downloads/yui2/yui_2.9.0.zip && \
     unzip yui.zip && mkdir -p  $YUI_ROOT && cp -R yui/* $YUI_ROOT/ )

    # run the tests
    source ./init_env.sh
    touch /tmp/x509up_u$UID  # or use grid-proxy-init if you require certs...
    python setup.py test

    # finally start das server
    # P.S. don't forget "source dasenv/bin/activate" when restarting in a new shell
    source ./init_env.sh
    bash das_server start
    # Finally you can access it at: http://localhost:8212/das/


If, while running tests or starting DAS, you experience problems like
"pycurl: libcurl link-time ssl backend (gnutls) is different from
compile-time ssl backend (openssl)", try reinstalling (pycurl) using
a different PYCURL_SSL_LIBRARY, e.g.:

.. code-block:: bash

    PYCURL_SSL_LIBRARY=openssl
    pip uninstall pycurl && pip install -r requirements_freezed.txt


For more details continue reading below.


.. rubric:: Source code

DAS source code is freely available from [DAS]_ github repository. To install
it on your system you need to use `git` which can be found from [GIT]_ web
site.

