Installation
============

Quick installation with pip and virtualenv
-----------------------------------------
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


Source code
-----------

DAS source code is freely available from [DAS]_ github repository. To install
it on your system you need to use `git` which can be found from [GIT]_ web
site.

Prerequisites
-------------
DAS depends on the following software:

- MongoDB and pymongo module
- libcurl library
- YUI library (Yahoo UI)
- python modules:

  - yajl (Yet Another JSON library) or cjson (C-JSON module)
  - CherryPy
  - Cheetah
  - PLY
  - PyYAML
  - pycurl

To install MongoDB visit their web site [Mongodb]_, download latest binary tar ball,
unpack it and make its bin directory available in your path.

To install libcurl library visit its web site [CURL]_ and install it on your
system.

To install YUI library, visit Yahoo developer web site [YUI]_ and install
version 2 of their yui library.

To install python dependencies it is easier to use standard python installer
*pip*. In on your to get it download virtual environment [VENV]_
and run it as following:

.. doctest::

    curl -O https://raw.github.com/pypa/virtualenv/master/virtualenv.py
    python virtualenv.py <install_dir>

Once it is installed you need to setup your path to point to *install_dir/bin*
and then invoke pip:

.. doctest::

    export PATH=<install_dir>/bin:$PATH
    pip install python-cjson
    pip install czjson
    pip install CherryPy
    pip install Cheetah
    pip install PyYAML
    pip install yajl
    pip install pymongo
    pip install ply
    pip install pycurl


DAS installation and configuration (old)
----------------------------------------

To get DAS release just clone it from GIT repository:

.. doctest::

    git clone git://github.com/dmwm/DAS.git
    export PYTHONPATH=<install_dir>/DAS/src/python

DAS configuration is located in DAS/etc/das.cfg file and DAS executables can be
found in DAS/bin area. To run DAS server you need to setup five configuration
parameters, DAS_CONFIG, DAS_CSSPATH, DAS_TMPLPATH, DAS_IMAGEPATH and
DAS_JSPATH. The former is location of your das.cfg, while later should point to
DAS/src/{css,templates,images,js} directories.

For your convenience you may create setup.sh script which will setup your
environment, its context should be something like:

.. doctest::

    #!/bin/bash
    dir=<install_dir> # put your install dir here
    export PATH=$dir/install/bin:$dir/mongodb-linux-x86_64-2.2.2/bin:$PATH
    export PATH=$dir/soft/DAS/bin:$PATH
    export DAS_CONFIG=$dir/soft/DAS/etc/das.cfg 
    export DAS_CSSPATH=$dir/soft/DAS/src/css
    export DAS_TMPLPATH=$dir/soft/DAS/src/templates
    export DAS_IMAGESPATH=$dir/soft/DAS/src/images
    export DAS_JSPATH=$dir/soft/DAS/src/js
    export YUI_ROOT=$dir/soft/yui
    export PYTHONPATH=$dir/soft/DAS/src/python
    export PYTHONPATH=$dir/install/lib/python2.6/site-packages:$PYTHONPATH
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$dir/install/lib

where I installed all packages under local $dir/install area and keep DAS under
$dir/soft/DAS.
