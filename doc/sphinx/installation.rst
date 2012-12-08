Installation
============

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


DAS installation and configuration
----------------------------------

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
