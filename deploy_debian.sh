#Installation with setup.py on Ubuntu
# prerequisities: 
# TODO: cjson, libyajl1  is not working
# get easy_install and mongodb
sudo apt-get install mongodb python-setuptools python-yaml

# yaml with easy_install didn't work 
sudo easy_install cherrypy>=3.1.2  pymongo ply Cheetah yajl-py 

#configuration:
# TODO: default mongodb port is: 27017


# get das installation
export DAS_ROOT=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`

# download and extract YUI
wget http://yuilibrary.com/downloads/yui2/yui_2.9.0.zip -O /tmp/yui.zip

sudo mkdir $DAS_ROOT/third-party/
sudo unzip /tmp/yui.zip -d $DAS_ROOT/third-party/
export DAS_CONFIG=$DAS_ROOT/etc/das.cfg
export YUI_ROOT=$DAS_ROOT/third-party/yui
# gedit $DAS_ROOT/etc/das.cfg 
# or start mongod on 8230 instead of default 27017
sudo mkdir -p /data/db/
sudo mongod --port 8230
# install DAS service mappings / initialize the database 
das_map $DAS_ROOT/services/cms_maps localhost 8230


# for enabling cern service auth via certs
sudo apt-get install voms-clients


# ----  initialization -- needed on every run ----
export DAS_ROOT=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
export DAS_CONFIG=$DAS_ROOT/etc/das.cfg
export YUI_ROOT=$DAS_ROOT/third-party/yui
das_server start

# bootstrap the values and the structure of service output (fields)
$DAS_ROOT/bin/das_bootstrap_kws


#finaly test:
firefox http://localhost:8212/das/

