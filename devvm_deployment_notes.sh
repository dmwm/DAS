


sudo yum install git.x86_64



VER=1303b

mkdir -p /tmp/foo
cd /tmp/foo
DEPLOYMENT="git://github.com/dmwm/deployment.git"
DEPLOYMENT="git://github.com/vidma/deployment.git"
# TODO: 
(git clone $DEPLOYMENT cfg && cd cfg && git reset --hard HG$VER )
sudo -l 
cfg/Deploy -t dummy -s post $PWD system/devvm
# OPTIONAL: review what happened: less /tmp/foo/.deploy/*
rm -fr /tmp/foo

#logout or even reboot


# clean up the previous installation, if any
cd /data
$PWD/cfg/admin/InstallDev -s stop
crontab -r
killall python
sudo rm -fr [^aceu]* .??* current enabled




cd /data
# should print out large number local _foo groups now
id 


DEPLOYMENT="git://github.com/vidma/deployment.git"
VER=1303b
rm -Rf cfg
(git clone $DEPLOYMENT cfg && cd /data/cfg && git reset --hard HG$VER )
(cd /data/cfg && git checkout  origin/master ./das/manage )
#git checkout  origin/master ./admin/InstallDev

sudo -l

#install


A=/data/cfg/admin REPO="-r comp=comp.pre.zemleris" VER=1303b

# get auth proxy
mkdir -p $PWD/HG$VER/auth/proxy
$PWD/cfg/admin/ProxySeed -t dev -d $PWD/HG$VER/auth/proxy

# TODO: not yet installed: phedex dbs
$A/InstallDev -R cmsweb@HG$VER -s image -v hg$VER  $REPO -p "admin das@1.11.9-hg1309-rc4 mongodb frontend overview t0datasvc t0mon reqmon"

touch /data/state/frontend/etc/voms-gridmap.txt
# bootstrap & start DAS
$A/InstallDev -s start















bash
cd /data
sudo -l
id
A=/data/cfg/admin REPO="-r comp=comp.pre.zemleris"

# test
$A/InstallDev -s status
firefox http://127.0.0.1:8212/das/
firefox http://localhost:8213/analytics/

# TO ENABLE ACESS FROM OUTSIDE:
# sudo system-config-securitylevel-tui
# customize: add 8212:tcp 





# run TESTS

cd ~/manage_tests /data/current/config/das/
export DAS_ROOT_MOD=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
sudo -H -u _das bashs -lc '/data/current/config/das/manage_tests test_kws '\''I did read documentation'\'''











---- NOT USED BELOW ----
cd /data
sudo -l
id
# add another target test?
export DAS_KWS_IR_INDEX=/data/state/das/das_kws_index/
# init the current installation
. /data/current/config/admin/init.sh
. /data/current/apps/das/etc/profile.d/init.sh
#. /data/current/apps/mongo/etc/profile.d/init.sh
export DAS_ROOT_MOD=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
echo $DAS_ROOT_MOD
export  DAS_CONFIG=/data/current/config/das/das_cms.py
python $DAS_ROOT/test/das_kwdsearch_t.py


# get auth proxy
mkdir -p $PWD/1207a/auth/proxy
$PWD/cfg/admin/ProxySeed -t dev -d $PWD/1207a/auth/proxy




# patch
#/data/hgHG1303b/sw.pre.zemleris/slc5_amd64_gcc461/cms/das/1.11.9-hg1309-rc1/lib/python2.6/site-packages/DAS/keywordsearch/metadata/
#/data/hgHG1303b/sw.pre.zemleris/slc5_amd64_gcc461/cms/das/1.11.9-hg1309-rc1/lib/python2.6/site-packages/DAS/analytics/
#dasroot=`python -c "import DAS; print '/'.join(DAS.__file__.split('/')[:-1])"`


EmptyIndexError: Index 'idx_name' does not exist in FileStorage('/data/state/das/das_kws_index')

echo $DAS_KWS_IR_INDEX





----


# Based on: [1] https://cms-http-group.web.cern.ch/cms-http-group/tutorials/environ/vm-setup.html
# first get a SLC5 x64. See [1]

# TODO: automatic deployment for CERN SLC machines using fabric or at least ssh!

sudo yum install git

#set up machine
kinit
mkdir -p /tmp/foo
cd /tmp/foo
git clone git://github.com/dmwm/deployment.git cfg
sudo -l 
cfg/Deploy -t dummy -s post $PWD system/devvm
less /tmp/foo/.deploy/*
rm -fr /tmp/foo


#logout or even reboot

bash
# TODO: make sure /data was created by set up script
cd /data
id
git clone git://github.com/dmwm/deployment.git cfg

# get auth proxy
mkdir -p $PWD/1207a/auth/proxy
$PWD/cfg/admin/ProxySeed -t dev -d $PWD/1207a/auth/proxy


sudo -l
A=/data/cfg/admin REPO="-r comp=comp.pre" VER=1207a

# add -a $PWD/auth for passwords
# TODO: not yet installed: phedex dbs
$A/InstallDev -R cmsweb@$VER -s image -v hg$VER  $REPO -p "admin das mongodb "
$A/InstallDev -s start



bash
# TODO: make sure /data was created by set up script
cd /data
sudo -l
id
A=/data/cfg/admin REPO="-r comp=comp.pre" VER=1207a
# test
$A/InstallDev -s status
firefox http://127.0.0.1:8212/das/
firefox http://localhost:8213/analytics/

# TO ENABLE ACESS FROM OUTSIDE:
# sudo system-config-securitylevel-tui
# customize: add 8212:tcp 



bash
# TODO: make sure /data was created by set up script
cd /data
sudo -l
A=/data/cfg/admin REPO="-r comp=comp.pre" VER=1207a
$A/InstallDev -s status:das
$A/InstallDev -s stop:das
# ---- UPDATE THE SOURCE FROM GIT ----
# init the current installation
. /data/current/config/admin/init.sh
. /data/current/apps/das/etc/profile.d/init.sh
#patching CMS VM from git
# TO check current das installation locnation
export DAS_ROOT=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
echo $DAS_ROOT
# /data/hg1207a/sw.pre/slc5_amd64_gcc461/cms/das/1.4.2-comp/lib/python2.6/site-packages/DAS

# get from git into
cd /data/git/DAS
git pull
./setup.py build
# make sure das get's installed on the right prefix
sudo -H -u _sw bashs -lc ". /data/current/config/admin/init.sh; ./setup.py -v install --prefix=/data/hg1207a/sw.pre/slc5_amd64_gcc461/cms/das/1.4.2-comp"

# go to hacking installation: renew das-maps and bootstrap kws (if needed)

# restart DAS
cd /data
A=/data/cfg/admin REPO="-r comp=comp.pre" VER=1207a
$A/InstallDev -s start:das


# --- RUN benchmarks ---

# init DAS
screen
cd /data/
bash
sudo -l
A=/data/cfg/admin REPO="-r comp=comp.pre" VER=1207a
#$A/InstallDev -s start
. /data/current/config/admin/init.sh
. /data/current/apps/das/etc/profile.d/init.sh
export DAS_ROOT=`python -c "import os, DAS; print os.path.dirname(DAS.__file__)"`
export DAS_CONFIG=$DAS_ROOT/etc/das.cfg

# run sevrice becnhmark
script bench_web_no_wildc_new
./das_service_benchmark.py
# git/DAS/src/python/DAS/tools/das_service_benchmark.py
