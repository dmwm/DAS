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

sudo -l
A=/data/cfg/admin REPO="-r comp=comp.pre" VER=1207a

# add -a $PWD/auth for passwords
$A/InstallDev -R cmsweb@$VER -s image -v hg$VER  $REPO -p "admin das dbs mongodb phedex"
$A/InstallDev -s start


# test
$A/InstallDev -s status
firefox http://127.0.0.1:8212/das/
firefox http://localhost:8213/analytics/
