.. _cms_operations:

DAS CMS Operations
==================

.. toctree::
   :maxdepth: 2

Here we outline CMS specific operations to build/install and maintain
DAS system. Please note, all instructions below refer to $DAS_VER
as DAS version.

Building RPM
------------

For generic CMS build procedure please refer to this 
`page <https://twiki.cern.ch/twiki/bin/view/CMS/BuildingForSLC5>`_. 
We build SLC5 RPMs on vocms82 build node using 64-bit architecture.
Here is a list of build steps:

.. doctest::

    # clean the area
    rm -rf bin bootstraptmp build BUILD cmsset_default.* \
        common RPMS slc5_amd64_gcc434 SOURCES SPECS SRPMS tmp var

    # get PKGTOOLS and CMSDIST
    cvs co -r $DAS_VER PKGTOOLS
    cvs co -r dg20091203b-comp-base CMSDIST
    cvs update -r 1.59 CMSDIST/python.spec

    # update/modify appropriate spec's, e.g.
    # cvs up -A CMSDIST/das.spec

    # perform the build
    export SCRAM_ARCH=slc5_amd64_gcc434
    PKGTOOLS/cmsBuild --architecture=$SCRAM_ARCH --cfg=./build.cfg

The build.cfg file has the following content:

.. doctest::

    [globals]
    assumeYes: True
    onlyOnce: True
    testTag: False
    trace: True
    tag: cmp
    repository: comp
    [bootstrap]
    priority: -30
    doNotBootstrap: True
    repositoryDir: comp
    [build das]
    compilingProcesses: 6
    workersPoolSize: 2
    priority: -20
    #[upload das]
    #priority: 100
    #syncBack: True

Build logs and packages are located in BUILD area.
Once build is complete all CMS RPMs are installed
under slc5_amd64_gcc434 area. Please verify that
DAS RPMs has been installed. Once everything is ok, 
request from CERN operator to upload DAS RPMs into
CMS COMP repository. It can be done using the following
set of commands:

.. doctest::

    eval `ssh-agent -s`
    ssh-add -t 36000
    export SCRAM_ARCH=slc5_amd64_gcc434
    PKGTOOLS/cmsBuild --architecture=$SCRAM_ARCH --cfg=./upload.cfg

where upload.cfg is similar to build.cfg with last three lines commented out.

Installing RPMs
---------------
DAS follows CMS build/install generic procedure. Here we outline all
necessary steps to install CMS DAS RPMs into your area

.. doctest::

    export PROJ_DIR=/data/projects/das # modify accordingly
    export SCRAM_ARCH=slc5_amd64_gcc434
    export APT_VERSION=0.5.15lorg3.2-cmp
    export V=$DAS_VER

    mkdir -p $PROJ_DIR

    wget -O$PROJ_DIR/bootstrap.sh http://cmsrep.cern.ch/cmssw/cms/bootstrap.sh
    chmod +x $PROJ_DIR/bootstrap.sh
    # perform this step only once
    $PROJ_DIR/bootstrap.sh -repository comp -arch $SCRAM_ARCH -path $PROJ_DIR setup 
    cd $PROJ_DIR
    source $SCRAM_ARCH/external/apt/$APT_VERSION/etc/profile.d/init.sh

    apt-get update
    apt-get install cms+das+$V

Updating RPM
------------
Please following these steps to update DAS RPM in your area:

.. doctest::

    export PROJ_DIR=/data/projects/das 
    export SCRAM_ARCH=slc5_amd64_gcc434
    export APT_VERSION=0.5.15lorg3.2-cmp
    export V=$DAS_VER

    cd $PROJ_DIR
    source $SCRAM_ARCH/external/apt/$APT_VERSION/etc/profile.d/init.sh

    apt-get update
    apt-get install cms+das+$V

.. _setup.sh:

setup.sh
--------
In order to run DAS installed via CMS RPMs you need to setup your
environment. Here we provide a simple steps which you can follow
to create a single setup.sh file and use it afterwards:

.. doctest::

  echo "ver=$V" > $PROJ_DIR/setup.sh
  echo "export PROJ_DIR=$PROJ_DIR" >> $PROJ_DIR/setup.sh
  echo "export SCRAM_ARCH=$SCRAM_ARCH" >> $PROJ_DIR/setup.sh
  echo -e "export APT_VERSION=$APT_VERSION\n" >> $PROJ_DIR/setup.sh
  echo 'source $SCRAM_ARCH/external/apt/$APT_VERSION/etc/profile.d/init.sh' >> $PROJ_DIR/setup.sh
  echo 'source $PROJ_DIR/slc4_ia32_gcc345/cms/das/$ver/etc/profile.d/init.sh' >> $PROJ_DIR/setup.sh

