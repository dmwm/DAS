CMS DAS queries
===============
Here we provide concrete examples of DAS queries used in CMS.

Find primary dataset

.. doctest::

   primary_dataset=Cosmics
   primary_dataset=Cosmics | grep primary_dataset.name
   primary_dataset=Cosmics | count(primary_dataset.name)

Find dataset

.. doctest::

   dataset primary_dataset=Cosmics
   dataset dataset=*Cosmic* site=T3_US_FSU
   dataset release=CMSSW_2_0_8
   dataset release=CMSSW_2_0_8 | grep dataset.name, dataset.nevents
   dataset run=148126
   dataset date=20101101

Find block

.. doctest::

   block dataset=/Wgamma/Winter09_IDEAL_V12_FastSim_v1/GEN-SIM-DIGI-RECO
   block=/ExpressPhysics/Commissioning10-Express-v6/FEVT#f86bef6a-86c2-48bc-9f46-2e868c13d86e
   block site=T3_US_Cornell*
   block site=srm-cms.cern.ch | count(block.name), sum(block.replica.size), avg(block.replica.size), median(block.replica.size)

Find file

.. doctest::

   file dataset=/Wgamma/Winter09_IDEAL_V12_FastSim_v1/GEN-SIM-DIGI-RECO
   file block=/ExpressPhysics/Commissioning10-Express-v6/FEVT#f86bef6a-86c2-48bc-9f46-2e868c13d86e
   file dataset=/Wgamma/Winter09_IDEAL_V12_FastSim_v1/GEN-SIM-DIGI-RECO | grep file.name, file.size
   file dataset=/Wgamma/Winter09_IDEAL_V12_FastSim_v1/GEN-SIM-DIGI-RECO | grep file.name, file.size>1500000000
   file dataset=/Wgamma/Winter09_IDEAL_V12_FastSim_v1/GEN-SIM-DIGI-RECO | sum(file.size), count(file.name)
   file block=/ExpressPhysics/Commissioning10-Express-v6/FEVT* site=T2_CH_CAF
   file run=148126 dataset=/ZeroBias/Run2010B-Dec4ReReco_v1/RECO
   file dataset=/ExpressPhysics/Commissioning10-Express-v6/FEVT | grep file.size | max(file.size),min(file.size),avg(file.size),median(file.size)

Find lumi information

.. doctest::

   lumi file=/store/data/Run2010B/ZeroBias/RAW-RECO/v2/000/145/820/784478E3-52C2-DF11-A0CC-0018F3D0969A.root

Find parents/children of a given dataset/files

.. doctest::

   child dataset=/QCDpt30/Summer08_IDEAL_V9_v1/GEN-SIM-RAW
   parent dataset=/QCDpt30/Summer08_IDEAL_V9_skim_hlt_v1/USER
   child file=/store/mc/Summer08/QCDpt30/GEN-SIM-RAW/IDEAL_V9_v1/0000/1EAE7A08-187D-DD11-85B5-001CC47D037C.root
   parent file=/store/mc/Summer08/QCDpt30/USER/IDEAL_V9_skim_hlt_v1/0003/367E05A0-707E-DD11-B0B9-001CC4A6AE4E.root

Find information in local DBS instances

.. doctest::

   instance=cms_dbs_ph_analysis_02 dataset=/QCD_Pt_*_TuneZ2_7TeV_pythia6/wteo-qcd_tunez2_pt*_pythia*

Find run information

.. doctest::

   run=148126
   run in [148124,148126]
   run date last 60m
   run date between [20101010, 20101011]
   run run_status=Complete
   run reco_status=1
   run dataset=/Monitor/Commissioning08-v1/RAW

Find site information

.. doctest::

   site=T1_CH_CERN
   site=T1_CH_CERN | grep site.admin

Jobsummary information

.. doctest::

   jobsummary date last 24h
   jobsummary site=T1_DE_KIT date last 24h
   jobsummary user=ValentinKuznetsov

Special keywords
++++++++++++++++
DAS has a several special keywords: *system, date, instance, records*.

- The *system* keyword is used to retrieve a records only from specified 
  system (data-service), e.g. DBS.
- The *date* can be used in different queries and accepts values in 
  YYYYMMDD format as well as can be specified as *last* value, e.g. 
  *date last 24h, date last 60m*, where h, m are 
  hours, minutes, respectively. 
- The *records* keyword can be used to retrieve DAS records regardless
  from their content. For instance, if one user place a query 
  *site=T1_CH_CERN\**, the DAS requests data from several data-services 
  (Phedex, SiteDB), while the output results will only show site 
  related records. If user wants to see which other records exists 
  in DAS cache for given parameter, he/she can use 
  *records site=T1_CH_CERN\** to do that. In that case user will get back
  all records (site, block records) associated with given condition.

