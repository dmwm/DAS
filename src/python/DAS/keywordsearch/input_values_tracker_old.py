__author__ = 'vidma'

import urllib, urllib2
# DAS modules
import DAS.utils.jsonwrapper as json
from DAS.utils.utils import qlxml_parser, dastimestamp, print_exc
from DAS.utils.das_db import db_connection, is_db_alive, create_indexes
from DAS.web.utils import db_monitor
from DAS.utils.utils import get_key_cert, genkey
from DAS.utils.thread import start_new_thread
from DAS.utils.url_utils import HTTPSClientAuthHandler
# sudo easy_install jsonpath
# sudo apt-get install python-lxml

from jsonpath import jsonpath
import os

from pprint import pprint



"""
P.S. use grid-proxy-init for not to be asked for PEM pass
# grid-proxy-init -debug -independent
"""

# TODO: some values may have ALIASES
class InputValueConstraints(dict):
    # entity -> (URI, xquery to fetch values)
    # TODO: finish the json and xquery patterns!

    service_input_value_providers = {
        'site.name': {'url': 'https://cmsweb.cern.ch/sitedb/data/prod/site-names',
                  'jsonpath': "$.result[*]..[2]" },
        'tier.name': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers',
                 'jsonpath': '$..data_tier_name' },
        'datatype.name': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatypes',
                     'jsonpath': '$..data_type' },
        'status.name': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasetaccesstypes',
                   'jsonpath': "$[0]['dataset_access_type'][*]" },
        'release.name': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/releaseversions',
                    'jsonpath': "$[0]['release_version'][*]" },

        # TODO: this also have potentially useful primary_ds_type': 'mc' and creation date
        # TODO: better to store in MongoDB?
        #'primary_dataset': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/primarydatasets',                            'jsonpath': '$..primary_ds_name' },
        'group.name': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/physicsgroups',
                  'jsonpath': '$..physics_group_name' },

    # TODO: site is XML/json, it is also in sitedb collection

    }


    # for dataset (dataset listing + wildcard) and block (regexp) we have special classes
    # TODO: support wildcard for other values (site, )


    def fetch_values(self, service):
        # request list of possible values
        params = {}
        encoded_data = urllib.urlencode(params, doseq=True)
        url =  service['url'] + encoded_data
        print str(url)
        req = urllib2.Request(url)

        # ensure we get json (sitedb is messed and randomly returns xml)
        if service['jsonpath']:
            req.add_header('Accept', 'application/json')
        #print req.get_full_url()



        stream = urllib2.urlopen(req)
        #stream = urllib2.urlopen(url)

        # parse the response, TODO: JSON only


        if service['jsonpath']:
            response = json.load(stream)
            results = jsonpath(response, service['jsonpath'])
            stream.close()
            return results


        return []

    def __init__(self):
        super(InputValueConstraints,self).__init__()

        # uses grid-proxy
        uid  = os.getuid()
        cert = '/tmp/x509up_u'+str(uid)
        ckey  = cert

        # TODO: if I use this, it asks for PEM (from my laptop)
        ckey, cert = get_key_cert()

        handler = HTTPSClientAuthHandler(ckey, cert)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)


        for (entity, service) in self.service_input_value_providers.items():
            self[entity] = self.fetch_values(service)

        # fetch the urls

    def __getitem__( self, *args, **kwrags):
        #if not self.has_key( key ):
        #    super(RetrieveServiceInputConstraints,self).__setitem__( key, [] )
        return super(InputValueConstraints,self).__getitem__(*args, **kwrags)






# input_values = InputValueConstraints()
# TODO: temporary I'm using static copy of all available values

input_values = {'status.name': [u'DELETED', u'DEPRECATED', u'INVALID', u'PROCESSING', u'PRODUCTION', u'VALID'],
                'group.name': [u'ALLGROUP', u'Analysis', u'AnalysisOps', u'Any', u'BoostedTop', u'Bphys', u'Btag', u'COMMON', u'CSA07', u'DBS3Test', u'DPG', u'DataOOps', u'DataOps', u'Diffraction', u'EWK', u'Egamma', u'FastSim', u'Generator', u'Generators', u'HCAL', u'HI', u'HeavyIon', u'HeavyIonPhysics', u'Higgs', u'Individual', u'JetMet', u'Maarten', u'Muons', u'NoGroup', u'OnSel', u'PFlowTau', u'PhysVal', u'Physics', u'QCD', u'RelVal', u'SUSYBSM', u'TEST', u'TOP', u'Tier0', u'Top', u'Tracker', u'Trigger', u'UCSD', u'b-physics', u'b-tagging', u'data', u'dataOps', u'e-gamma_ecal', u'ewk', u'exotica', u'heavy-ions', u'higgs', u'jets-met_hcal', u'muon', u'susy', u'tau-pflow', u'test', u'tracker-pog'],
                'datatype.name': [u'DATA', u'align', u'calib', u'cosmic', u'data', u'lumi', u'mc', u'raw', u'test'],
                'site.name': [u'T1_TW_ASGC', u'T3_BY_NCPHEP', u'T2_IT_Bari', u'T3_US_Baylor', u'T2_CN_Beijing', u'T3_IT_Bologna', u'T2_UK_SGrid_Bristol', u'T3_US_Brown', u'T2_UK_London_Brunel', u'T1_FR_CCIN2P3', u'T2_FR_CCIN2P3', u'T1_CH_CERN', u'T0_CH_CERN', u'T2_CH_CERN', u'T2_ES_CIEMAT', u'T1_IT_CNAF', u'T3_CN_PKU', u'T2_CH_CSCS', u'T2_TH_CUNSTDA', u'T2_US_Caltech', u'T3_US_Colorado', u'T3_US_Cornell', u'T2_PL_Cracow', u'T2_DE_DESY', u'T3_GR_Demokritos', u'T3_UK_ScotGrid_ECDF', u'T2_EE_Estonia', u'T3_RU_FIAN', u'T3_US_FIT', u'T1_US_FNAL', u'T1_US_FNAL_Disk', u'T3_US_FNALLPC', u'T3_US_FNALXEN', u'T3_US_Omaha', u'T3_IT_Firenze', u'T2_US_Florida', u'T2_FR_GRIF_IRFU', u'T2_FR_GRIF_LLR', u'T2_BR_UERJ', u'T2_FI_HIP', u'T2_AT_Vienna', u'T2_HU_Budapest', u'T3_GR_IASA', u'T2_UK_London_IC', u'T2_ES_IFCA', u'T2_RU_IHEP', u'T2_BE_IIHE', u'T3_FR_IPNL', u'T3_IT_MIB', u'T3_IT_Napoli', u'T2_RU_INR', u'T2_FR_IPHC', u'T3_IR_IPM', u'T2_RU_ITEP', u'T2_GR_Ioannina', u'T3_US_JHU', u'T2_RU_JINR', u'T2_UA_KIPT', u'T1_DE_KIT', u'T2_KR_KNU', u'T3_KR_KNU', u'T3_US_Kansas', u'T2_PT_LIP_Lisbon', u'T2_IT_Legnaro', u'T2_BE_UCL', u'T2_TR_METU', u'T2_US_MIT', u'T3_US_Minnesota', u'T2_PT_NCG_Lisbon', u'T2_PK_NCP', u'T3_TW_NCU', u'T3_TW_NTU_HEP', u'T3_US_NotreDame', u'T3_NZ_UOA', u'T2_US_Nebraska', u'T3_US_OSU', u'T3_ES_Oviedo', u'T3_UK_SGrid_Oxford', u'T1_ES_PIC', u'T2_RU_PNPI', u'T3_CH_PSI', u'T3_IT_Perugia', u'T2_IT_Pisa', u'T3_US_Princeton', u'T3_US_Princeton_ICSE', u'T2_US_Purdue', u'T3_UK_London_QMUL', u'T1_UK_RAL', u'T1_UK_RAL_Disk', u'T3_UK_London_RHUL', u'T2_RU_RRC_KI', u'T2_DE_RWTH', u'T3_US_Rice', u'T2_IT_Rome', u'T3_US_Rutgers', u'T2_UK_SGrid_RALPP', u'T2_RU_SINP', u'T2_BR_SPRACE', u'T3_US_FIU', u'T3_US_FSU', u'T3_US_TAMU', u'T2_IN_TIFR', u'T3_US_TTU', u'T2_TW_Taiwan', u'T3_IT_Trieste', u'T3_US_UCR', u'T3_US_UCD', u'T3_US_UCLA', u'T2_US_UCSD', u'T3_US_UIowa', u'T3_UK_ScotGrid_GLA', u'T3_US_UMD', u'T3_US_UMiss', u'T3_CO_Uniandes', u'T3_KR_UOS', u'T3_US_PuertoRico', u'T3_US_UTENN', u'T3_US_UVA', u'T3_UK_London_UCL', u'T2_US_Vanderbilt', u'T3_US_Vanderbilt_EC2', u'T2_PL_Warsaw', u'T2_US_Wisconsin', u'T3_MX_Cinvestav', u'Taiwan-LCG2', u'BY-NCPHEP', u'INFN-BARI', u'Baylor-Tier3', u'BEIJING-LCG2', u'INFN-BOLOGNA-T3', u'UKI-SOUTHGRID-BRIS-HEP', u'brown-cms-new', u'UKI-LT2-Brunel', u'IN2P3-CC', u'IN2P3-CC-T2', u'CERN-PROD', u'CERN-PROD', u'CERN-PROD', u'CIEMAT-LCG2', u'INFN-T1', u'CN-BEIJING-PKU', u'CSCS-LCG2', u'T2-TH-CUNSTDA', u'CIT_CMS_T2', u'UColorado_HEP', u'NYSGRID_CORNELL_NYS1', u'CYFRONET-LCG2', u'DESY-HH', u'GR-05-DEMOKRITOS', u'UKI-SCOTGRID-ECDF', u'T2_Estonia', u'ru-Moscow-FIAN-LCG2', u'FLTECH', u'USCMS-FNAL-WC1', u'USCMS-FNAL-WC1-DISK', u'USCMS-FNAL-LPC', u'USCMS-FNAL-XEN', u'Firefly', u'INFN-FIRENZE', u'UFlorida-HPC', u'IRFU', u'GRIF', u'HEPGRID_UERJ', u'FI_HIP_T2', u'Hephy-Vienna', u'BUDAPEST', u'GR-06-IASA', u'HG-02-IASA', u'UKI-LT2-IC-HEP', u'IFCA-LCG2', u'RU-Protvino-IHEP', u'BEgrid-ULB-VUB', u'IN2P3-IPNL', u'INFN-MIB', u'INFN-NAPOLI-CMS', u'Ru-Troitsk-INR-LCG2', u'IN2P3-IRES', u'IR-IPM-HEP', u'ITEP', u'GR-07-UOI-HEPLAB', u'T3_US_JHU', u'JINR-LCG2', u'Kharkov-KIPT-LCG2', u'FZK-LCG2', u'LCG_KNU', u'KR-KNU-T3', u'T3_US_Kansas', u'LIP-Lisbon', u'INFN-LNL-2', u'BelGrid-UCL', u'TR-03-METU', u'MIT_CMS', u'UMN-CMS', u'NCG-INGRID-PT', u'NCP-LCG2', u'TW-NCUHEP', u'TW-NTU-HEP', u'NWICG_NDCMS', u'NZ-UOA', u'Nebraska', u'osu-cms', u'UOGRID', u'UKI-SOUTHGRID-OX-HEP', u'pic', u'ru-PNPI', u'T3_CH_PSI', u'INFN-PERUGIA', u'INFN-PISA', u'Princeton', u'T3_US_Princeton_ICSE', u'Purdue-Carter', u'Purdue-Hansen', u'Purdue-RCAC', u'Purdue-Rossmann', u'Purdue-Steele', u'UKI-LT2-QMUL', u'RAL-LCG2', u'RAL-LCG2-DISK', u'UKI-LT2-RHUL', u'RRC-KI', u'RWTH-Aachen', u'Rice', u'INFN-ROMA1-CMS', u'rutgers-cms', u'UKI-SOUTHGRID-RALPP', u'ru-Moscow-SINP-LCG2', u'SPRACE', u'FIUPG', u'FSU-HEP', u'TAMU_BRAZOS', u'INDIACMS-TIFR', u'ttu-antaeus', u'TW-FTT', u'INFN-TRIESTE', u'UCR-HEP', u'UCD', u'UCLA_Saxon_Tier3', u'UCSDT2', u'UCSDT2-B', u'GROW-PROD', u'UKI-SCOTGRID-GLASGOW', u'umd-cms', u'UMissHEP', u'UNIANDES', u'KR_UOS_SSCC', u'uprm-cms', u'T3_US_UTENN', u'UVA-HEP', u'UKI-LT2-UCL-HEP', u'Vanderbilt', u'Vanderbilt_EC2', u'ICM', u'GLOW', u'cinvestav', u'T1_TW_ASGC_Buffer', u'T1_TW_ASGC_MSS', u'T1_TW_ASGC_Stage', u'T3_BY_NCPHEP', u'T2_IT_Bari', u'T3_US_Baylor', u'T2_CN_Beijing', u'T3_IT_Bologna', u'T2_UK_SGrid_Bristol', u'T3_US_Brown', u'T2_UK_London_Brunel', u'T1_FR_CCIN2P3_Buffer', u'T1_FR_CCIN2P3_MSS', u'T2_FR_CCIN2P3', u'T1_CH_CERN_Buffer', u'T1_CH_CERN_MSS', u'T0_CH_CERN_Export', u'T0_CH_CERN_MSS', u'T2_CH_CERN', u'T2_ES_CIEMAT', u'T1_IT_CNAF_Buffer', u'T1_IT_CNAF_Disk', u'T1_IT_CNAF_MSS', u'T3_CN_PKU', u'T2_CH_CSCS', u'T2_TH_CUNSTDA', u'T2_US_Caltech', u'T3_US_Colorado', u'T3_US_Cornell', u'T2_PL_Cracow', u'T2_DE_DESY', u'T3_GR_Demokritos', u'T3_UK_ScotGrid_ECDF', u'T2_EE_Estonia', u'T3_US_FIT', u'T1_US_FNAL_Buffer', u'T1_US_FNAL_MSS', u'T1_US_FNAL_Disk', u'T3_US_FNALLPC', u'T3_US_FNALXEN', u'T3_US_Omaha', u'T3_IT_Firenze', u'T2_US_Florida', u'T2_FR_GRIF_IRFU', u'T2_FR_GRIF_LLR', u'T2_BR_UERJ', u'T2_FI_HIP', u'T2_AT_Vienna', u'T2_HU_Budapest', u'T3_GR_IASA_GR', u'T3_GR_IASA_HG', u'T2_UK_London_IC', u'T2_ES_IFCA', u'T2_RU_IHEP', u'T2_BE_IIHE', u'T3_FR_IPNL', u'T3_IT_MIB', u'T3_IT_Napoli', u'T2_RU_INR', u'T2_FR_IPHC', u'T3_IR_IPM', u'T2_RU_ITEP', u'T2_GR_Ioannina', u'T3_US_JHU', u'T2_RU_JINR', u'T2_UA_KIPT', u'T1_DE_KIT_Buffer', u'T1_DE_KIT_MSS', u'T2_KR_KNU', u'T3_KR_KNU', u'T3_US_Kansas', u'T2_PT_LIP_Lisbon', u'T2_IT_Legnaro', u'T2_BE_UCL', u'T2_TR_METU', u'T2_US_MIT', u'T3_US_Minnesota', u'T2_PT_NCG_Lisbon', u'T2_PK_NCP', u'T3_TW_NCU', u'T3_TW_NTU_HEP', u'T3_US_NotreDame', u'T3_NZ_UOA', u'T2_US_Nebraska', u'T3_US_OSU', u'T3_ES_Oviedo', u'T3_UK_SGrid_Oxford', u'T1_ES_PIC_Buffer', u'T1_ES_PIC_MSS', u'T2_RU_PNPI', u'T3_CH_PSI', u'T3_IT_Perugia', u'T2_IT_Pisa', u'T3_US_Princeton', u'T3_US_Princeton_ICSE', u'T2_US_Purdue', u'T3_UK_London_QMUL', u'T1_UK_RAL_Buffer', u'T1_UK_RAL_MSS', u'T1_UK_RAL_Stage', u'T1_UK_RAL_Disk', u'T3_UK_London_RHUL', u'T2_RU_RRC_KI', u'T2_DE_RWTH', u'T3_US_Rice', u'T2_IT_Rome', u'T3_US_Rutgers', u'T2_UK_SGrid_RALPP', u'T2_RU_SINP', u'T2_BR_SPRACE', u'T3_US_FIU', u'T3_US_FSU', u'T3_US_TAMU', u'T2_IN_TIFR', u'T3_US_TTU', u'T2_TW_Taiwan', u'T3_IT_Trieste', u'T3_US_UCR', u'T3_US_UCD', u'T3_US_UCLA', u'T2_US_UCSD', u'T3_US_UIowa', u'T3_UK_ScotGrid_GLA', u'T3_US_UMD', u'T3_US_UMiss', u'T3_CO_Uniandes', u'T3_KR_UOS', u'T3_US_PuertoRico', u'T3_US_UTENN', u'T3_US_UVA', u'T3_UK_London_UCL', u'T2_US_Vanderbilt', u'T3_US_Vanderbilt_EC2', u'T2_PL_Warsaw', u'T2_US_Wisconsin', u'T3_MX_Cinvestav'],
                'release.name': [u'2', u'CMSSW', u'CMSSWError', u'CMSSW_1_1_0', u'CMSSW_1_1_1', u'CMSSW_1_2_0', u'CMSSW_1_2_0_g4_81', u'CMSSW_1_2_1', u'CMSSW_1_2_2', u'CMSSW_1_2_3', u'CMSSW_1_2_4', u'CMSSW_1_2_6', u'CMSSW_1_3_0', u'CMSSW_1_3_0_pre3', u'CMSSW_1_3_0_pre5', u'CMSSW_1_3_0_pre6', u'CMSSW_1_3_1', u'CMSSW_1_3_1_HLT1', u'CMSSW_1_3_1_HLT2', u'CMSSW_1_3_2', u'CMSSW_1_3_3', u'CMSSW_1_3_4', u'CMSSW_1_3_5', u'CMSSW_1_3_6', u'CMSSW_1_4_0', u'CMSSW_1_4_0_DAQ1', u'CMSSW_1_4_0_pre7', u'CMSSW_1_4_1', u'CMSSW_1_4_12', u'CMSSW_1_4_2', u'CMSSW_1_4_3', u'CMSSW_1_4_4', u'CMSSW_1_4_5', u'CMSSW_1_4_6', u'CMSSW_1_4_7', u'CMSSW_1_4_8', u'CMSSW_1_4_9', u'CMSSW_1_5_0', u'CMSSW_1_5_0_pre5', u'CMSSW_1_5_0_pre6', u'CMSSW_1_5_1', u'CMSSW_1_5_2', u'CMSSW_1_5_3', u'CMSSW_1_5_4', u'CMSSW_1_6_0', u'CMSSW_1_6_0_DAQ1', u'CMSSW_1_6_0_DAQ2', u'CMSSW_1_6_0_DAQ3', u'CMSSW_1_6_0_pre10', u'CMSSW_1_6_0_pre11', u'CMSSW_1_6_0_pre13', u'CMSSW_1_6_0_pre14', u'CMSSW_1_6_0_pre4', u'CMSSW_1_6_0_pre5', u'CMSSW_1_6_0_pre6', u'CMSSW_1_6_0_pre7', u'CMSSW_1_6_0_pre8', u'CMSSW_1_6_0_pre9', u'CMSSW_1_6_1', u'CMSSW_1_6_10', u'CMSSW_1_6_10_pre1', u'CMSSW_1_6_12', u'CMSSW_1_6_1_pre1', u'CMSSW_1_6_2_pre1', u'CMSSW_1_6_3', u'CMSSW_1_6_4', u'CMSSW_1_6_5', u'CMSSW_1_6_5_pre1', u'CMSSW_1_6_5_pre2', u'CMSSW_1_6_6', u'CMSSW_1_6_7', u'CMSSW_1_6_8', u'CMSSW_1_6_8_pre1', u'CMSSW_1_6_8_pre2', u'CMSSW_1_6_9', u'CMSSW_1_6_9_pre2', u'CMSSW_1_7_0', u'CMSSW_1_7_0_pre10', u'CMSSW_1_7_0_pre11', u'CMSSW_1_7_0_pre12', u'CMSSW_1_7_0_pre13', u'CMSSW_1_7_0_pre3', u'CMSSW_1_7_0_pre4', u'CMSSW_1_7_0_pre5', u'CMSSW_1_7_0_pre6', u'CMSSW_1_7_1', u'CMSSW_1_7_2', u'CMSSW_1_7_3', u'CMSSW_1_7_4', u'CMSSW_1_7_5', u'CMSSW_1_7_6', u'CMSSW_1_7_7', u'CMSSW_1_8_0', u'CMSSW_1_8_0_pre1', u'CMSSW_1_8_0_pre10', u'CMSSW_1_8_0_pre3a', u'CMSSW_1_8_0_pre4', u'CMSSW_1_8_0_pre5', u'CMSSW_1_8_0_pre6', u'CMSSW_1_8_0_pre8', u'CMSSW_1_8_0_pre9', u'CMSSW_1_8_1', u'CMSSW_1_8_2', u'CMSSW_1_8_3', u'CMSSW_1_8_4', u'CMSSW_2_0_0', u'CMSSW_2_0_0_pre1', u'CMSSW_2_0_0_pre2', u'CMSSW_2_0_0_pre3', u'CMSSW_2_0_0_pre4', u'CMSSW_2_0_0_pre5', u'CMSSW_2_0_0_pre6', u'CMSSW_2_0_0_pre7', u'CMSSW_2_0_0_pre8', u'CMSSW_2_0_0_pre9', u'CMSSW_2_0_10', u'CMSSW_2_0_10_ONLINE1', u'CMSSW_2_0_11', u'CMSSW_2_0_12', u'CMSSW_2_0_2', u'CMSSW_2_0_3', u'CMSSW_2_0_4', u'CMSSW_2_0_4_ONLINE1', u'CMSSW_2_0_5', u'CMSSW_2_0_6', u'CMSSW_2_0_7', u'CMSSW_2_0_8', u'CMSSW_2_0_8_ONLINE1-cms2', u'CMSSW_2_0_9', u'CMSSW_2_1_0', u'CMSSW_2_1_0_pre1', u'CMSSW_2_1_0_pre10', u'CMSSW_2_1_0_pre11', u'CMSSW_2_1_0_pre2', u'CMSSW_2_1_0_pre3', u'CMSSW_2_1_0_pre4', u'CMSSW_2_1_0_pre5', u'CMSSW_2_1_0_pre6', u'CMSSW_2_1_0_pre8', u'CMSSW_2_1_0_pre9', u'CMSSW_2_1_1', u'CMSSW_2_1_10', u'CMSSW_2_1_10_patch1', u'CMSSW_2_1_11', u'CMSSW_2_1_12', u'CMSSW_2_1_17', u'CMSSW_2_1_19', u'CMSSW_2_1_2', u'CMSSW_2_1_3', u'CMSSW_2_1_4', u'CMSSW_2_1_5', u'CMSSW_2_1_6', u'CMSSW_2_1_7', u'CMSSW_2_1_8', u'CMSSW_2_1_9', u'CMSSW_2_2_0', u'CMSSW_2_2_0_pre1', u'CMSSW_2_2_1', u'CMSSW_2_2_10', u'CMSSW_2_2_11', u'CMSSW_2_2_11_offpatch1', u'CMSSW_2_2_12', u'CMSSW_2_2_13', u'CMSSW_2_2_13_offpatch1', u'CMSSW_2_2_2', u'CMSSW_2_2_3', u'CMSSW_2_2_4', u'CMSSW_2_2_5', u'CMSSW_2_2_6', u'CMSSW_2_2_7', u'CMSSW_2_2_8', u'CMSSW_2_2_9', u'CMSSW_3_0_0_pre10', u'CMSSW_3_0_0_pre2', u'CMSSW_3_0_0_pre3', u'CMSSW_3_0_0_pre5', u'CMSSW_3_0_0_pre6', u'CMSSW_3_0_0_pre7', u'CMSSW_3_0_0_pre9', u'CMSSW_3_10_0', u'CMSSW_3_10_0_pre1', u'CMSSW_3_10_0_pre2', u'CMSSW_3_10_0_pre3', u'CMSSW_3_10_0_pre4', u'CMSSW_3_10_0_pre5', u'CMSSW_3_10_0_pre7', u'CMSSW_3_10_0_pre7g494c1', u'CMSSW_3_10_0_pre7r52706b', u'CMSSW_3_10_0_pre8', u'CMSSW_3_10_0_pre9', u'CMSSW_3_10_0_pre9G493', u'CMSSW_3_10_0_pre9r52706b', u'CMSSW_3_10_1', u'CMSSW_3_11_0', u'CMSSW_3_11_0_pre2', u'CMSSW_3_11_0_pre3', u'CMSSW_3_11_0_pre5', u'CMSSW_3_11_0_pre5r52706b', u'CMSSW_3_11_0_pre5r52706bT2', u'CMSSW_3_11_1', u'CMSSW_3_11_1_hclpatch1', u'CMSSW_3_11_1_hltpatch1', u'CMSSW_3_11_1_patch1', u'CMSSW_3_11_1_patch2', u'CMSSW_3_11_1_patch3', u'CMSSW_3_11_2', u'CMSSW_3_11_3', u'CMSSW_3_1_0', u'CMSSW_3_1_0_patch1', u'CMSSW_3_1_0_pre1', u'CMSSW_3_1_0_pre10', u'CMSSW_3_1_0_pre11', u'CMSSW_3_1_0_pre2', u'CMSSW_3_1_0_pre3', u'CMSSW_3_1_0_pre4', u'CMSSW_3_1_0_pre5', u'CMSSW_3_1_0_pre6', u'CMSSW_3_1_0_pre7', u'CMSSW_3_1_0_pre8', u'CMSSW_3_1_0_pre9', u'CMSSW_3_1_1', u'CMSSW_3_1_1_patch1', u'CMSSW_3_1_2', u'CMSSW_3_1_3', u'CMSSW_3_1_4', u'CMSSW_3_1_5', u'CMSSW_3_1_6', u'CMSSW_3_2_0', u'CMSSW_3_2_1', u'CMSSW_3_2_2', u'CMSSW_3_2_2_patch1', u'CMSSW_3_2_2_patch2', u'CMSSW_3_2_3', u'CMSSW_3_2_4', u'CMSSW_3_2_4_patch1', u'CMSSW_3_2_5', u'CMSSW_3_2_6', u'CMSSW_3_2_7', u'CMSSW_3_2_8', u'CMSSW_3_3_0', u'CMSSW_3_3_0_pre1', u'CMSSW_3_3_0_pre2', u'CMSSW_3_3_0_pre3', u'CMSSW_3_3_0_pre4', u'CMSSW_3_3_0_pre5', u'CMSSW_3_3_0_pre6', u'CMSSW_3_3_1', u'CMSSW_3_3_2', u'CMSSW_3_3_3', u'CMSSW_3_3_3_patch1', u'CMSSW_3_3_4', u'CMSSW_3_3_5', u'CMSSW_3_3_5_patch1', u'CMSSW_3_3_5_patch2', u'CMSSW_3_3_5_patch3', u'CMSSW_3_3_5_patch4', u'CMSSW_3_3_6', u'CMSSW_3_3_6_patch1', u'CMSSW_3_3_6_patch2', u'CMSSW_3_3_6_patch3', u'CMSSW_3_3_6_patch4', u'CMSSW_3_3_6_patch5', u'CMSSW_3_3_6_patch6', u'CMSSW_3_4_0', u'CMSSW_3_4_0_pre1', u'CMSSW_3_4_0_pre2', u'CMSSW_3_4_0_pre3', u'CMSSW_3_4_0_pre4', u'CMSSW_3_4_0_pre5', u'CMSSW_3_4_0_pre6', u'CMSSW_3_4_0_pre7', u'CMSSW_3_4_1', u'CMSSW_3_4_2', u'CMSSW_3_4_2_patch1', u'CMSSW_3_5_0', u'CMSSW_3_5_0_patch1', u'CMSSW_3_5_0_patch_1', u'CMSSW_3_5_0_pre1', u'CMSSW_3_5_0_pre2', u'CMSSW_3_5_0_pre3', u'CMSSW_3_5_0_pre5', u'CMSSW_3_5_0_pre5g493', u'CMSSW_3_5_1', u'CMSSW_3_5_1_patch1', u'CMSSW_3_5_2', u'CMSSW_3_5_2_patch2', u'CMSSW_3_5_3', u'CMSSW_3_5_4', u'CMSSW_3_5_4_patch1', u'CMSSW_3_5_5', u'CMSSW_3_5_6', u'CMSSW_3_5_6_patch1', u'CMSSW_3_5_7', u'CMSSW_3_5_7_hltpatch4', u'CMSSW_3_5_8', u'CMSSW_3_5_8_patch2', u'CMSSW_3_5_8_patch3', u'CMSSW_3_5_8_patch4', u'CMSSW_3_6_0', u'CMSSW_3_6_0_patch2', u'CMSSW_3_6_0_pre1', u'CMSSW_3_6_0_pre2', u'CMSSW_3_6_0_pre3', u'CMSSW_3_6_0_pre4', u'CMSSW_3_6_0_pre5', u'CMSSW_3_6_0_pre6', u'CMSSW_3_6_1', u'CMSSW_3_6_1_patch1', u'CMSSW_3_6_1_patch2', u'CMSSW_3_6_1_patch3', u'CMSSW_3_6_1_patch4', u'CMSSW_3_6_1_patch5', u'CMSSW_3_6_1_patch6', u'CMSSW_3_6_1_patch7', u'CMSSW_3_6_2', u'CMSSW_3_6_3', u'CMSSW_3_6_3_SLHC1', u'CMSSW_3_6_3_SLHC1_patch1', u'CMSSW_3_6_3_SLHC1_patch3', u'CMSSW_3_6_3_SLHC3_patch1', u'CMSSW_3_6_3_hltpatch4', u'CMSSW_3_6_3_patch1', u'CMSSW_3_6_3_patch2', u'CMSSW_3_7_0', u'CMSSW_3_7_0_patch1', u'CMSSW_3_7_0_patch2', u'CMSSW_3_7_0_patch3', u'CMSSW_3_7_0_patch4', u'CMSSW_3_7_0_pre1', u'CMSSW_3_7_0_pre2', u'CMSSW_3_7_0_pre3', u'CMSSW_3_7_0_pre4', u'CMSSW_3_7_0_pre5', u'CMSSW_3_7_1', u'CMSSW_3_8_0', u'CMSSW_3_8_0_patch1', u'CMSSW_3_8_0_patch2', u'CMSSW_3_8_0_pre1', u'CMSSW_3_8_0_pre2', u'CMSSW_3_8_0_pre4', u'CMSSW_3_8_0_pre4catfix', u'CMSSW_3_8_0_pre5', u'CMSSW_3_8_0_pre6', u'CMSSW_3_8_0_pre7', u'CMSSW_3_8_0_pre8', u'CMSSW_3_8_1', u'CMSSW_3_8_1_patch1', u'CMSSW_3_8_1_patch2', u'CMSSW_3_8_1_patch3', u'CMSSW_3_8_1_patch4', u'CMSSW_3_8_2', u'CMSSW_3_8_2_patch1', u'CMSSW_3_8_3', u'CMSSW_3_8_4', u'CMSSW_3_8_4_patch2', u'CMSSW_3_8_4_patch3', u'CMSSW_3_8_4_patch4', u'CMSSW_3_8_5', u'CMSSW_3_8_5_patch1', u'CMSSW_3_8_5_patch2', u'CMSSW_3_8_5_patch3', u'CMSSW_3_8_6', u'CMSSW_3_8_6_patch1', u'CMSSW_3_8_6_patch2', u'CMSSW_3_8_7', u'CMSSW_3_9_0', u'CMSSW_3_9_0_pre1', u'CMSSW_3_9_0_pre2', u'CMSSW_3_9_0_pre3', u'CMSSW_3_9_0_pre4', u'CMSSW_3_9_0_pre5', u'CMSSW_3_9_0_pre6', u'CMSSW_3_9_0_pre7', u'CMSSW_3_9_1', u'CMSSW_3_9_1_patch1', u'CMSSW_3_9_2', u'CMSSW_3_9_2_patch1', u'CMSSW_3_9_2_patch2', u'CMSSW_3_9_2_patch3', u'CMSSW_3_9_2_patch4', u'CMSSW_3_9_2_patch5', u'CMSSW_3_9_3', u'CMSSW_3_9_4', u'CMSSW_3_9_5', u'CMSSW_3_9_5_patch1', u'CMSSW_3_9_5_patch2', u'CMSSW_3_9_6', u'CMSSW_3_9_7', u'CMSSW_3_9_8', u'CMSSW_3_9_8_patch1', u'CMSSW_3_9_8_patch2', u'CMSSW_3_9_9', u'CMSSW_3_9_9_patch1', u'CMSSW_4_1_0_pre1', u'CMSSW_4_1_0_pre2', u'CMSSW_4_1_1', u'CMSSW_4_1_2', u'CMSSW_4_1_2_patch1', u'CMSSW_4_1_3', u'CMSSW_4_1_3_patch2', u'CMSSW_4_1_3_patch3', u'CMSSW_4_1_4', u'CMSSW_4_1_4_patch1', u'CMSSW_4_1_4_patch2', u'CMSSW_4_1_4_patch3', u'CMSSW_4_1_4_patch4', u'CMSSW_4_1_5', u'CMSSW_4_1_6', u'CMSSW_4_1_6_patch1', u'CMSSW_4_1_7', u'CMSSW_4_1_7_patch1', u'CMSSW_4_1_7_patch2', u'CMSSW_4_1_7_patch3', u'CMSSW_4_1_8', u'CMSSW_4_1_8_patch1', u'CMSSW_4_2_0', u'CMSSW_4_2_0_pre1', u'CMSSW_4_2_0_pre2', u'CMSSW_4_2_0_pre4', u'CMSSW_4_2_0_pre5', u'CMSSW_4_2_0_pre6', u'CMSSW_4_2_0_pre7', u'CMSSW_4_2_0_pre8', u'CMSSW_4_2_1', u'CMSSW_4_2_1_patch1', u'CMSSW_4_2_1_patch2', u'CMSSW_4_2_2', u'CMSSW_4_2_2_SLHC_pre1', u'CMSSW_4_2_2_patch1', u'CMSSW_4_2_2_patch2', u'CMSSW_4_2_3', u'CMSSW_4_2_3_SLHC2', u'CMSSW_4_2_3_SLHC3', u'CMSSW_4_2_3_SLHC_pre1', u'CMSSW_4_2_3_patch1', u'CMSSW_4_2_3_patch2', u'CMSSW_4_2_3_patch3', u'CMSSW_4_2_3_patch5', u'CMSSW_4_2_4', u'CMSSW_4_2_4_g93p01', u'CMSSW_4_2_4_g94p02', u'CMSSW_4_2_4_hltpatch1', u'CMSSW_4_2_4_patch1', u'CMSSW_4_2_4_patch2', u'CMSSW_4_2_5', u'CMSSW_4_2_6', u'CMSSW_4_2_7', u'CMSSW_4_2_7_patch1', u'CMSSW_4_2_7_patch2', u'CMSSW_4_2_8', u'CMSSW_4_2_8_patch1', u'CMSSW_4_2_8_patch2', u'CMSSW_4_2_8_patch3', u'CMSSW_4_2_8_patch4', u'CMSSW_4_2_8_patch6', u'CMSSW_4_2_9_HLT', u'CMSSW_4_2_9_HLT1', u'CMSSW_4_2_9_HLT1_hltpatch1', u'CMSSW_4_2_9_HLT1_patch1', u'CMSSW_4_2_9_HLT3_hltpatch2', u'CMSSW_4_2_9_HLT3_hltpatch3', u'CMSSW_4_3_0', u'CMSSW_4_3_0_dqmpatch2', u'CMSSW_4_3_0_pre1', u'CMSSW_4_3_0_pre2', u'CMSSW_4_3_0_pre3', u'CMSSW_4_3_0_pre4', u'CMSSW_4_3_0_pre5', u'CMSSW_4_3_0_pre6', u'CMSSW_4_3_0_pre7', u'CMSSW_4_4_0', u'CMSSW_4_4_0_patch1', u'CMSSW_4_4_0_patch2', u'CMSSW_4_4_0_patch3', u'CMSSW_4_4_0_pre1', u'CMSSW_4_4_0_pre10', u'CMSSW_4_4_0_pre2', u'CMSSW_4_4_0_pre3', u'CMSSW_4_4_0_pre4', u'CMSSW_4_4_0_pre5', u'CMSSW_4_4_0_pre6', u'CMSSW_4_4_0_pre7', u'CMSSW_4_4_0_pre7_g494p02', u'CMSSW_4_4_0_pre8', u'CMSSW_4_4_0_pre9', u'CMSSW_4_4_1', u'CMSSW_5_0_0_pre2', u'CMSSW_5_0_0_pre3', u'CVS_HEAD', u'TestVersion01_20070523_22h56m14s', u'UNKNOWN', u'Undefined', u'Unknown', u'v001', u'v1'],
                'tier.name': [u'GEN-SIM-DIGI-RECODEBUG', u'GEN-SIM-RAWDEBUG', u'RAWDEBUG', u'DQM', u'ALCAPROMPT', u'GEN-SIM-RECODEBUG', u'RECODEBUG', u'GEN-SIM-RAW-HLT', u'HLT', u'FEVT', u'HLTDEBUG', u'RAW-HLT', u'RAW-HLY', u'HLY', u'FEVTDEBUGHLT', u'AODSIM', u'AOD', u'USER', u'ALCARECO', u'RECO', u'DIGI', u'SIM', u'GEN', u'RAW', u'RAW-RECO', u'GEN-SIM-RECO', u'GEN-SIM-RAW-RECO', u'GEN-SIM-RAW-HLTDEBUG-RECO', u'GEN-SIM-RAW-HLTDEBUG', u'GEN-SIM-RAW', u'GEN-SIM-DIGI-RECO', u'GEN-SIM-DIGI-RAW-RECO', u'GEN-SIM-DIGI-RAW-HLTDEBUG-RECO', u'GEN-SIM-DIGI-RAW-HLTDEBUG', u'GEN-SIM-DIGI-RAW', u'GEN-SIM-DIGI-HLTDEBUG-RECO', u'GEN-SIM-DIGI-HLTDEBUG', u'GEN-SIM-DIGI', u'GEN-SIM', u'DIGI-RECO', u'FEVTHLTALL', u'DAVE', u'CRAP', u'GEN-RAWDEBUG', u'DQMROOT', u'GEN-RAW', u'DBS3_DEPLOYMENT_TEST_TIER']}
#print 'first site', input_values['site'][0]
#print input_values
pprint(map(lambda (k,v): (k, bool(v)), input_values.items()))

output_values = {
    # looking at the returned value makes sense only for status...
    'status.name',
    #'release.name', 'group.name', 'site.name'
    # TODO: site contains more: site.se, block/dataset/replica franction

}

"""
is XYZ valid?
what is XYZ status?
"""



def input_value_matches(keyword):
    # TODO: support wildcards for site
    # TODO: store everything at mongoDB
    scores_by_entity = {}
    for field, values in input_values.items():
        # full match
        full_maches = [v for v in values if keyword.lower() == v.lower()]

        if full_maches:
            scores_by_entity[field] = (full_maches[0] == keyword
                and 1.0 or 0.9, {'map_to': field, 'adjusted_keyword': full_maches[0]})


    return scores_by_entity