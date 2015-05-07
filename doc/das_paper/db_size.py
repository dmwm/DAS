#!/usr/bin/env python
from __future__ import print_function
import sys
import matplotlib.pyplot as plt

# DBS instance
dbs_info = [
('CMS_DBS_PROD_GLOBAL', 12.9034424, 22.1050415),
('CMS_DBS_CAF_ANALYSIS_01', 0.894, 1.961),
('CMS_DBS_PH_ANALYSIS_01', 0.601135254, 1.27581787),
('CMS_DBS_PH_ANALYSIS_02', 0.969543457, 2.47174072),
('CMS_DBS_PROD_TIER0', 2.01916504, 4.98065186),
('CMS_DBS_PROD_LOCAL_01', 2.72540283, 4.42938232),
('CMS_DBS_PROD_LOCAL_02', 1.2432251, 3.29675293),
('CMS_DBS_PROD_LOCAL_03', 1.20812988, 2.66101074 ), 
('CMS_DBS_PROD_LOCAL_04', 1.9777832, 4.06530762), 
('CMS_DBS_PROD_LOCAL_05', 0.003723145, 0.010925293 ), 
('CMS_DBS_PROD_LOCAL_06', 0.681152344, 1.80340576), 
('CMS_DBS_PROD_LOCAL_07', 2.19720459, 7.07037354), 
('CMS_DBS_PROD_LOCAL_08', 0.201416016, 0.397583008), 
('CMS_DBS_PROD_LOCAL_09', 0.399047852, .720947266),
]
dbs_tables = sum(i[1] for i in dbs_info)
dbs_index = sum(i[2] for i in dbs_info)
print("Total DBS size:", dbs_tables, dbs_index)
db_info = [
('dashboard', 40.82977294921875, 135.9593505859375),
('phedex', 5.51031494, 12.5614014),
('dbs', dbs_tables, dbs_index),
('sitedb', 0.0057373046875, 0.0035400390625),
('runsum', 0.920349121, 2.72839355), # need real numbers
]

db_names = [i[0].replace('CMS_DBS_','') for i in db_info]
x = [i for i in range(0, len(db_info))]

y_table=[i[1] for i in db_info]
y_index=[i[2] for i in db_info]

#dbs_tables=[i[1] for i in db_info if i[0].find('DBS') != -1]
#dbs_indexies=[i[2] for i in db_info if i[0].find('DBS') != -1]
#print "DBS tables size", sum(dbs_tables)
#print "DBS index size", sum(dbs_indexies)

# pie stuff
#phedex_tables=[i[1] for i in db_info if i[0].find('phedex') != -1]
#dashboard_tables=[i[1] for i in db_info if i[0].find('dashboard') != -1]

#fracs = [sum(dbs_tables), sum(phedex_tables), sum(dashboard_tables)]
#labels = ['dbs', 'phedex', 'dashboard']
#explode = [0 for i in range(0, len(fracs))]
#plt.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True)
#plt.show()

width = 0.35 # width of the bars
rect1 = plt.bar(x, y_table, width, color='r')
rect2 = plt.bar([i+width for i in x], y_index, width, color='y')
plt.ylabel('Size GBs')
#plt.yscale('log')
plt.xlabel('CMS data-service')
#plt.xticks([i+width for i in x], db_names, rotation=70 )
plt.xticks([i+width for i in x], db_names)
plt.legend( (rect1[0], rect2[0]), ('tables size', 'index size') )
plt.savefig('db_size.pdf', format='pdf', transparent=True)
#plt.show()
plt.close()
