{ "_id" : { "$oid" : "530b2072d033b44e41857781" }, "keys" : [ "block", "run", "lumi", "dataset" ], "members" : [ "run.run_number", "block.name", "lumi.number", "qhash", "dataset.name" ], "system" : "dbs3", "urn" : "block_run_lumi4dataset" }
{ "_id" : { "$oid" : "530b2072d033b44e41857782" }, "member" : "run.run_number", "stems" : [ "run", "run_number" ] }
{ "_id" : { "$oid" : "530b2072d033b44e41857783" }, "member" : "block.name", "stems" : [ "block", "name" ] }
{ "_id" : { "$oid" : "530b2072d033b44e41857784" }, "member" : "lumi.number", "stems" : [ "lumi", "number" ] }
{ "_id" : { "$oid" : "530b2072d033b44e41857785" }, "member" : "qhash", "stems" : [ "qhash" ] }
{ "_id" : { "$oid" : "530b2072d033b44e41857786" }, "member" : "dataset.name", "stems" : [ "dataset", "name" ] }
{ "_id" : { "$oid" : "530b2074d033b44ead3e0c60" }, "keys" : [ "file", "run", "lumi", "dataset" ], "members" : [ "file.name", "lumi.number", "qhash", "dataset.name" ], "system" : "dbs3", "urn" : "file_lumi4dataset" }
{ "_id" : { "$oid" : "530b2074d033b44ead3e0c61" }, "member" : "file.name", "stems" : [ "file", "name" ] }
{ "_id" : { "$oid" : "530b2077d033b44f1adda203" }, "keys" : [ "file", "run", "lumi", "dataset" ], "members" : [ "run.run_number", "file.name", "lumi.number", "qhash", "dataset.name" ], "system" : "dbs3", "urn" : "file_run_lumi4dataset" }
{ "_id" : { "$oid" : "530b207ad033b44f86f57a4b" }, "keys" : [ "run", "lumi", "dataset" ], "members" : [ "run.run_number", "lumi.number", "qhash", "dataset.name" ], "system" : "dbs3", "urn" : "run_lumi4dataset" }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a16" }, "keys" : [ "run", "date" ], "members" : [ "run.gtkey", "run.modification_time", "run.creation_time", "run.lumi_section_ranges", "run.triggers", "run.start_time", "run.group_name", "run.nlumis", "run.hltkey", "run.stop_reason", "qhash", "run.beam_e", "run.lhcFill", "run.duration", "run.run_number", "run.l1key", "run.bfield", "run.end_time" ], "system" : "runregistry", "urn" : "rr_xmlrpc" }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a17" }, "member" : "run.gtkey", "stems" : [ "run", "gtkey" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a18" }, "member" : "run.modification_time", "stems" : [ "run", "modification_time" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a19" }, "member" : "run.creation_time", "stems" : [ "run", "creation_time" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a1a" }, "member" : "run.lumi_section_ranges", "stems" : [ "run", "lumi_section_ranges" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a1b" }, "member" : "run.triggers", "stems" : [ "run", "triggers" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a1c" }, "member" : "run.start_time", "stems" : [ "run", "start_time" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a1d" }, "member" : "run.group_name", "stems" : [ "run", "group_name" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a1e" }, "member" : "run.nlumis", "stems" : [ "run", "nlumis" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a1f" }, "member" : "run.hltkey", "stems" : [ "run", "hltkey" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a20" }, "member" : "run.stop_reason", "stems" : [ "run", "stop_reason" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a21" }, "member" : "run.beam_e", "stems" : [ "run", "beam_e" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a22" }, "member" : "run.lhcFill", "stems" : [ "run", "lhcfill" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a23" }, "member" : "run.duration", "stems" : [ "run", "duration" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a24" }, "member" : "run.l1key", "stems" : [ "run", "l1key" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a25" }, "member" : "run.bfield", "stems" : [ "run", "bfield" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a26" }, "member" : "run.end_time", "stems" : [ "run", "end_time" ] }
{ "_id" : { "$oid" : "530b207dd033b44ff4e69a27" }, "keys" : [ "run" ], "members" : [ "run.run_number", "qhash" ], "system" : "dbs3", "urn" : "runs" }
{ "_id" : { "$oid" : "530b207fd033b450610af800" }, "keys" : [ "run", "date" ], "members" : [ "run.run_number", "qhash", "run.delivered_lumi" ], "system" : "conddb", "urn" : "get_run_info" }
{ "_id" : { "$oid" : "530b207fd033b450610af801" }, "member" : "run.delivered_lumi", "stems" : [ "run", "delivered_lumi" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83eb9" }, "keys" : [ "dataset" ], "members" : [ "dataset.nevents", "dataset.nblocks", "qhash", "dataset.size", "dataset.nlumis", "dataset.nfiles", "dataset.name", "dataset.creation_time", "dataset.data_tier_name", "dataset.prep_id", "dataset.status", "dataset.dataset_id", "dataset.modified_by", "dataset.created_by", "dataset.modification_time", "dataset.datatype", "dataset.processing_version", "dataset.physics_group_name", "dataset.processed_ds_name", "dataset.primary_dataset.name", "dataset.xtcrosssection", "dataset.acquisition_era_name" ], "system" : "dbs3", "urn" : "filesummaries" }
{ "_id" : { "$oid" : "530b2082d033b450cff83eba" }, "member" : "dataset.nevents", "stems" : [ "dataset", "nevents" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ebb" }, "member" : "dataset.nblocks", "stems" : [ "dataset", "nblocks" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ebc" }, "member" : "dataset.size", "stems" : [ "dataset", "size" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ebd" }, "member" : "dataset.nlumis", "stems" : [ "dataset", "nlumis" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ebe" }, "member" : "dataset.nfiles", "stems" : [ "dataset", "nfiles" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ebf" }, "member" : "dataset.creation_time", "stems" : [ "dataset", "creation_time" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec0" }, "member" : "dataset.data_tier_name", "stems" : [ "dataset", "data_tier_name" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec1" }, "member" : "dataset.prep_id", "stems" : [ "dataset", "prep_id" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec2" }, "member" : "dataset.status", "stems" : [ "dataset", "status" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec3" }, "member" : "dataset.dataset_id", "stems" : [ "dataset", "dataset_id" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec4" }, "member" : "dataset.modified_by", "stems" : [ "dataset", "modified_by" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec5" }, "member" : "dataset.created_by", "stems" : [ "dataset", "created_by" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec6" }, "member" : "dataset.modification_time", "stems" : [ "dataset", "modification_time" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec7" }, "member" : "dataset.datatype", "stems" : [ "dataset", "datatype" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec8" }, "member" : "dataset.processing_version", "stems" : [ "dataset", "processing_version" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ec9" }, "member" : "dataset.physics_group_name", "stems" : [ "dataset", "physics_group_name" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83eca" }, "member" : "dataset.processed_ds_name", "stems" : [ "dataset", "processed_ds_name" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ecb" }, "member" : "dataset.primary_dataset.name", "stems" : [ "dataset", "primary_dataset", "name" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ecc" }, "member" : "dataset.xtcrosssection", "stems" : [ "dataset", "xtcrosssection" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ecd" }, "member" : "dataset.acquisition_era_name", "stems" : [ "dataset", "acquisition_era_name" ] }
{ "_id" : { "$oid" : "530b2082d033b450cff83ece" }, "keys" : [ "dataset" ], "members" : [ "dataset.nevents", "dataset.nblocks", "qhash", "dataset.size", "dataset.nlumis", "dataset.nfiles", "dataset.name", "dataset.creation_time", "dataset.data_tier_name", "dataset.prep_id", "dataset.status", "dataset.dataset_id", "dataset.modified_by", "dataset.created_by", "dataset.modification_time", "dataset.datatype", "dataset.processing_version", "dataset.physics_group_name", "dataset.processed_ds_name", "dataset.primary_dataset.name", "dataset.xtcrosssection", "dataset.acquisition_era_name" ], "system" : "dbs3", "urn" : "dataset_info" }
{ "_id" : { "$oid" : "530b2082d033b450cff83ecf" }, "keys" : [ "dataset", "block", "primary_dataset", "tier", "release", "run", "file", "era", "group", "status", "date", "user" ], "members" : [ "dataset.nevents", "dataset.nblocks", "qhash", "dataset.size", "dataset.nlumis", "dataset.nfiles", "dataset.name", "dataset.creation_time", "dataset.data_tier_name", "dataset.prep_id", "dataset.status", "dataset.dataset_id", "dataset.modified_by", "dataset.created_by", "dataset.modification_time", "dataset.datatype", "dataset.processing_version", "dataset.physics_group_name", "dataset.processed_ds_name", "dataset.primary_dataset.name", "dataset.xtcrosssection", "dataset.acquisition_era_name" ], "system" : "dbs3", "urn" : "datasets" }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d87" }, "keys" : [ "summary", "dataset", "run" ], "members" : [ "summary.nevents", "summary.nblocks", "qhash", "summary.file_size", "summary.nlumis", "dataset", "summary.nfiles", "run" ], "system" : "dbs3", "urn" : "summary4dataset_run" }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d88" }, "member" : "summary.nevents", "stems" : [ "summary", "nevents" ] }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d89" }, "member" : "summary.nblocks", "stems" : [ "summary", "nblocks" ] }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d8a" }, "member" : "summary.file_size", "stems" : [ "summary", "file_size" ] }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d8b" }, "member" : "summary.nlumis", "stems" : [ "summary", "nlumis" ] }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d8c" }, "member" : "dataset", "stems" : [ "dataset" ] }
{ "_id" : { "$oid" : "530b2085d033b4513c4f3d8d" }, "member" : "summary.nfiles", "stems" : [ "summary", "nfiles" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e5fa" }, "keys" : [ "block", "dataset", "run", "file" ], "members" : [ "block.nfiles", "block.name", "block.creation_time", "qhash", "block.block_id", "block.modification_time", "block.dataset_id", "block.modified_by", "block.origin_site_name", "block.open_for_writing", "block.size", "block.dataset", "block.created_by" ], "system" : "dbs3", "urn" : "blocks" }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e5fb" }, "member" : "block.nfiles", "stems" : [ "block", "nfiles" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e5fc" }, "member" : "block.creation_time", "stems" : [ "block", "creation_time" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e5fd" }, "member" : "block.block_id", "stems" : [ "block", "block_id" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e5fe" }, "member" : "block.modification_time", "stems" : [ "block", "modification_time" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e5ff" }, "member" : "block.dataset_id", "stems" : [ "block", "dataset_id" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e600" }, "member" : "block.modified_by", "stems" : [ "block", "modified_by" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e601" }, "member" : "block.origin_site_name", "stems" : [ "block", "origin_site_name" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e602" }, "member" : "block.open_for_writing", "stems" : [ "block", "open_for_writing" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e603" }, "member" : "block.size", "stems" : [ "block", "size" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e604" }, "member" : "block.dataset", "stems" : [ "block", "dataset" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e605" }, "member" : "block.created_by", "stems" : [ "block", "created_by" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e606" }, "keys" : [ "block", "dataset", "site", "site" ], "members" : [ "block.nfiles", "block.replica.subscribed", "block.replica.complete", "block.replica.node_id", "block.replica.group", "block.replica.nfiles", "block.size", "block.replica.site", "block.is_open", "block.name", "qhash", "block.replica.size", "block.id", "block.dataset", "block.replica.se", "block.replica.creation_time", "block.replica.modification_time", "block.replica.custodial" ], "system" : "phedex", "urn" : "blockReplicas" }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e607" }, "member" : "block.replica.subscribed", "stems" : [ "block", "replica", "subscribed" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e608" }, "member" : "block.replica.complete", "stems" : [ "block", "replica", "complete" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e609" }, "member" : "block.replica.node_id", "stems" : [ "block", "replica", "node_id" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e60a" }, "member" : "block.replica.group", "stems" : [ "block", "replica", "group" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e60b" }, "member" : "block.replica.nfiles", "stems" : [ "block", "replica", "nfiles" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e60c" }, "member" : "block.replica.site", "stems" : [ "block", "replica", "site" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e60d" }, "member" : "block.is_open", "stems" : [ "block", "is_open" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e60e" }, "member" : "block.replica.size", "stems" : [ "block", "replica", "size" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e60f" }, "member" : "block.id", "stems" : [ "block", "id" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e610" }, "member" : "block.replica.se", "stems" : [ "block", "replica", "se" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e611" }, "member" : "block.replica.creation_time", "stems" : [ "block", "replica", "creation_time" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e612" }, "member" : "block.replica.modification_time", "stems" : [ "block", "replica", "modification_time" ] }
{ "_id" : { "$oid" : "530b2088d033b451a9a5e613" }, "member" : "block.replica.custodial", "stems" : [ "block", "replica", "custodial" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7bf" }, "keys" : [ "file", "site", "site" ], "members" : [ "file.id", "file.replica.node_id", "qhash", "file.replica.creation_time", "file.name", "file.replica.subscribed", "file.replica.custodial", "file.replica.group", "file.replica.site", "file.adler32", "file.checksum", "file.size", "file.creation_time", "file.replica.se", "file.original_node" ], "system" : "phedex", "urn" : "fileReplicas4file" }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c0" }, "member" : "file.id", "stems" : [ "file", "id" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c1" }, "member" : "file.replica.node_id", "stems" : [ "file", "replica", "node_id" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c2" }, "member" : "file.replica.creation_time", "stems" : [ "file", "replica", "creation_time" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c3" }, "member" : "file.replica.subscribed", "stems" : [ "file", "replica", "subscribed" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c4" }, "member" : "file.replica.custodial", "stems" : [ "file", "replica", "custodial" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c5" }, "member" : "file.replica.group", "stems" : [ "file", "replica", "group" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c6" }, "member" : "file.replica.site", "stems" : [ "file", "replica", "site" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c7" }, "member" : "file.adler32", "stems" : [ "file", "adler32" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c8" }, "member" : "file.checksum", "stems" : [ "file", "checksum" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7c9" }, "member" : "file.size", "stems" : [ "file", "size" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7ca" }, "member" : "file.creation_time", "stems" : [ "file", "creation_time" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7cb" }, "member" : "file.replica.se", "stems" : [ "file", "replica", "se" ] }
{ "_id" : { "$oid" : "530b208dd033b4528109b7cc" }, "member" : "file.original_node", "stems" : [ "file", "original_node" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2af" }, "keys" : [ "site" ], "members" : [ "site.node.noncust_node_bytes", "site.node.cust_dest_bytes", "site.node.cust_dest_files", "site.node.cust_node_files", "site.node.cust_node_bytes", "site.node.nonsrc_node_files", "site.node.noncust_dest_bytes", "site.name", "proximity.site.name", "qhash", "site.node.name", "site.node.src_node_bytes", "site.node.noncust_node_files", "site.node.src_node_files", "site.node.noncust_dest_files", "site.node.nonsrc_node_bytes", "site.technology", "site.id", "site.se", "site.kind" ], "system" : "phedex", "urn" : "tfc" }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b0" }, "member" : "site.node.noncust_node_bytes", "stems" : [ "site", "node", "noncust_node_bytes" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b1" }, "member" : "site.node.cust_dest_bytes", "stems" : [ "site", "node", "cust_dest_bytes" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b2" }, "member" : "site.node.cust_dest_files", "stems" : [ "site", "node", "cust_dest_files" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b3" }, "member" : "site.node.cust_node_files", "stems" : [ "site", "node", "cust_node_files" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b4" }, "member" : "site.node.cust_node_bytes", "stems" : [ "site", "node", "cust_node_bytes" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b5" }, "member" : "site.node.nonsrc_node_files", "stems" : [ "site", "node", "nonsrc_node_files" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b6" }, "member" : "site.node.noncust_dest_bytes", "stems" : [ "site", "node", "noncust_dest_bytes" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b7" }, "member" : "site.name", "stems" : [ "site", "name" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b8" }, "member" : "proximity.site.name", "stems" : [ "proximity", "site", "name" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2b9" }, "member" : "site.node.name", "stems" : [ "site", "node", "name" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2ba" }, "member" : "site.node.src_node_bytes", "stems" : [ "site", "node", "src_node_bytes" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2bb" }, "member" : "site.node.noncust_node_files", "stems" : [ "site", "node", "noncust_node_files" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2bc" }, "member" : "site.node.src_node_files", "stems" : [ "site", "node", "src_node_files" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2bd" }, "member" : "site.node.noncust_dest_files", "stems" : [ "site", "node", "noncust_dest_files" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2be" }, "member" : "site.node.nonsrc_node_bytes", "stems" : [ "site", "node", "nonsrc_node_bytes" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2bf" }, "member" : "site.technology", "stems" : [ "site", "technology" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c0" }, "member" : "site.id", "stems" : [ "site", "id" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c1" }, "member" : "site.se", "stems" : [ "site", "se" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c2" }, "member" : "site.kind", "stems" : [ "site", "kind" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c3" }, "keys" : [ "site", "site" ], "members" : [ "site.resources.is_primary", "site.info.manual_install", "site.resources.type", "site.info.tier", "site.info.country", "site.site_name", "site.resources.site_name", "site.resources.fqdn", "site.info.usage", "site.admin.email", "site.type", "site.name", "site.info.logo_url", "site.admin.surname", "qhash", "site.info.url", "site.info.tier_level", "site.info.id", "site.admin.phone1", "site.info.site_name", "site.admin.phone2", "site.info.devel_release", "site.admin.username", "site.admin.im_handle", "site.admin.forename", "site.admin.dn" ], "system" : "sitedb2", "urn" : "site_names" }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c4" }, "member" : "site.resources.is_primary", "stems" : [ "site", "resources", "is_primary" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c5" }, "member" : "site.info.manual_install", "stems" : [ "site", "info", "manual_install" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c6" }, "member" : "site.resources.type", "stems" : [ "site", "resources", "type" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c7" }, "member" : "site.info.tier", "stems" : [ "site", "info", "tier" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c8" }, "member" : "site.info.country", "stems" : [ "site", "info", "country" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2c9" }, "member" : "site.site_name", "stems" : [ "site", "site_name" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2ca" }, "member" : "site.resources.site_name", "stems" : [ "site", "resources", "site_name" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2cb" }, "member" : "site.resources.fqdn", "stems" : [ "site", "resources", "fqdn" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2cc" }, "member" : "site.info.usage", "stems" : [ "site", "info", "usage" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2cd" }, "member" : "site.admin.email", "stems" : [ "site", "admin", "email" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2ce" }, "member" : "site.type", "stems" : [ "site", "type" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2cf" }, "member" : "site.info.logo_url", "stems" : [ "site", "info", "logo_url" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d0" }, "member" : "site.admin.surname", "stems" : [ "site", "admin", "surname" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d1" }, "member" : "site.info.url", "stems" : [ "site", "info", "url" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d2" }, "member" : "site.info.tier_level", "stems" : [ "site", "info", "tier_level" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d3" }, "member" : "site.info.id", "stems" : [ "site", "info", "id" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d4" }, "member" : "site.admin.phone1", "stems" : [ "site", "admin", "phone1" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d5" }, "member" : "site.info.site_name", "stems" : [ "site", "info", "site_name" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d6" }, "member" : "site.admin.phone2", "stems" : [ "site", "admin", "phone2" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d7" }, "member" : "site.info.devel_release", "stems" : [ "site", "info", "devel_release" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d8" }, "member" : "site.admin.username", "stems" : [ "site", "admin", "username" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2d9" }, "member" : "site.admin.im_handle", "stems" : [ "site", "admin", "im_handle" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2da" }, "member" : "site.admin.forename", "stems" : [ "site", "admin", "forename" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2db" }, "member" : "site.admin.dn", "stems" : [ "site", "admin", "dn" ] }
{ "_id" : { "$oid" : "530b2094d033b452f454b2dc" }, "keys" : [ "site" ], "members" : [ "site.node.noncust_node_bytes", "site.node.cust_dest_bytes", "site.node.cust_dest_files", "site.node.cust_node_files", "site.node.cust_node_bytes", "site.node.nonsrc_node_files", "site.node.noncust_dest_bytes", "site.name", "proximity.site.name", "qhash", "site.node.name", "site.node.src_node_bytes", "site.node.noncust_node_files", "site.node.src_node_files", "site.node.noncust_dest_files", "site.node.nonsrc_node_bytes", "site.technology", "site.id", "site.se", "site.kind" ], "system" : "phedex", "urn" : "nodeusage" }
{ "_id" : { "$oid" : "530b2095d033b452f454b2dd" }, "keys" : [ "site" ], "members" : [ "site.node.noncust_node_bytes", "site.node.cust_dest_bytes", "site.node.cust_dest_files", "site.node.cust_node_files", "site.node.cust_node_bytes", "site.node.nonsrc_node_files", "site.node.noncust_dest_bytes", "site.name", "proximity.site.name", "qhash", "site.node.name", "site.node.src_node_bytes", "site.node.noncust_node_files", "site.node.src_node_files", "site.node.noncust_dest_files", "site.node.nonsrc_node_bytes", "site.technology", "site.id", "site.se", "site.kind" ], "system" : "phedex", "urn" : "nodes" }
{ "_id" : { "$oid" : "530b2097d033b45362fd7a7d" }, "keys" : [ "release", "dataset", "file" ], "members" : [ "release.name", "qhash", "proximity.release.name" ], "system" : "dbs3", "urn" : "releaseversions" }
{ "_id" : { "$oid" : "530b2097d033b45362fd7a7e" }, "member" : "release.name", "stems" : [ "release", "name" ] }
{ "_id" : { "$oid" : "530b2097d033b45362fd7a7f" }, "member" : "proximity.release.name", "stems" : [ "proximity", "release", "name" ] }
{ "_id" : { "$oid" : "530b209ad033b453d0857a3d" }, "keys" : [ "group" ], "members" : [ "proximity.group.name", "group.username", "qhash", "group.name", "group.role" ], "system" : "sitedb2", "urn" : "groups" }
{ "_id" : { "$oid" : "530b209ad033b453d0857a3e" }, "member" : "proximity.group.name", "stems" : [ "proximity", "group", "name" ] }
{ "_id" : { "$oid" : "530b209ad033b453d0857a3f" }, "member" : "group.username", "stems" : [ "group", "username" ] }
{ "_id" : { "$oid" : "530b209ad033b453d0857a40" }, "member" : "group.name", "stems" : [ "group", "name" ] }
{ "_id" : { "$oid" : "530b209ad033b453d0857a41" }, "member" : "group.role", "stems" : [ "group", "role" ] }
{ "_id" : { "$oid" : "530b209ad033b453d0857a42" }, "keys" : [ "group" ], "members" : [ "proximity.group.name", "group.username", "qhash", "group.name", "group.role" ], "system" : "sitedb2", "urn" : "group_responsibilities" }
{ "_id" : { "$oid" : "530b209ad033b453d0857a43" }, "keys" : [ "group" ], "members" : [ "group.physics_group_name", "qhash", "group.name" ], "system" : "dbs3", "urn" : "physicsgroup" }
{ "_id" : { "$oid" : "530b209ad033b453d0857a44" }, "member" : "group.physics_group_name", "stems" : [ "group", "physics_group_name" ] }
{ "_id" : { "$oid" : "530b209ed033b4543db96085" }, "keys" : [ "site", "dataset" ], "members" : [ "site.replica_fraction", "qhash", "site.name", "site.se", "dataset.name" ], "system" : "phedex", "urn" : "site4dataset" }
{ "_id" : { "$oid" : "530b209ed033b4543db96086" }, "member" : "site.replica_fraction", "stems" : [ "site", "replica_fraction" ] }
{ "_id" : { "$oid" : "530b209ed033b4543db96087" }, "keys" : [ "site", "dataset" ], "members" : [ "site.block_completion", "site.dataset_fraction", "qhash", "site.block_fraction", "site.name", "dataset.name" ], "system" : "combined", "urn" : "site4dataset" }
{ "_id" : { "$oid" : "530b209ed033b4543db96088" }, "member" : "site.block_completion", "stems" : [ "site", "block_completion" ] }
{ "_id" : { "$oid" : "530b209ed033b4543db96089" }, "member" : "site.dataset_fraction", "stems" : [ "site", "dataset_fraction" ] }
{ "_id" : { "$oid" : "530b209ed033b4543db9608a" }, "member" : "site.block_fraction", "stems" : [ "site", "block_fraction" ] }
{ "_id" : { "$oid" : "530b20a1d033b454a97ca155" }, "keys" : [ "lumi", "run", "dataset" ], "members" : [ "run.run_number", "lumi.max_lumi", "qhash" ], "system" : "dbs3", "urn" : "runsummaries" }
{ "_id" : { "$oid" : "530b20a1d033b454a97ca156" }, "member" : "lumi.max_lumi", "stems" : [ "lumi", "max_lumi" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8959" }, "keys" : [ "jobsummary", "site", "site", "user", "date", "release" ], "members" : [ "jobsummary.activity", "jobsummary.submissiontool", "jobsummary.rb", "jobsummary.terminated", "jobsummary.date1", "jobsummary.date2", "jobsummary.grid", "jobsummary.cancelled", "jobsummary.user", "jobsummary.site-calc-failed", "qhash", "jobsummary.ce", "jobsummary.app-unknown", "jobsummary.site-failed", "jobsummary.user-failed", "jobsummary.running", "jobsummary.cpu", "jobsummary.wc", "jobsummary.grid-unknown", "jobsummary.check", "jobsummary.jobtype", "jobsummary.app-succeeded", "jobsummary.application-failed", "jobsummary.tier", "jobsummary.pending", "date", "jobsummary.site", "jobsummary.app-failed", "jobsummary.events", "jobsummary.dataset", "jobsummary.allunk", "jobsummary.unsuccess", "jobsummary.application", "jobsummary.submitted", "jobsummary.unk-failed", "jobsummary.unknown", "jobsummary.aborted", "jobsummary.name", "jobsummary.done", "jobsummary.applic-failed" ], "system" : "dashboard", "urn" : "jobsummary-plot-or-table" }
{ "_id" : { "$oid" : "530b20a7d033b45584ea895a" }, "member" : "jobsummary.activity", "stems" : [ "jobsummary", "activity" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea895b" }, "member" : "jobsummary.submissiontool", "stems" : [ "jobsummary", "submissiontool" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea895c" }, "member" : "jobsummary.rb", "stems" : [ "jobsummary", "rb" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea895d" }, "member" : "jobsummary.terminated", "stems" : [ "jobsummary", "terminated" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea895e" }, "member" : "jobsummary.date1", "stems" : [ "jobsummary", "date1" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea895f" }, "member" : "jobsummary.date2", "stems" : [ "jobsummary", "date2" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8960" }, "member" : "jobsummary.grid", "stems" : [ "jobsummary", "grid" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8961" }, "member" : "jobsummary.cancelled", "stems" : [ "jobsummary", "cancelled" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8962" }, "member" : "jobsummary.user", "stems" : [ "jobsummary", "user" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8963" }, "member" : "jobsummary.site-calc-failed", "stems" : [ "jobsummary", "site-calc-failed" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8964" }, "member" : "jobsummary.ce", "stems" : [ "jobsummary", "ce" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8965" }, "member" : "jobsummary.app-unknown", "stems" : [ "jobsummary", "app-unknown" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8966" }, "member" : "jobsummary.site-failed", "stems" : [ "jobsummary", "site-failed" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8967" }, "member" : "jobsummary.user-failed", "stems" : [ "jobsummary", "user-failed" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8968" }, "member" : "jobsummary.running", "stems" : [ "jobsummary", "running" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8969" }, "member" : "jobsummary.cpu", "stems" : [ "jobsummary", "cpu" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea896a" }, "member" : "jobsummary.wc", "stems" : [ "jobsummary", "wc" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea896b" }, "member" : "jobsummary.grid-unknown", "stems" : [ "jobsummary", "grid-unknown" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea896c" }, "member" : "jobsummary.check", "stems" : [ "jobsummary", "check" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea896d" }, "member" : "jobsummary.jobtype", "stems" : [ "jobsummary", "jobtype" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea896e" }, "member" : "jobsummary.app-succeeded", "stems" : [ "jobsummary", "app-succeeded" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea896f" }, "member" : "jobsummary.application-failed", "stems" : [ "jobsummary", "application-failed" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8970" }, "member" : "jobsummary.tier", "stems" : [ "jobsummary", "tier" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8971" }, "member" : "jobsummary.pending", "stems" : [ "jobsummary", "pending" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8972" }, "member" : "date", "stems" : [ "date" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8973" }, "member" : "jobsummary.site", "stems" : [ "jobsummary", "site" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8974" }, "member" : "jobsummary.app-failed", "stems" : [ "jobsummary", "app-failed" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8975" }, "member" : "jobsummary.events", "stems" : [ "jobsummary", "events" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8976" }, "member" : "jobsummary.dataset", "stems" : [ "jobsummary", "dataset" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8977" }, "member" : "jobsummary.allunk", "stems" : [ "jobsummary", "allunk" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8978" }, "member" : "jobsummary.unsuccess", "stems" : [ "jobsummary", "unsuccess" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8979" }, "member" : "jobsummary.application", "stems" : [ "jobsummary", "application" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea897a" }, "member" : "jobsummary.submitted", "stems" : [ "jobsummary", "submitted" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea897b" }, "member" : "jobsummary.unk-failed", "stems" : [ "jobsummary", "unk-failed" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea897c" }, "member" : "jobsummary.unknown", "stems" : [ "jobsummary", "unknown" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea897d" }, "member" : "jobsummary.aborted", "stems" : [ "jobsummary", "aborted" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea897e" }, "member" : "jobsummary.name", "stems" : [ "jobsummary", "name" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea897f" }, "member" : "jobsummary.done", "stems" : [ "jobsummary", "done" ] }
{ "_id" : { "$oid" : "530b20a7d033b45584ea8980" }, "member" : "jobsummary.applic-failed", "stems" : [ "jobsummary", "applic-failed" ] }
{ "_id" : { "$oid" : "530b20b3d033b456cddd0a5a" }, "member" : "run", "stems" : [ "run" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec87" }, "keys" : [ "user" ], "members" : [ "qhash", "user.im_handle", "user.username", "user.phone2", "user.email", "user.forename", "user.dn", "user.name", "user.surname", "user.phone1" ], "system" : "sitedb2", "urn" : "people_via_name" }
{ "_id" : { "$oid" : "530b20bed033b457a467ec88" }, "member" : "user.im_handle", "stems" : [ "user", "im_handle" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec89" }, "member" : "user.username", "stems" : [ "user", "username" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec8a" }, "member" : "user.phone2", "stems" : [ "user", "phone2" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec8b" }, "member" : "user.email", "stems" : [ "user", "email" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec8c" }, "member" : "user.forename", "stems" : [ "user", "forename" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec8d" }, "member" : "user.dn", "stems" : [ "user", "dn" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec8e" }, "member" : "user.name", "stems" : [ "user", "name" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec8f" }, "member" : "user.surname", "stems" : [ "user", "surname" ] }
{ "_id" : { "$oid" : "530b20bed033b457a467ec90" }, "member" : "user.phone1", "stems" : [ "user", "phone1" ] }
{ "_id" : { "$oid" : "530b20c1d033b4580fe3d879" }, "keys" : [ "run", "dataset" ], "members" : [ "run.run_number", "qhash", "dataset.name" ], "system" : "dbs3", "urn" : "runs_via_dataset" }
