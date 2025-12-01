from tools import download_files_from_ftp

#Download the OT annotated literature
download_files_from_ftp('pub/databases/opentargets/platform/latest/intermediate/literature_match/', 'data/literature_match')

#Download the OT evidence data
download_files_from_ftp('pub/databases/opentargets/platform/25.09/output/association_by_datasource_indirect', 'data/association_by_datasource_indirect')

#Download OT therapeutic area from diseases
download_files_from_ftp('pub/databases/opentargets/platform/25.09/output/disease', 'data/disease')

#Download known drugs
download_files_from_ftp('/pub/databases/opentargets/platform/25.09/output/known_drug', 'data/known_drug')