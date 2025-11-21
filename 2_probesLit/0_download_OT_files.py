from tools import download_files_from_ftp

#Download known drugs
download_files_from_ftp('/pub/databases/opentargets/platform/25.09/output/known_drug', 'data/known_drug')