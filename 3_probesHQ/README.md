# Generates dataset of high quality probes


### Dataset construction

#### The script uses the Probes&Drugs database to extract the main info about the three selected datasets. Probes&Drugs collects and updates data from those sources regularly and defines three main setids for them.

* SGC probes [setid 28] (last update 04/2022)  
* Open Science probes [setid 213] (last update 04/2023)  
* ChemicalProbes.org (last update 04/2023) [setid 408] (last update 04/2023)  

From Probes&Drugs the script extracts:  
* SET ID
* UNIPROTID
* PROBE
* SET NAME
* INCHIKEY
* GENE
* CONTROL
* PROBE SYNONYMS
* TARGET SYNONYMS

#### The script also extract data from ChEMBL database, mostly about synonyms and names for the entities.

From ChEMBL database the script extracts:  
* PREFNAME
* PROBE SYNONYMS
* TARGET SYNONYMS


### Data filtering
As we do not trust the rankings from Probes&Drugs, we only get the general info for the probes-associations, inchikeys mapping, and synonyms from them and any other filter can be later applied.  

Therefore, the mainset class extracts all data available for the 3 main sources without applying an initial filter.  

*** Also to keep it more open for OpenTargets requirements, as they do not plan to filter data.  

On the other hand, for ChEMBL we want to filter the data by "good quality" ChemicalProbes.org meaning:  

* more than 3 stars in cell
* More than 3 stars in vivo


The filtering is done defining the inchikeys of "good quality" and applying inchikeys-based filter to the main set. For that I manually used the filter section and got the data from ChemicalProbes .org site, then get the inchikeys meeting the thresholds and use them to filter the mainset.  

If thresholds change in the future, it can be easily modified without interfeering the Probes&Drugs database queries.  


### Inputs

#### Databases
Both databases Probes&Drugs and ChEMBL are loaded from the SQLite dumps files and processed in tools.py. The dumps can be obtained from:

* **ChEMBL SQLite dump** https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_33_sqlite.tar.gz
* **Probes&Drugs SQLite dump** https://www.probes-drugs.org/media/download/dump/pd_01_2023_sqlite.zip

After download and uncompressed, the path to the dumps has to be modified accordingly in the main notebook. 

**Using dumps so the scripts can be run by anyone even outside chembl.

#### Filtering file
It requires the file from ChemicalProbes.org after filter apllied to extract the "good quality" probes inchikeys. It can be downloaded from he portal after using the filtering tool.

### Outputs
A csv file with the main dataset without filtering and a subset after filtering.
