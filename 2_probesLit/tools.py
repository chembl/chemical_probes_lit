from psutil import virtual_memory
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkConf
import socket
import logging
import sqlite3
from ftplib import FTP
import glob
import os
import subprocess
import shutil
import pandas as pd
import requests
import time

def define_logging():
    # create logger with 'spam_application'
    logger = logging.getLogger('LOG')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('spam.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s',
                                "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    return logger

#Increasing memory limit
def detect_spark_memory_limit():
    """Spark does not automatically use all available memory on a machine. When working on large datasets, this may
    cause Java heap space errors, even though there is plenty of RAM available. To fix this, we detect the total amount
    of physical memory and allow Spark to use (almost) all of it."""
    mem_gib = virtual_memory().total >> 30
    return int(mem_gib * 0.9)

def initialize_sparksession() -> SparkSession:
    """Initialize spark session."""

    # Dynamically get the hostname of the current node
    bind_address = 'codon-slurm-login-01.ebi.ac.uk'#socket.gethostname()
    spark_mem_limit = detect_spark_memory_limit()
    spark_conf = (
        SparkConf()
        .set("spark.driver.memory", f"{spark_mem_limit}g")
        .set("spark.executor.memory", f"{spark_mem_limit}g")
        .set("spark.driver.maxResultSize", "0")
        .set("spark.debug.maxToStringFields", "2000")
        .set("spark.sql.execution.arrow.maxRecordsPerBatch", "500000")
        .set("spark.ui.showConsoleProgress", "false")
    )
    return (
        SparkSession.builder.config(conf=spark_conf)
        #.master("local[*]")
        .getOrCreate()
    )

def write_merged_files(data, temp_output_dir, final_output_file):
    """
    Given a Spark dataframe, writes it to temp part-files (headerless)
    and then merges them into a single final file with one header.
    """
    try:
        if os.path.exists(temp_output_dir):
            print(f"Cleaning up old temp directory: {temp_output_dir}", flush=True)
            shutil.rmtree(temp_output_dir)

        # STEP 1: Write all sub-files *without* the header
        data.write.csv(
            temp_output_dir,
            sep='\t',
            header=False,
            mode='overwrite'
        )
        print(f"✅ Spark successfully wrote headerless parts to {temp_output_dir}", flush=True)

        # STEP 2: Manually write the header to the final file
        # Get column names from the dataframe and format as a TSV line
        header = "\t".join(data.columns) + "\n"
        
        with open(final_output_file, 'w') as f:
            f.write(header)
        print(f"✅ Successfully wrote header to {final_output_file}", flush=True)

        # STEP 3: *Append* the headerless part-files to the final file
        # Note the '>>' (append) instead of '>' (overwrite)
        merge_command = f"cat {temp_output_dir}/part-*.csv >> {final_output_file}"
        print(f"Running merge command: {merge_command}")
        subprocess.run(merge_command, shell=True, check=True)
        print(f"✅ Successfully merged data into {final_output_file}", flush=True)

    finally:
        # STEP 4: Clean up the temporary directory
        if os.path.exists(temp_output_dir):
            print(f"Cleaning up temporary directory: {temp_output_dir}", flush=True)
            shutil.rmtree(temp_output_dir)


def download_files_from_ftp(ftp_dir, dir):
    """From the EBI ftp fetch the parquet files and saves them into a local folder within the project"""
    
    # 2. Create the local directory if it doesn't exist
    os.makedirs(dir, exist_ok=True)

    # define FTP connection
    ftp_server = 'ftp.ebi.ac.uk'
    # Connect to the FTP server
    ftp = FTP(ftp_server)
    ftp.login("anonymous", "anonymous@domain.com")
    # Change to the remote directory
    ftp.cwd(ftp_dir)
    # Get a list of files in the remote directory
    file_list = ftp.nlst()
    
    # Download each file to the local directory
    for filename in file_list:
        
        # 3. Use os.path.join to correctly build the file path
        local_filepath = os.path.join(dir, filename) 
        
        with open(local_filepath, 'wb') as file:
            ftp.retrbinary('RETR ' + filename, file.write)
            
    # Close the FTP connection
    ftp.quit()

def find_preferred_ta(ta_string):
    """Selects the preferred Therapeutic Area (TA) ID from a '|' separated string based on a predefined priority: EFO_ > MONDO_ > Other."""
    # Handle missing data (NaN, None, etc.) or non-string types
    if pd.isna(ta_string) or not isinstance(ta_string, str):
        return pd.NA
    # Split the string into a list of individual IDs
    ids = ta_string.split('|')
    # Categorize the IDs to find the highest priority.
    efo_ids = []
    mondo_ids = []
    other_ids = []
    for id_val in ids:
        if id_val.startswith('OTAR_'):
            efo_ids.append(id_val)
        elif id_val.startswith('MONDO_'):
            mondo_ids.append(id_val)
        elif id_val.startswith('EFO_'):
            mondo_ids.append(id_val)
        elif id_val:  # Ensure it's not an empty string (e.g., from 'A||B')
            other_ids.append(id_val)
    # Return the first item from the highest-priority list that has items
    if efo_ids:
        return efo_ids[0]
    if mondo_ids:
        return mondo_ids[0]
    if other_ids:
        return other_ids[0]
    return pd.NA # Return NA if the original string was empty or contained no valid IDs


def classify_pubmed_articles(pmid_list):
    """
    Takes a list of PMIDs, queries Europe PMC, and classifies them
    as 'Review' or 'Primary Research/Other'.
    """
    base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    results_data = []
    
    # Remove duplicates and ensure IDs are strings
    pmids = list(set([str(p).strip() for p in pmid_list]))
    
    # Batch processing: Europe PMC handles about 20-50 complex OR queries well in one URL
    batch_size = 20
    
    print(f"Processing {len(pmids)} PMIDs...")

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        
        # Construct query: "ext_id:123 OR ext_id:456"
        # We limit source to MED (PubMed) to be specific
        query_parts = [f'ext_id:{pmid} SRC:MED' for pmid in batch]
        query = " OR ".join(query_parts)
        
        params = {
            'query': query,
            'format': 'json',
            'resultType': 'core',
            'pageSize': batch_size
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status() # Check for connection errors
            data = response.json()
            
            # Map the results
            if 'resultList' in data and 'result' in data['resultList']:
                articles = data['resultList']['result']
                
                # Create a lookup dict for this batch to handle missing IDs
                # (Europe PMC sometimes drops IDs if they don't exist)
                found_articles = {art.get('id'): art for art in articles}
                
                for pmid in batch:
                    if pmid in found_articles:
                        article = found_articles[pmid]
                        title = article.get('title', 'No Title Found')
                        
                        # EXTRACT PUBLICATION TYPES
                        # pubTypeList is usually a dict like {'pubType': ['Review', 'Journal Article']}
                        pub_types = article.get('pubTypeList', {}).get('pubType', [])
                        
                        # CLASSIFICATION LOGIC
                        # We explicitly look for "Review" or "Systematic Review"
                        is_review = any("Review" in pt for pt in pub_types)
                        
                        classification = "Review Article" if is_review else "Primary Research / Other"
                        
                        results_data.append({
                            'PMID': pmid,
                            'Classification': classification,
                            'Publication Types': ", ".join(pub_types),
                            'Title': title
                        })
                    else:
                        results_data.append({
                            'PMID': pmid,
                            'Classification': "ID Not Found",
                            'Publication Types': "N/A",
                            'Title': "N/A"
                        })
            
            # Be nice to the API
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"Error processing batch {i}: {e}")

    # Convert to DataFrame for easy viewing/saving
    df = pd.DataFrame(results_data)
    return df


class ChemblDB():
    def __init__(self,chemblpath):
        self.cursor = self.connect(chemblpath)

    def connect(self, chemblpath):
        """Establishes the connection to the database and returns the cursor to perform further queries"""
        connection = sqlite3.connect(chemblpath)
        cursor = connection.cursor()
        return cursor
    
    def get_inchikey(self, inchi):
        """Given an inchikey it obtains the chemblid"""
        query = """
                SELECT s.standard_inchi_key
                FROM molecule_dictionary d
                JOIN compound_structures s on s.molregno = d.molregno
                WHERE d.CHEMBL_ID  = '{}'
                """
        
        result = self.cursor.execute(query.format(inchi)).fetchone()
        return result
    
    def get_max_phase(self, chemblid):
        """Given an chembl it obtains the max phase in chembl"""
        query = """
                SELECT d.MAX_PHASE
                FROM molecule_dictionary d
                WHERE d.CHEMBL_ID  = '{}'
                """
        result = self.cursor.execute(query.format(chemblid)).fetchone()
        return result


class PnDDB():
    def __init__(self, path):
        self.cursor = self.connect(path)

    def connect(self,path):
        """Establishes the connection to the database and returns the cursor to perform further queries"""
        connection = sqlite3.connect(path)
        cursor = connection.cursor()
        return cursor
    
    def test(self):
        """Using the database cursor perfors the query to extract the golden set from Probes & Drugs and returns it as a pandas dataframe"""
        query = """
        SELECT * FROM sqlite_master WHERE type='table';
        """
        print(query)
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        return records