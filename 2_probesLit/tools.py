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