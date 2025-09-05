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

def write_merged_files(data, temp_output_dir,final_output_file):
    """ Given a dataframe it writes it doesn in multiple subprocess and subfiles into the temproal path and then it merges them into the output file"""
    try:
        # STEP 1: write all sub files in temporal path
        data.write.csv(
            temp_output_dir,
            sep='\t',
            header=True,
            mode='overwrite'
        )
        print(f"✅ Spark successfully wrote parts to {temp_output_dir}", flush=True)
    
        # STEP 2: Use Python's subprocess to call the shell 'cat' command. The 'shell=True' part is crucial for handling the '*' wildcard.
        merge_command = f"cat {temp_output_dir}/part-*.csv > {final_output_file}"
        print(f"Running merge command: {merge_command}")
        subprocess.run(merge_command, shell=True, check=True)
        print(f"✅ Successfully merged files into {final_output_file}", flush=True)
    
    finally:
        # STEP 3: Clean up the temporary directory.
        print(f"Cleaning up temporary directory: {temp_output_dir}", flush=True)
        #shutil.rmtree(temp_output_dir)



def download_files_from_ftp(ftp_dir, dir):
    """From the EBI ftp fetch the parquet files and saves them into a local folder within the project"""
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
        local_filepath = dir + filename
        with open(local_filepath, 'wb') as file:
            ftp.retrbinary('RETR ' + filename, file.write)
    # Close the FTP connection
    ftp.quit()


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