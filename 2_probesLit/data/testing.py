###  Author: Melissa F. Adasme ###

"""
The script uses the EPM literature annotations from Open Targets. Data is organised with each found entity reported in one row, 
so the script first identifies articles mentioning a chemical probe and then subsets the data to obtain all rows (entities) associated with that article.
The input file can be obtained from the OT cloud (if you have access) or from the public FTP.
"""
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import argparse

# Define memory settings
# Choose a value less than the --mem you request in SLURM (e.g., 120g for a 150g node)
driver_mem = "200g"
executor_mem = "200g"

spark = SparkSession.builder \
    .appName("Chemical Probes Lit Job") \
    .config("spark.driver.memory", driver_mem) \
    .config("spark.executor.memory", executor_mem) \
    .config("spark.driver.memoryOverhead", "10g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

def main(matches_path): 
    # 1. Initialise Spark Session 
    spark = SparkSession.builder.appName("Chemical Probes Lit Job").getOrCreate()
    print("✅ Spark Session initialized.", flush=True)
    
    # 3. Load OT NER dataset which includes only the matches (OT grounded entities) 
    # If you change your code to this:
    epmc_ner_results = (
        spark.read.parquet(matches_path)
        .select("pmid")  # <--- ONLY selecting pmid
        .distinct()
    )
    
    # Then this IS the unique pmid count
    count = epmc_ner_results.count()
    print(count)
    
    #Close spark session
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all NER sentences from articles containing HQ probes.') 
    parser.add_argument('--input', required=True, help='Path to the HQ probes CSV file.')
    args = parser.parse_args()
    
    main(args.input)