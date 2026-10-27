###  Author: Melissa F. Adasme ###

"""
The script uses the EPM literature annotations from Open Targets. Data is organised with each found entity reported in one row,
so the script first identifies articles mentioning a chemical probe and then subsets the data to obtain all rows (entities) associated with that article.
The input file can be obtained from the OT cloud (if you have access) or from the public FTP.
"""

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from tools import write_merged_files
import argparse
from pipeline_paths import output_path

def main(probes_path, matches_path, output, intermediate_output=None):
    """
    Identifies articles containing HQ probes and then extracts all NER sentences from them.
    This process is done in two stages, creating an intermediate file.
    """
    output = output_path(output)
    inter_output_dir = output_path(intermediate_output or (output + ".hq_mentions.tsv"))
    if inter_output_dir == output:
        raise ValueError("Intermediate and final output paths must differ.")
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

    # 1. Initialise Spark Session
    print("✅ Spark Session initialized.", flush=True)


    # 2. Define the list of High Quality probes
    hq_probes = (
        spark.read.csv(probes_path, sep="\t", header=True)
        .selectExpr("SYNOMS as probes")
        .select(f.explode(f.split(f.col("probes"), ", ")))
        .distinct()
        .withColumnRenamed("col", "label")
    )
    print("✅ Loaded HQ probe labels.", flush=True)


    # 3. Load OT NER dataset which includes only the matches (OT grounded entities)
    epmc_ner_results = (
        spark.read.parquet(matches_path)
        .selectExpr(
            "pmid", "label", "keywordId as label_id", "type as entity_type",
            "pubDate as date", "section as section", "text as sentence",
        )
        .distinct()
    )
    print("✅ Loaded OT NER results.",flush=True)


    # 4. Find articles with HQ probes and write the intermediate file containning all articles mentioning at least one chemical probe
    print(f"Finding articles with HQ probes and writing to intermediate file: {inter_output_dir}", flush=True)
    epmc_ner_results_subset = epmc_ner_results.join(f.broadcast(hq_probes), "label")

    #Writing output file
    temp_output_dir = output + ".parts"
    write_merged_files(epmc_ner_results_subset, temp_output_dir, inter_output_dir) #function from tools.py
    print("✅ Intermediate file created.", flush=True)


    # 5. Read intermediate file to get PMIDs and find all sentences
    # Articles of interest are identified from the intermediate file created above
    pmids_of_interest = (
        spark.read.csv(inter_output_dir, sep="\t", header=True)
        .selectExpr("pmid as pmid")
        .distinct()
    )
    print("✅ Read PMIDs of interest from intermediate file.", flush=True)


    # 6. Finds and subset all data from articles of interest mentioning a chemical probe
    final_results = epmc_ner_results.join(f.broadcast(pmids_of_interest), "pmid")

    #Writing output file
    temp_output_dir = output + ".parts"
    final_output_dir = output
    write_merged_files(final_results, temp_output_dir, final_output_dir) #function from tools.py
    print("✅ Analysis complete!", flush=True)

    #Close spark session
    spark.stop()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all NER sentences from articles containing HQ probes.')
    parser.add_argument('--probes', required=True, help='Path to the HQ probes tab-separated file.') #HQ probes dictionary file in ../probesHQ/files/probesSubset.csv
    parser.add_argument('--matches', required=True, help='Path to the OT NER "matches" parquet directory.') #Files from OT cloud or FTP
    parser.add_argument('--output', required=True, help='Destination TSV file; parent directories are created.')
    parser.add_argument('--intermediate-output', help='HQ mentions TSV; defaults to OUTPUT.hq_mentions.tsv.')
    args = parser.parse_args()

    main(args.probes, args.matches, args.output, args.intermediate_output)
