import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql import SparkSession
from tools import merge_spark_output_files 
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
    .getOrCreate()

def main(probes_path, matches_path, failed_matches_path):
    """
    Identifies articles containing HQ probes and extracts all NER sentences from them
    in a single, efficient Spark job.
    """
    # 1. Initialize Spark Session
    #spark = initialize_sparksession()
    
    spark = SparkSession.builder.appName("Chemical Probes Lit Job").getOrCreate()
    print("✅ Spark Session initialized.", flush=True)

    # 2. Prepare the list of High Quality (HQ) probe labels
    # This is a small DataFrame, perfect for broadcasting.
    hq_probes = (
        spark.read.csv(probes_path, sep="\t", header=True)
        .select(f.explode(f.split(f.col("SYNOMS"), ", ")).alias("label"))
        .distinct()
        .cache() # Cache this small, reused DataFrame for performance
    )
    print(f"✅ Loaded {hq_probes.count()} unique HQ probe labels.", flush=True)

    # Create a Python set from the probes for an efficient broadcast lookup
    hq_probes_list = [row.label for row in hq_probes.collect()]
    broadcasted_probes = spark.sparkContext.broadcast(set(hq_probes_list))

    # 3. Load and combine the full OT NER dataset from both sources
    # We read both 'matches' and 'failed_matches' and unify their schema.
    matches_df = spark.read.parquet(matches_path)
    failed_matches_df = spark.read.parquet(failed_matches_path)

    # Ensure 'failed_matches' has the 'keywordId' column before unioning
    if 'keywordId' not in failed_matches_df.columns:
        failed_matches_df = failed_matches_df.withColumn('keywordId', f.lit(None).cast('string'))

    # Select and rename columns to create a consistent structure
    common_columns = [
        f.col("pmid"),
        f.col("label"),
        f.col("keywordId").alias("label_id"),
        f.col("type").alias("entity_type"),
        f.col("pubDate").alias("date"),
        f.col("section"),
        f.col("text").alias("sentence")
    ]

    epmc_ner_results = (
        matches_df.select(*common_columns)
        .unionByName(failed_matches_df.select(*common_columns))
        .distinct() # Apply distinct once on the combined data
    )
    print("✅ Loaded and combined OT NER results from matches and failed_matches.", flush=True)

    # 4. Identify articles with HQ probes and filter in a single pass using a Window Function
    # This is the core optimization. We avoid joins and intermediate files.
    article_window = Window.partitionBy("pmid")

    # Define a UDF to check if a label is in our broadcasted set of probes
    is_probe_udf = f.udf(lambda label: label in broadcasted_probes.value, 'boolean')

    # --- START DEBUGGING ---
    # Create the dataframe right before the filter
    results_before_filtering = (
        epmc_ner_results
        .withColumn("is_probe", is_probe_udf(f.col("label")))
        .withColumn("has_hq_probe", f.max("is_probe").over(article_window))
    )
    count_before = results_before_filtering.count()
    print(f"DEBUG: Row count BEFORE filtering is: {count_before}", flush=True)

    final_results = (
        epmc_ner_results
        .withColumn("is_probe", is_probe_udf(f.col("label")))
        .withColumn("has_hq_probe", f.max("is_probe").over(article_window))
        .where(f.col("has_hq_probe") == True) # Keep only articles that were flagged
        .drop("is_probe", "has_hq_probe") # Drop the temporary helper columns
    )
    count_after = final_results.count()
    print(f"DEBUG: Row count AFTER filtering is: {count_after}", flush=True)
    # --- END DEBUGGING ---

    # 5. Write the final results directly to the hardcoded path
    temp_output_path = 'temp/'
    print(f"🚀 Identified articles and prepared final dataset. Writing to: {temp_output_path}", flush=True)

    final_results.write.csv(
        temp_output_path,
        sep='\t',
        header=True,
        mode='overwrite'
    )

    print("✅ Analysis complete!", flush=True)

    # Close the Spark session
    spark.stop()

    # 6. Merge the output files using in house method from tools.py
    print("🚀 Starting file merge process...")
    merge_spark_output_files(temp_output_path, 'data/1_epmc_ner_results_hq_probes_all_sent.tsv')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Efficiently find all NER sentences from articles containing HQ probes.')
    parser.add_argument('--probes', required=True, help='Path to the HQ probes CSV file.')
    parser.add_argument('--matches', required=True, help='Path to the OT NER "matches" parquet directory.')
    parser.add_argument('--failed_matches', required=True, help='Path to the OT NER "failedMatches" parquet directory.')
    
    args = parser.parse_args()
    
    main(args.probes, args.matches, args.failed_matches)