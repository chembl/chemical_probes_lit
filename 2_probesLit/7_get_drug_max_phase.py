from tools import write_merged_files
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, row_number, desc
from pyspark.sql.window import Window
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


def main(input_path, OTdrug_path):
    ## 1. Load NER probes with therapeutic areas
    probes_data = (
            spark.read.csv(input_path, sep="\t", header=True)
        )
    print("✅ Loaded NER probes with therapeutic areas from step 6.", flush=True)

    ## 2. Get OT drug data
    otdrugs = (
        spark.read.parquet(OTdrug_path)
        .selectExpr(
            'drugId', 
            'targetId',
            'diseaseId',
            'phase',
            'status'
        )
        .distinct()
    )
    print("✅ Loaded drug data from OT.", flush=True)

    ## 3. De-duplicate drug data to get only the max phase for each key
    # group by the join keys: diseaseId and targetId.
    window_spec = (
        Window.partitionBy("diseaseId", "targetId")
              .orderBy(col("phase").desc()) # Order by phase descending
    )
    # Apply the window function
    otdrugs_deduped = (
        otdrugs
        # Create a row number 'rn' for each group, starting at 1 for the max phase
        .withColumn("rn", row_number().over(window_spec))
        # Keep only the rows where the row number is 1 (i.e., the max phase)
        .where(col("rn") == 1)
        # Drop the temporary row number column
        .drop("rn")
    )

    ## 4. Map using disease ids and add new column into main dataframe
    probes_data_drug = probes_data.join(
        otdrugs_deduped,
        ["diseaseId", "targetId"], 
        "left"
    )

    ## 5. Write the final DataFrame as a .tsv
    temp_output_dir = "temp/disease_output_parts"
    final_output_dir = "data/7_ner_probes_triplets_ptpairs_dr.tsv"
    write_merged_files(probes_data_drug, temp_output_dir, final_output_dir) #function from tools.py
    print("✅ File with OT dated evidence linked to chemical probes data created", flush=True)

    # 6. Close current spark session
    spark.stop() 
    print("✅ Analysis complete!", flush=True)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.') 
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs and with dated evidence.')  # main dataset with OT evidence in data/5_ner_probes_triplets_ptpairs_evd_ta.tsv
    parser.add_argument('--otdrug', required=True, help='Path to OT drug data folder with all files.') # http://ftp.ebi.ac.uk/pub/databases/opentargets/platform/25.09/output/known_drug
    args = parser.parse_args()
    main(args.input, args.otdrug)