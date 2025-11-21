from tools import write_merged_files
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
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


def main(input_path, OTdisease_path):
    ## 1. Load NER probes with dated evidence dataset
    probes_data = (
            spark.read.csv(input_path, sep="\t", header=True)
        )
    print("✅ Loaded NER probes with dated evidence from step 5.", flush=True)

    ## 2. Get OT disease data
    otdisese = (
        spark.read.parquet(OTdisease_path)
        .selectExpr(
            'id', 
            'name',
            'therapeuticAreas'
        )
        .distinct()
    )
    print("✅ Loaded disease data from OT.", flush=True)

    ## 3. Map using disease ids and add new column into main dataframe
    probes_data_with_ta = probes_data.join(
        otdisese,
        probes_data.diseaseId == otdisese.id,  # The mapping condition
        "left"
    )

    ## 4. Clean up columns
    probes_data_final = (
        probes_data_with_ta
        .withColumn(
            "therapeuticAreas", 
            concat_ws("|", "therapeuticAreas")
        )
        .drop(otdisese.id) 
    )
    print("✅ Joined probes data and flattened therapeutic areas.", flush=True)
    
    # Optional: Show the schema and a few rows to verify
    probes_data_final.printSchema()
    probes_data_final.show(5)


    ## 5. Write the final DataFrame as a .tsv
    temp_output_dir = "temp/disease_output_parts"
    final_output_dir = "data/6_ner_probes_triplets_ptpairs_evd_ta.tsv"
    write_merged_files(probes_data_final, temp_output_dir, final_output_dir) #function from tools.py
    print("✅ File with OT dated evidence linked to chemical probes data created", flush=True)

    # 6. Close current spark session
    spark.stop() 
    print("✅ Analysis complete!", flush=True)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.') 
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs and with dated evidence.')  # main dataset with OT evidence in data/5_ner_probes_triplets_ptpairs_evd.tsv
    parser.add_argument('--otdisease', required=True, help='Path to OT disease/phenotype data folder with all files.') # http://ftp.ebi.ac.uk/pub/databases/opentargets/platform/25.09/output/disease
    args = parser.parse_args()
    main(args.input, args.otdisease)