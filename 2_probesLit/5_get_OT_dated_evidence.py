from tools import write_merged_files
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, when, col, size
import pyspark.sql.functions as f
import argparse
import os
from functools import reduce

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


def main(input_path, OTdatedevidence_path):
    ## 1. Initialise Spark Session 
    spark = SparkSession.builder.appName("Chemical Probes Lit Job").getOrCreate()
    print("✅ Spark Session initialized.", flush=True)

    ## 2. Load NER probes with evidence dataset
    probes_data = (
            spark.read.csv(input_path, sep="\t", header=True)
        )
    print("✅ Loaded NER probes with evidence from step 4.", flush=True)

    ## 3. Get a list of all subfolders
    data_folder = OTdatedevidence_path # path of the main folder containing al subfolders for each data type
    subfolders = [f.path for f in os.scandir(data_folder) if f.is_dir()]
    print("✅ Subfolders retrieved.", flush=True)


    ## 4. obtained evidence with date for all subfolders
    # Create an empty list to store the results
    probes_dataed_list = []

    # Loop through each subfolder
    probes_dataed_list = []
    for subfolder in subfolders:
        # Extract the sourceId from the subfolder name
        sourceId = subfolder.split("=")[-1]

        ### OT evidence with dates ###
        otevidenced = (
            spark.read.parquet(subfolder)
            .selectExpr(
                'diseaseId', 
                'targetId', 
                'datasourceId',
                'curationYear',
                'publicationYear',
                'studyYear'
            )
            .distinct()
        )

        # Append the result to the list
        probes_dataed_list.append(otevidenced)

    # Combine all dataframes in the list
    probes_dataev = reduce(DataFrame.unionAll, probes_dataed_list) 
    print("✅ All evidence combined.", flush=True)

    ## 5. Link probes taregt-disease pairs with dated evidence. Perform the join on both diseaseId and targetId
    #probes_dataed = probes_dataev.join(probes_data, ["diseaseId", "targetId", "datasourceId"], "right_outer")

    probes_dataed = (
        probes_dataev
        .groupBy("diseaseId", "targetId", "datasourceId")
        .agg(
            collect_list("curationYear").alias("allcurationYears"),  # Collect all curationYears (optional)
            collect_list("publicationYear").alias("allpublicationYears"),  # Collect all publicationYears (optional)
            collect_list("studyYear").alias("allstudyYears")  # Collect all studyYears (optional)
        )
        .join(probes_data, ["diseaseId", "targetId", "datasourceId"], "right_outer")
        .withColumn("allcurationYears", when(size(col("allcurationYears")) == 0, None).otherwise(col("allcurationYears")))
        .withColumn("allpublicationYears", when(size(col("allpublicationYears")) == 0, None).otherwise(col("allpublicationYears")))
        .withColumn("allstudyYears", when(size(col("allstudyYears")) == 0, None).otherwise(col("allstudyYears")))
    )
    print("✅ All dated evidence linked.", flush=True)

    ## 6. Create the file with the results
    probes_dataed.toPandas().to_csv("data/ner_probes_triplets_ptpairs_evd.csv", sep="\t", index=False)
    ## 5. Writing output file
    temp_output_dir = "temp/evidence_output_parts"
    final_output_dir = "data/5_ner_probes_triplets_ptpairs_evd.tsv"
    write_merged_files(probes_dataed, temp_output_dir, final_output_dir) #function from tools.py
    print("✅ File with OT dated evidence linked to chemical probes data created", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.') 
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs and with evidence.')  # main dataset with OT evidence in data/ner_probes_triplets_ptpairs_ev.csv
    parser.add_argument('--datedevidence', required=True, help='Path to OT dated evidence folder with all subfolder inside for each data type.') # 
    args = parser.parse_args()
    main(args.input, args.datedevidence)