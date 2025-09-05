from tools import download_files_from_ftp
import pyarrow.parquet as pq
from tools import initialize_sparksession
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, when, col, size
import pyspark.sql.functions as f
import argparse
import os
from functools import reduce


def main(input_path, OTdatedevidence_path):
    # Sparl session initialized with increased  Memory
    spark = initialize_sparksession()

    ### PROBES dataset ###
    probes_data = (
            spark.read.csv(input_path, sep="\t", header=True)
        )

    # Get a list of all subfolders
    data_folder = OTdatedevidence_path # path of the main folder containing al subfolders for each data type
    subfolders = [f.path for f in os.scandir(data_folder) if f.is_dir()]

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

    # Perform the join on both diseaseId and targetId
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

    # Add the probes_data with NULL datasourceId to the final result
    probes_dataed.show(5)

    # Save the results
    #probes_dataed.toPandas().to_csv("data/ner_probes_triplets_ptpairs_evd.csv", sep="\t", index=False)
    probes_dataed.show(5)
    probes_dataed.toPandas().to_csv("data/ner_probes_triplets_ptpairs_evd.csv", sep="\t", index=False)
    print(probes_data.count())
    print(probes_dataed.count())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.') 
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs and with evidence.')  # main dataset with OT evidence in data/ner_probes_triplets_ptpairs_ev.csv
    parser.add_argument('--datedevidence', required=True, help='Path to OT dated evidence folder with all subfolder inside for each data type.') # 
    args = parser.parse_args()
    main(args.input, args.datedevidence)