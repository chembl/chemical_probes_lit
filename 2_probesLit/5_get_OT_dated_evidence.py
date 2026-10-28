from association_schema import join_dated_evidence
import json
from pathlib import Path
from tsv_format import read_spark_tsv
from tools import write_merged_files
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list, when, col, size
import pyspark.sql.functions as f
import argparse
from pipeline_paths import output_path
import os
from functools import reduce

def main(input_path, OTdatedevidence_path, output):
    output = output_path(output)
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
    ## 1. Initialise Spark Session
    print("✅ Spark Session initialized.", flush=True)

    ## 2. Load NER probes with evidence dataset
    probes_data = (
            read_spark_tsv(spark, input_path)
        )
    print("✅ Loaded NER probes with evidence from step 4.", flush=True)

    ## 3. Get a list of all subfolders
    data_folder = OTdatedevidence_path # path of the main folder containing al subfolders for each data type
    subfolders = [f.path for f in os.scandir(data_folder) if f.is_dir()]
    if not subfolders:
        raise ValueError(f"No dated evidence subdirectories in {data_folder}")
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

    probes_dataed = join_dated_evidence(probes_data, probes_dataev)
    # Count unique association keys, not repeated literature sentences.
    coverage = probes_dataed.select('diseaseId', 'targetId', 'datasourceId', 'datedEvidenceStatus').distinct()
    report = [row.asDict() for row in coverage.groupBy('datasourceId', 'datedEvidenceStatus').count().collect()]
    report.sort(key=lambda row: (row['datasourceId'] or '', row['datedEvidenceStatus']))
    report_path = output + '.dated_coverage.json'
    Path(report_path).write_text(json.dumps({
        'dated_evidence_input': str(Path(OTdatedevidence_path).resolve()),
        'join_policy': 'Exact diseaseId, targetId, datasourceId; no source aliases',
        'counts_by_unique_association_key': report,
    }, indent=2) + '\n')
    print(f"Dated-evidence coverage: {report_path}", flush=True)
    for row in report:
        print(row, flush=True)

    ## 6. Create the file with the results
    temp_output_dir = output + ".parts"
    final_output_dir = output
    write_merged_files(probes_dataed, temp_output_dir, final_output_dir) #function from tools.py
    print("✅ File with OT dated evidence linked to chemical probes data created", flush=True)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.')
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs and with evidence.')  # main dataset with OT evidence in data/ner_probes_triplets_ptpairs_ev.csv
    parser.add_argument('--datedevidence', required=True, help='Path to OT dated evidence folder with all subfolder inside for each data type.') #
    parser.add_argument('--output', required=True, help='Destination TSV file; parent directories are created.')
    args = parser.parse_args()
    main(args.input, args.datedevidence, args.output)
