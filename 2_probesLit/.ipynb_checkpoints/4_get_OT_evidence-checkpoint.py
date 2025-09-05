###  Author: Melissa F. Adasme ###

from tools import download_files_from_ftp
from tools import write_merged_files
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

def main(input_path, OTevidence_path):
    ## 1. Initialise Spark Session 
    spark = SparkSession.builder.appName("Chemical Probes Lit Job").getOrCreate()
    print("✅ Spark Session initialized.", flush=True)

    ## 2. Load probes dataset with triples
    probes = (
            spark.read.csv(input_path, sep="\t", header=True)
            .distinct()
            .withColumnRenamed("DS", "diseaseId").withColumnRenamed("GP", "targetId")
            .withColumnRenamed("CD", "probeId").withColumnRenamed("SETID", "setId").
            withColumnRenamed("UNIPROT", "uniprot").withColumnRenamed("PROBE", "probe").withColumnRenamed("SET", "set")
            .withColumnRenamed("INCHI", "inchiKey").withColumnRenamed("GENE", "gene").withColumnRenamed("CONTR", "control")
            .withColumnRenamed("PREF_NAME", "prefName").withColumnRenamed("SYNOMS", "probeSyn").withColumnRenamed("SYNOMS_TARGET", "targetSyn")
        ) 
    print("✅ Loaded NER probes triples from step 3.", flush=True)

    ### 3. Load OT evidence data
    otevidence = (
        spark.read.parquet(
            OTevidence_path #"data/associationByDatasourceIndirect" 
        )
        .selectExpr(
            "datatypeId", 
            'datasourceId', 
            'diseaseId', 
            'targetId', 
            'score', 
            'evidenceCount',
        )
        .distinct()
    )
    print("✅ Loaded OT evidence.", flush=True)

    ## 4. Link OT evidence to target-disease pairs. Perform the join on both diseaseId and targetId
    probes_evidence = otevidence.join(probes, ["diseaseId", "targetId"], "right_outer")
    #probes_evidence = probes_evidence.withColumnRenamed("diseaseId", "DS").withColumnRenamed("targetId", "GP")

    ## 5. Writing output file
    temp_output_dir = "temp/evidence_output_parts"
    final_output_dir = "data/4_ner_probes_triplets_ptpairs_ev.tsv"
    write_merged_files(probes_evidence, temp_output_dir, final_output_dir) #function from tools.py
    print("✅ File with OT evidence linked to chemical probes data created", flush=True)

    # Show the results
    #probes_evidence.show(5) 

    # Close current spark session
    spark.stop() 
    print("✅ Analysis complete!", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.') 
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs.')  # main dataset with articles triplets in data/ner_probes_triplets_ptpairs.csv
    parser.add_argument('--evidence', required=True, help='Path to OT evidence files.') # OT evidence for target-disease pairs
    args = parser.parse_args()
    main(args.input, args.evidence)
