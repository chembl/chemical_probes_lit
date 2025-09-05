###  Author: Melissa F. Adasme ###

from tools import download_files_from_ftp
import pyarrow.parquet as pq
from tools import initialize_sparksession
import pyspark.sql.functions as f
import argparse

def main(input_path, OTevidence_path):
    # Sparl session initialized with increased  Memory
    spark = initialize_sparksession()

    ### PROBES dataset ###
    probes = (
            spark.read.csv(input_path, sep="\t", header=True)
            .distinct()
            .withColumnRenamed("DS", "diseaseId").withColumnRenamed("GP", "targetId")
            .withColumnRenamed("CD", "probeId").withColumnRenamed("SETID", "setId").
            withColumnRenamed("UNIPROT", "uniprot").withColumnRenamed("PROBE", "probe").withColumnRenamed("SET", "set")
            .withColumnRenamed("INCHI", "inchiKey").withColumnRenamed("GENE", "gene").withColumnRenamed("CONTR", "control")
            .withColumnRenamed("PREF_NAME", "prefName").withColumnRenamed("SYNOMS", "probeSyn").withColumnRenamed("SYNOMS_TARGET", "targetSyn")
        ) 


    ### OT evidence ###
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

    # Perform the join on both diseaseId and targetId
    probes_evidence = otevidence.join(probes, ["diseaseId", "targetId"], "right_outer")
    #probes_evidence = probes_evidence.withColumnRenamed("diseaseId", "DS").withColumnRenamed("targetId", "GP")

    # Save the results
    probes_evidence.toPandas().to_csv("data/ner_probes_triplets_ptpairs_ev.csv", sep="\t", index=False)

    # Show the results
    probes_evidence.show(5) 

    # Close current spark session
    spark.stop() 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all OT evidence for each T-D from the NER chemical probes literature data.') 
    parser.add_argument('--input', required=True, help='Path to the ner triplets with P-T pairs.')  # main dataset with articles triplets in data/ner_probes_triplets_ptpairs.csv
    parser.add_argument('--evidence', required=True, help='Path to OT evidence files.') # 
    args = parser.parse_args()
    main(args.input, args.evidence)
