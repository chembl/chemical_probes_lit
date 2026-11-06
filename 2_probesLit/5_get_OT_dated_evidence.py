from helpers.evidence_dates import association_dates, dated_records, summarize_dates, join_date_summary, release_year
import json
from pathlib import Path
from helpers.tsv_format import read_spark_tsv
from helpers.tools import write_merged_files
from pyspark.sql import SparkSession, functions as f
import argparse
from helpers.pipeline_paths import output_path

def main(input_path, date_input, output, release, associations=False):
    max_year = release_year(release)
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

    # Restrict expensive date aggregation to this run's keys, but retain the full
    # source catalog so source_absent and pair_absent remain distinguishable.
    keys = probes_data.select('diseaseId', 'targetId', 'datasourceId').distinct()
    raw = spark.read.parquet(date_input)
    if associations:
        raw = raw.filter(f.col('aggregationType') == 'datasourceId')
        available_sources = raw.select(f.col('aggregationValue').alias('datasourceId'))
        selected = raw.join(keys.withColumnRenamed('datasourceId', 'aggregationValue'),
                            ['diseaseId', 'targetId', 'aggregationValue'], 'left_semi')
        summary = association_dates(selected, max_year)
        policy = 'association:OT_timeseries'
    else:
        available_sources = raw.select('datasourceId')
        selected = raw.join(keys, ['diseaseId', 'targetId', 'datasourceId'], 'left_semi')
        records, policy = dated_records(selected, max_year)
        summary = summarize_dates(records, policy)
    probes_dataed = join_date_summary(probes_data, summary, available_sources)
    # Count unique association keys, not repeated literature sentences.
    coverage = probes_dataed.select('diseaseId', 'targetId', 'datasourceId', 'datedEvidenceStatus').distinct()
    report = [row.asDict() for row in coverage.groupBy('datasourceId', 'datedEvidenceStatus').count().collect()]
    report.sort(key=lambda row: (row['datasourceId'] or '', row['datedEvidenceStatus']))
    report_path = output + '.dated_coverage.json'
    Path(report_path).write_text(json.dumps({
        'dated_evidence_input': str(Path(date_input).resolve()),
        'release': release,
        'date_policy': policy,
        'timeseries_rule': 'Only positive yearlyEvidenceCount and valid year <= release year; padding excluded' if associations else None,
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
    dates = parser.add_mutually_exclusive_group(required=True)
    dates.add_argument('--associations', help='Release-matched association_by_datasource_indirect parquet directory (recommended).')
    dates.add_argument('--datedevidence', help='Alternative record-level parquet: publicationYear, curationYear, studyYear and/or evidenceDate.')
    parser.add_argument('--release', required=True, help='Pinned input release, e.g. 26.09; bounds valid evidence years.')
    parser.add_argument('--output', required=True, help='Destination TSV file; parent directories are created.')
    args = parser.parse_args()
    main(args.input, args.associations or args.datedevidence, args.output, args.release, bool(args.associations))
