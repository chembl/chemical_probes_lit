from tsv_format import read_spark_tsv
"""Add disease-specific maximum clinical stage using OT clinical datasets."""
import argparse
from clinical_stages import STAGE_RANK
from pipeline_paths import output_path


def clinical_pairs(targets, indications):
    """One representative drug per target/disease, with deterministic tie-breaking.

    Clinical Target is used only for drug/target links, never its across-indication
    maximum. Clinical Indication supplies the disease and disease-specific stage.
    """
    from pyspark.sql import functions as f
    from pyspark.sql.window import Window

    for name, frame, required in (
        ('clinical_target', targets, {'drugId', 'targetId'}),
        ('clinical_indication', indications, {'drugId', 'diseaseId', 'maxClinicalStage'}),
    ):
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f'{name} is missing required columns: {sorted(missing)}')

    links = targets.select('drugId', 'targetId').dropna().distinct()
    stages = indications.select('drugId', 'diseaseId', 'maxClinicalStage').dropna(
        subset=['drugId', 'diseaseId'])
    invalid = stages.filter(
        f.col('maxClinicalStage').isNotNull()
        & ~f.col('maxClinicalStage').isin(list(STAGE_RANK))
    ).select('maxClinicalStage').distinct().limit(20).collect()
    if invalid:
        raise ValueError(f'Unrecognised clinical stages; update the explicit ranking: {invalid}')
    # A mapped indication with a missing stage is still clinical evidence.
    stages = stages.fillna({'maxClinicalStage': 'UNKNOWN'})
    mapping = f.create_map(*[f.lit(value) for pair in STAGE_RANK.items() for value in pair])
    joined = links.join(stages, 'drugId', 'inner').withColumn(
        'clinicalStageRank', mapping[f.col('maxClinicalStage')])
    window = Window.partitionBy('targetId', 'diseaseId').orderBy(
        f.col('clinicalStageRank').desc(), f.col('drugId').asc())
    return (joined.withColumn('_row', f.row_number().over(window))
            .filter(f.col('_row') == 1).drop('_row')
            .withColumn('hasClinicalIndication', f.lit(True)))


def main(input_path, clinical_target_path, clinical_indication_path, output):
    from pyspark.sql import SparkSession, functions as f
    from tools import write_merged_files

    output = output_path(output)
    spark = (SparkSession.builder.appName('Chemical Probes Clinical Stage')
             .config('spark.driver.memory', '200g')
             .config('spark.executor.memory', '200g')
             .config('spark.driver.memoryOverhead', '10g')
             .config('spark.driver.maxResultSize', '2g').getOrCreate())
    try:
        probes = read_spark_tsv(spark, input_path)
        targets = spark.read.parquet(clinical_target_path)
        indications = spark.read.parquet(clinical_indication_path)
        pairs = clinical_pairs(targets, indications)
        result = probes.join(pairs, ['diseaseId', 'targetId'], 'left').withColumn(
            'hasClinicalIndication', f.coalesce(f.col('hasClinicalIndication'), f.lit(False)))
        write_merged_files(result, output + '.parts', output)
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Step 6 TSV file.')
    parser.add_argument('--clinical-target', required=True, help='Clinical Target parquet directory.')
    parser.add_argument('--clinical-indication', required=True, help='Clinical Indication parquet directory.')
    parser.add_argument('--output', required=True, help='Destination TSV; parent directories are created.')
    args = parser.parse_args()
    main(args.input, args.clinical_target, args.clinical_indication, args.output)
