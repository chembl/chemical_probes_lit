"""Normalize OT association columns and audit dated-evidence coverage."""
# Mapping verified against Open Targets' public pipeline configuration, not a
# release-tagged 26.09 metadata file. Unknown datasource IDs fail explicitly.
MAPPING_SOURCE = "https://github.com/opentargets-archive/pts/blob/main/config.yaml"
MAPPING_CONFIG_SHA256 = "53aba8a22dea3a12332443f23789d63c62db1bdf7694963fc4c6353ab8ae641b"
DATASOURCE_DATATYPE = {'cancer_biomarkers': 'affected_pathway',
 'cancer_gene_census': 'somatic_mutation',
 'clingen': 'genetic_literature',
 'clinical_precedence': 'known_drug',
 'crispr': 'affected_pathway',
 'crispr_screen': 'affected_pathway',
 'encore': 'ot_partner',
 'europepmc': 'literature',
 'eva': 'genetic_association',
 'eva_somatic': 'somatic_mutation',
 'expression_atlas': 'rna_expression',
 'gene2phenotype': 'genetic_literature',
 'gene_burden': 'genetic_association',
 'genomics_england': 'genetic_literature',
 'gwas_credible_sets': 'genetic_association',
 'impc': 'animal_model',
 'intogen': 'somatic_mutation',
 'orphanet': 'genetic_association',
 'ot_crispr': 'ot_partner',
 'ot_crispr_validation': 'ot_validation_lab',
 'reactome': 'affected_pathway',
 'uniprot_literature': 'genetic_literature',
 'uniprot_variants': 'genetic_association'}


def normalize_associations(frame):
    from pyspark.sql import functions as f
    required = {'diseaseId', 'targetId', 'aggregationType', 'aggregationValue',
                'associationScore', 'evidenceCount'}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Association schema missing columns: {sorted(missing)}")
    sources = frame.filter(f.col('aggregationType') == 'datasourceId')
    if not sources.take(1):
        raise ValueError('No datasourceId aggregation rows in association input')
    observed = {r[0] for r in sources.select('aggregationValue').distinct().collect()}
    unknown = observed - set(DATASOURCE_DATATYPE)
    if unknown:
        raise ValueError(f"Unmapped datasource identifiers: {sorted(map(str, unknown))}; verify upstream mapping")
    mapping = f.create_map(*[f.lit(v) for pair in DATASOURCE_DATATYPE.items() for v in pair])
    return sources.select(
        'diseaseId', 'targetId', f.col('aggregationValue').alias('datasourceId'),
        mapping[f.col('aggregationValue')].alias('datatypeId'),
        f.col('associationScore').alias('score'), 'evidenceCount').distinct()


def join_dated_evidence(probes, evidence):
    """Exact-ID joins only; no historical datasource renaming or invented dates."""
    from pyspark.sql import functions as f
    keys = ['diseaseId', 'targetId', 'datasourceId']
    years = [('curationYear', 'allcurationYears'),
             ('publicationYear', 'allpublicationYears'), ('studyYear', 'allstudyYears')]
    dated = evidence.groupBy(*keys).agg(*[
        f.sort_array(f.collect_set(f.col(source).cast('int'))).alias(dest) for source, dest in years
    ]).withColumn('_datedPair', f.lit(True))
    sources = evidence.select('datasourceId').where('datasourceId IS NOT NULL').distinct().withColumn('_datedSource', f.lit(True))
    joined = probes.join(dated, keys, 'left').join(sources, 'datasourceId', 'left')
    has_dates = (f.size('allcurationYears') > 0) | (f.size('allpublicationYears') > 0) | (f.size('allstudyYears') > 0)
    joined = joined.withColumn('datedEvidenceStatus',
        f.when(f.col('datasourceId').isNull(), 'no_association')
         .when(f.col('_datedSource').isNull(), 'source_absent')
         .when(f.col('_datedPair').isNull(), 'pair_absent')
         .when(~has_dates, 'dates_missing').otherwise('matched'))
    # TSV cannot hold Spark array columns. JSON arrays also match the notebook's
    # existing list-text parser. Missing dates stay null rather than [] or zero.
    for _, name in years:
        joined = joined.withColumn(name, f.when(f.size(name) > 0, f.regexp_replace(f.to_json(f.col(name)), ',', ', ')))
    return joined.drop('_datedPair', '_datedSource')
