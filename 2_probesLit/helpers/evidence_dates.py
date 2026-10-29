"""Release-matched association dates or per-record publication/curation/study fallback."""
import re

KEYS = ['diseaseId', 'targetId', 'datasourceId']
DATE_COLUMNS = ['allEvidenceYears', 'firstEvidenceYear', 'evidenceYearPolicy',
                'earliestYearOrigins', 'datedEvidenceCount', 'undatedEvidenceCount']
RAW_YEARS = [('publicationYear', 'allpublicationYears'),
             ('curationYear', 'allcurationYears'), ('studyYear', 'allstudyYears')]


def release_year(release):
    if not re.fullmatch(r'\d{2}\.(?:0[1-9]|1[0-2])', release):
        raise ValueError('Use a pinned OT release such as 26.09, not latest')
    return 2000 + int(release[:2])


def valid_year(column, max_year):
    from pyspark.sql import functions as f
    # Accept a four-digit year or an ISO date, never silently truncate decimals.
    value = f.regexp_extract(f.trim(column.cast('string')), r'^(\d{4})(?:-\d{2}-\d{2}(?:[T ].*)?)?$', 1).cast('int')
    return f.when(value.between(1000, max_year), value)


def dated_records(evidence, max_year):
    """Select a date before aggregation, retaining the selected field's identity."""
    from pyspark.sql import functions as f
    missing = set(KEYS) - set(evidence.columns)
    if missing:
        raise ValueError(f'Evidence missing columns: {sorted(missing)}')
    available = [name for name, _ in RAW_YEARS if name in evidence.columns]
    if 'evidenceDate' in evidence.columns:
        available.append('evidenceDate')
    if not available:
        raise ValueError('Evidence requires publicationYear, curationYear, studyYear or evidenceDate')
    selected = f.lit(None).cast('int')
    origin = f.lit(None).cast('string')
    for name in reversed(available):
        year = valid_year(f.col(name), max_year)
        selected = f.coalesce(year, selected)
        origin = f.when(year.isNotNull(), f.lit(name)).otherwise(origin)
    policy = 'record:publication>curation>study>evidenceDate' if any(n != 'evidenceDate' for n in available) else 'record:OT_evidenceDate'
    frame = evidence.withColumn('_year', selected).withColumn('_origin', origin)
    for name, _ in RAW_YEARS:
        frame = frame.withColumn(name, valid_year(f.col(name), max_year) if name in evidence.columns else f.lit(None).cast('int'))
    return frame, policy


def association_dates(associations, max_year):
    """Summarize datasource timeseries; exclude padding and undated/future bins."""
    from pyspark.sql import functions as f
    required = {'diseaseId', 'targetId', 'aggregationType', 'aggregationValue', 'timeseries'}
    if required - set(associations.columns):
        raise ValueError(f'Association dates missing columns: {sorted(required - set(associations.columns))}')
    fields = set(associations.schema['timeseries'].dataType.elementType.fieldNames())
    if not {'year', 'yearlyEvidenceCount'} <= fields:
        raise ValueError('timeseries must contain year and yearlyEvidenceCount')
    sources = associations.filter(f.col('aggregationType') == 'datasourceId').select(
        'diseaseId', 'targetId', f.col('aggregationValue').alias('datasourceId'), 'timeseries')
    # Duplicate keys would inflate counts or conceal mixed releases.
    if sources.groupBy(*KEYS).count().filter('count > 1').take(1):
        raise ValueError('Duplicate association keys; provide one release and one association dataset')
    records = sources.select(*KEYS, f.explode_outer('timeseries').alias('_bin'))
    year = valid_year(f.col('_bin.year'), max_year)
    records = records.withColumn('_count', f.coalesce(f.col('_bin.yearlyEvidenceCount'), f.lit(0)))
    records = records.withColumn('_year', f.when(f.col('_count') > 0, year))
    records = records.withColumn('_origin', f.when(f.col('_year').isNotNull(), f.lit('OT_timeseries')))
    for name, _ in RAW_YEARS:
        records = records.withColumn(name, f.lit(None).cast('int'))
    return summarize_dates(records, 'association:OT_timeseries', weighted=True)


def summarize_dates(records, policy, weighted=False):
    from pyspark.sql import functions as f
    weight = f.greatest(f.col('_count'), f.lit(0)) if weighted else f.lit(1)
    summary = records.groupBy(*KEYS).agg(
        f.sort_array(f.collect_set('_year')).alias('allEvidenceYears'),
        f.min('_year').alias('firstEvidenceYear'),
        f.sum(f.when(f.col('_year').isNotNull(), weight).otherwise(0)).alias('datedEvidenceCount'),
        f.sum(f.when(f.col('_year').isNull(), weight).otherwise(0)).alias('undatedEvidenceCount'),
        *[f.sort_array(f.collect_set(name)).alias(dest) for name, dest in RAW_YEARS])
    origins = records.join(summary.select(*KEYS, 'firstEvidenceYear'), KEYS).filter(
        f.col('_year') == f.col('firstEvidenceYear')).groupBy(*KEYS).agg(
            f.sort_array(f.collect_set('_origin')).alias('earliestYearOrigins'))
    return summary.join(origins, KEYS, 'left').withColumn('evidenceYearPolicy', f.lit(policy))


def join_date_summary(probes, summary, available_sources=None):
    """Left join one row per association key without inventing source aliases."""
    from pyspark.sql import functions as f
    source_frame = summary if available_sources is None else available_sources
    sources = source_frame.select('datasourceId').distinct().withColumn('_datedSource', f.lit(True))
    frame = probes.join(summary.withColumn('_datedPair', f.lit(True)), KEYS, 'left').join(sources, 'datasourceId', 'left')
    frame = frame.withColumn('datedEvidenceStatus',
        f.when(f.col('datasourceId').isNull(), 'no_association')
        .when(f.col('_datedSource').isNull(), 'source_absent')
        .when(f.col('_datedPair').isNull(), 'pair_absent')
        .when(f.col('firstEvidenceYear').isNull(), 'dates_missing').otherwise('matched'))
    for name in ['allEvidenceYears', 'earliestYearOrigins'] + [dest for _, dest in RAW_YEARS]:
        frame = frame.withColumn(name, f.when(f.size(name) > 0, f.regexp_replace(f.to_json(f.col(name)), ',', ', ')))
    return frame.drop('_datedPair', '_datedSource')
