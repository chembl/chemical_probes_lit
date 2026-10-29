"""Shared TSV dialect: doubled quotes, literal backslashes, multiline fields."""

SPARK_TSV_OPTIONS = {
    'sep': '\t',
    'quote': '"',
    'escape': '"',
    'charToEscapeQuoteEscaping': '\x00',
    'ignoreLeadingWhiteSpace': 'false',
    'ignoreTrailingWhiteSpace': 'false',
}


def read_spark_tsv(spark, path):
    """Read pipeline/pandas TSV without splitting quoted multiline records."""
    return (spark.read.options(**SPARK_TSV_OPTIONS)
            .option('header', True).option('multiLine', True)
            .option('mode', 'FAILFAST').csv(str(path)))
