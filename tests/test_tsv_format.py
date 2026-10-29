"""Regression checks for literal backslashes and quoted multiline sentences."""
import csv
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd

PIPELINE = Path(__file__).resolve().parents[1] / '2_probesLit'
sys.path.insert(0, str(PIPELINE))
from helpers.tsv_format import read_spark_tsv

SENTENCE = '  NF-\\ and \\documentclass: "quoted"\ttext\nnext line  '


class TripletTSVTests(unittest.TestCase):
    def test_sentence_and_label_survive_triplet_extraction(self):
        spec = importlib.util.spec_from_file_location('triplets', PIPELINE / '2_get_triplets.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pd.DataFrame({'CHEMBLID': ['CHEMBL1'], 'SYNOMS': ['probe']}).to_csv(root / 'probes.tsv', sep='\t', index=False)
            rows = [('probe', 'CHEMBL1', 'CD'), ('NF-\\', 'ENSG1', 'GP'), ('disease', 'EFO1', 'DS')]
            with (root / 'input.tsv').open('w', newline='') as handle:
                writer = csv.writer(handle, delimiter='\t', lineterminator='\n')
                writer.writerow(['pmid', 'label', 'label_id', 'entity_type', 'date', 'section', 'sentence'])
                for label, identifier, kind in rows:
                    writer.writerow(['1', label, identifier, kind, '2020', 'results', SENTENCE])
            module.main(root / 'input.tsv', root / 'probes.tsv', root / 'output.tsv')
            result = pd.read_csv(root / 'output.tsv', sep='\t')
            self.assertEqual(len(result), 1)
            self.assertEqual(result.iloc[0]['sentence'], SENTENCE)
            self.assertEqual(result.iloc[0]['GP'], 'ENSG1')


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Requires Java/PySpark; set RUN_SPARK_TESTS=1')
class SparkTSVTests(unittest.TestCase):
    def test_spark_pandas_roundtrip(self):
        from pyspark.sql import SparkSession
        from helpers.tools import write_merged_files
        spark = SparkSession.builder.master('local[1]').appName('tsv-regression').getOrCreate()
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                rows = [('NF-\\', SENTENCE), ('"quoted label"', 'a\r\nb'), ('plain', 'ends\\')]
                frame = spark.createDataFrame(rows, ['label', 'sentence'])
                output = root / 'spark.tsv'
                write_merged_files(frame, str(root / 'parts'), output)
                pandas_frame = pd.read_csv(output, sep='\t', keep_default_na=False)
                self.assertEqual(list(pandas_frame.itertuples(index=False, name=None)), rows)
                self.assertEqual([tuple(r) for r in read_spark_tsv(spark, output).collect()], rows)
                pandas_frame.to_csv(root / 'pandas.tsv', sep='\t', index=False)
                self.assertEqual([tuple(r) for r in read_spark_tsv(spark, root / 'pandas.tsv').collect()], rows)
        finally:
            spark.stop()
