import os
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / '2_probesLit'))
from helpers.association_schema import DATASOURCE_DATATYPE, normalize_associations, join_dated_evidence


class MappingTests(unittest.TestCase):
    def test_new_sources_are_not_aliased_to_legacy_ids(self):
        self.assertEqual(DATASOURCE_DATATYPE['clinical_precedence'], 'known_drug')
        self.assertEqual(DATASOURCE_DATATYPE['gwas_credible_sets'], 'genetic_association')
        self.assertNotIn('chembl', DATASOURCE_DATATYPE)
        self.assertNotIn('ot_genetics_portal', DATASOURCE_DATATYPE)


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'Requires Java/PySpark; set RUN_SPARK_TESTS=1')
class AssociationSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master('local[1]').appName('association-tests').config('spark.sql.shuffle.partitions','1').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_normalization_and_unmapped_source(self):
        schema = 'diseaseId string, targetId string, aggregationType string, aggregationValue string, associationScore double, evidenceCount long'
        frame = self.spark.createDataFrame([
            ('D', 'T', 'datasourceId', 'clinical_precedence', 0.5, 2),
            ('D', 'T', 'datatypeId', 'known_drug', 0.8, 3)], schema)
        rows = normalize_associations(frame).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].datasourceId, 'clinical_precedence')
        self.assertEqual(rows[0].datatypeId, 'known_drug')
        self.assertEqual(rows[0].score, 0.5)
        bad = self.spark.createDataFrame([('D','T','datasourceId','new_source',0.5,2)],schema)
        with self.assertRaisesRegex(ValueError, 'Unmapped'):
            normalize_associations(bad)

    def test_coverage_and_missing_dates_preserve_rows(self):
        probes = self.spark.createDataFrame([
            ('D','T','clinical_precedence'), ('D','T','gwas_credible_sets'),
            ('D','T','eva'), ('D','T','impc'), ('D','T','orphanet'), ('D','T',None)
        ], 'diseaseId string, targetId string, datasourceId string')
        dated = self.spark.createDataFrame([
            ('D','T','chembl',2000,None,None),
            ('D','T','ot_genetics_portal',2000,None,None),
            ('D','T','eva',2001,2002,None),
            ('D','T','eva',2003,None,None),
            ('D','T','impc',None,None,None),
            ('OTHER','T','orphanet',2001,None,None)
        ], 'diseaseId string, targetId string, datasourceId string, curationYear int, publicationYear int, studyYear int')
        rows = {r.datasourceId: r for r in join_dated_evidence(probes,dated).collect()}
        self.assertEqual(len(rows),6)
        for source in ['clinical_precedence','gwas_credible_sets']:
            self.assertEqual(rows[source].datedEvidenceStatus,'source_absent')
            self.assertIsNone(rows[source].allcurationYears)
        self.assertEqual(rows['eva'].datedEvidenceStatus,'matched')
        self.assertEqual(rows['eva'].allcurationYears,'[2001, 2003]')
        self.assertEqual(rows['impc'].datedEvidenceStatus,'dates_missing')
        self.assertIsNone(rows['impc'].allcurationYears)
        self.assertEqual(rows['orphanet'].datedEvidenceStatus,'pair_absent')
        self.assertEqual(rows[None].datedEvidenceStatus,'no_association')
