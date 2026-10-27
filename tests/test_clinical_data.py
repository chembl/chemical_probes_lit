"""Clinical stage regression and HTTPS download checks."""
from pathlib import Path
import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

PIPELINE = Path(__file__).resolve().parents[1] / '2_probesLit'
sys.path.insert(0, str(PIPELINE))
from clinical_stages import STAGE_RANK, stage_label
from ot_download import list_parquet_files, download_files


def response(text='', content=b'', headers=None):
    result = Mock()
    result.__enter__ = Mock(return_value=result)
    result.__exit__ = Mock(return_value=False)
    result.text = text
    result.headers = headers or {}
    result.iter_content.return_value = iter([content])
    return result


class DownloadTests(unittest.TestCase):
    def test_listing_ignores_parent_links_and_other_hosts(self):
        session = Mock()
        session.get.return_value = response(text='''
        <a href="../">Parent</a><a href="?C=N">Sort</a>
        <a href="part-1.parquet">part</a><a href="part-1.parquet">duplicate</a>
        <a href="_SUCCESS">marker</a><a href="subdir/part.parquet">nested</a>
        <a href="https://other.test/part.parquet">foreign</a>
        <a href="%2Fescape.parquet">bad</a>''')
        self.assertEqual(list_parquet_files(session, 'https://example.test/data/'),
                         [('part-1.parquet', 'https://example.test/data/part-1.parquet')])

    def test_empty_or_error_listing_fails(self):
        session = Mock()
        session.get.return_value = response(text='<html>No parquet</html>')
        with self.assertRaisesRegex(ValueError, 'No directly listed parquet'):
            list_parquet_files(session, 'https://example.test/data/')

    def test_incomplete_transfer_preserves_existing_file(self):
        import requests
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'file.parquet'
            output.write_bytes(b'previous')
            session = Mock()
            session.get.side_effect = [response(content=b'bad', headers={'Content-Length': '10'}) for _ in range(3)]
            with patch('ot_download.time.sleep'), self.assertRaises(requests.RequestException):
                download_files(session, [('file.parquet', 'https://example.test/file.parquet')], directory)
            self.assertEqual(output.read_bytes(), b'previous')
            self.assertFalse(Path(str(output) + '.part').exists())

    def test_completed_transfer_publishes_file(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Mock()
            session.get.return_value = response(content=b'PAR1', headers={'Content-Length': '4'})
            download_files(session, [('file.parquet', 'https://example.test/file.parquet')], directory)
            self.assertEqual((Path(directory) / 'file.parquet').read_bytes(), b'PAR1')

    def test_missing_clinical_listing_prevents_all_downloads(self):
        spec = importlib.util.spec_from_file_location('download_step', PIPELINE / '0_download_OT_files.py')
        step = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(step)
        with tempfile.TemporaryDirectory() as directory:
            session = Mock()
            session.__enter__ = Mock(return_value=session)
            session.__exit__ = Mock(return_value=False)
            with patch.object(step, 'make_session', return_value=session), \
                 patch.object(step, 'list_parquet_files', side_effect=[
                     [('part.parquet', 'https://example.test/part.parquet')]] * 3 +
                     [ValueError('missing clinical listing')]), \
                 patch.object(step, 'download_files') as download, \
                 patch.object(sys, 'argv', ['step0', '--output-dir', directory]):
                with self.assertRaisesRegex(ValueError, 'missing clinical'):
                    step.main()
                download.assert_not_called()
                self.assertFalse((Path(directory) / 'download_manifest.json').exists())

    def test_unknown_is_evidence_not_absence(self):
        self.assertEqual(stage_label(STAGE_RANK['UNKNOWN']), 'UNKNOWN')
        self.assertEqual(stage_label(None), 'No matched indication')
        self.assertLess(STAGE_RANK['PHASE_2_3'], STAGE_RANK['PHASE_3'])
        self.assertNotEqual(STAGE_RANK['APPROVAL'], STAGE_RANK['PHASE_4'])


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1',
                     'Set RUN_SPARK_TESTS=1 with compatible Java/PySpark to run Spark integration tests')
class ClinicalSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[1]').appName('clinical-test')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '1').getOrCreate())
        spec = importlib.util.spec_from_file_location('step7', PIPELINE / '7_get_drug_max_phase.py')
        cls.step = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.step)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_disease_specific_stage_ties_and_left_join(self):
        targets = self.spark.createDataFrame([
            ('drugB', 'target1', 'APPROVAL'), ('drugA', 'target1', 'APPROVAL'),
            ('drugA', 'target1', 'APPROVAL'), ('drugC', 'target2', 'UNKNOWN')
        ], ['drugId', 'targetId', 'maxClinicalStage'])
        indications = self.spark.createDataFrame([
            ('drugA', 'disease1', 'PHASE_2'), ('drugB', 'disease1', 'PHASE_2'),
            ('drugA', 'disease2', 'APPROVAL'), ('drugC', 'disease3', None)
        ], ['drugId', 'diseaseId', 'maxClinicalStage'])
        pairs = self.step.clinical_pairs(targets, indications)
        rows = {(r.targetId, r.diseaseId): r for r in pairs.collect()}
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows['target1', 'disease1'].maxClinicalStage, 'PHASE_2')
        self.assertEqual(rows['target1', 'disease1'].drugId, 'drugA')
        self.assertEqual(rows['target1', 'disease2'].maxClinicalStage, 'APPROVAL')
        self.assertEqual(rows['target2', 'disease3'].maxClinicalStage, 'UNKNOWN')
        probes = self.spark.createDataFrame([('target1', 'disease1'), ('absent', 'disease4')], ['targetId', 'diseaseId'])
        joined = probes.join(pairs, ['targetId', 'diseaseId'], 'left')
        self.assertEqual(joined.count(), 2)
        self.assertIsNone(joined.filter("targetId = 'absent'").first().clinicalStageRank)

    def test_schema_and_new_stage_fail_explicitly(self):
        targets = self.spark.createDataFrame([('drugA', 'target1')], ['drugId', 'targetId'])
        indications = self.spark.createDataFrame([('drugA', 'disease1', 'NEW_STAGE')], ['drugId', 'diseaseId', 'maxClinicalStage'])
        with self.assertRaisesRegex(ValueError, 'Unrecognised'):
            self.step.clinical_pairs(targets, indications)
        with self.assertRaisesRegex(ValueError, 'missing required columns'):
            self.step.clinical_pairs(targets.drop('targetId'), indications)
