import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import requests

PIPELINE = Path(__file__).resolve().parents[1] / '2_probesLit'
sys.path.insert(0, str(PIPELINE))
spec = importlib.util.spec_from_file_location('filter_targets', PIPELINE/'3_filter_targets.py')
step = importlib.util.module_from_spec(spec)
spec.loader.exec_module(step)


def response(status, payload=None, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = payload
    return result


class TargetLookupTests(unittest.TestCase):
    @patch.object(step.time, 'sleep')
    def test_retry_server_and_rate_limit_then_success(self, sleep):
        session = Mock()
        session.get.side_effect = [response(500), response(503), response(429, headers={'Retry-After':'10'}),
                                   response(200, {'id':'ENSG000001'})]
        self.assertEqual(step.lookup_gene(session,'MTOR'),'ENSG000001')
        self.assertEqual(session.get.call_count,4)
        self.assertEqual(sleep.call_args_list[-1].args,(10.0,))
        self.assertEqual(session.get.call_args.kwargs['timeout'],(20,60))

    @patch.object(step.time, 'sleep')
    def test_timeout_and_invalid_success_response_are_failures(self, sleep):
        session = Mock()
        session.get.side_effect = [requests.Timeout('timeout'), response(200, {}), response(200, {'id':'ENSG123'})]
        self.assertEqual(step.lookup_gene(session,'MTOR'),'ENSG123')
        session.get.side_effect = None
        session.get.return_value = response(403)
        with self.assertRaisesRegex(RuntimeError,'HTTP 403'):
            step.lookup_gene(session,'MTOR')

    @patch.object(step.time, 'sleep')
    def test_partial_success_cached_outage_aborts_and_retry_reuses_cache(self, sleep):
        with tempfile.TemporaryDirectory() as directory:
            cache=Path(directory)/'cache.json'
            session=Mock()
            session.get.side_effect=[response(200,{'id':'ENSG1'})]+[response(500)]*5
            with self.assertRaisesRegex(RuntimeError,'failed for B'):
                step.map_gene_ids_to_ensembl(['A','A','B'],cache,session)
            self.assertEqual(set(json.loads(cache.read_text())['genes']),{'A'})
            session=Mock()
            session.get.return_value=response(200,{'id':'ENSG2'})
            result,missing,count=step.map_gene_ids_to_ensembl(['A','B'],cache,session)
            self.assertEqual(result,{'A':'ENSG1','B':'ENSG2'})
            self.assertEqual(missing,[])
            self.assertEqual(count,2)
            self.assertEqual(session.get.call_count,1)

    @patch.object(step.time, 'sleep')
    def test_not_found_is_explicit_and_not_cached(self, sleep):
        with tempfile.TemporaryDirectory() as directory:
            session=Mock()
            session.get.return_value=response(400,{'error':'No valid lookup found for symbol UNKNOWN'})
            cache=Path(directory)/'cache.json'
            result,missing,count=step.map_gene_ids_to_ensembl(['UNKNOWN'],cache,session)
            self.assertEqual((result,missing,count),({},['UNKNOWN'],1))
            self.assertFalse(cache.exists())
            session.get.return_value=response(400,{'error':'Bad request'})
            with self.assertRaisesRegex(RuntimeError,'HTTP 400'):
                step.lookup_gene(session,'UNKNOWN')

    @patch.object(step.time, 'sleep')
    def test_main_preserves_output_on_outage_then_filters_and_reports(self, sleep):
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory)
            probes=base/'probes.tsv'
            probes.write_text('CHEMBLID\tGENE\nCHEMBL1\tMTOR\n')
            triplets=base/'triplets.tsv'
            triplets.write_text('CD\tGP\tDS\nCHEMBL1\tENSG1\tD\nCHEMBL1\tENSG2\tD\n')
            output=base/'out.tsv';output.write_text('previous output\n')
            session=Mock();session.__enter__=Mock(return_value=session);session.__exit__=Mock(return_value=False)
            session.get.return_value=response(503)
            with patch.object(step.requests,'Session',return_value=session):
                with self.assertRaisesRegex(RuntimeError,'Step 3 stopped'):
                    step.main(triplets,probes,output)
            self.assertEqual(output.read_text(),'previous output\n')
            session.get.return_value=response(200,{'id':'ENSG1'})
            with patch.object(step.requests,'Session',return_value=session):
                step.main(triplets,probes,output)
            report=json.loads(Path(str(output)+'.mapping_report.json').read_text())
            self.assertEqual((report['input_rows'],report['output_rows']),(2,1))
            self.assertEqual(report['mapped_symbols'],1)
            self.assertEqual(step.pd.read_csv(output,sep='\t').GP.tolist(),['ENSG1'])

if __name__ == '__main__': unittest.main()
