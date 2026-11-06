import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import pandas as pd

spec = importlib.util.spec_from_file_location('reviews', Path(__file__).resolve().parents[1] / '2_probesLit/8_classify_article_types.py')
reviews = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reviews)


class ReviewTests(unittest.TestCase):
    def test_chunking_cache_resume_and_text_preservation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output, cache = root/'in.tsv', root/'out.tsv', root/'cache.sqlite'
            original = pd.DataFrame({'pmid':['1','2','1','3',''],
                'sentence':['NF-\\', 'a\t"quote"\nline', 'NA', 'None', ''],
                'score':['0.500','0','1.00','', '2']})
            original.to_csv(source, sep='\t', index=False)
            with patch.object(reviews, 'fetch_batch', return_value={'1':'review','2':'primary'}) as fetch:
                reviews.annotate(source, output, cache, 2)
                self.assertEqual(set(fetch.call_args.args[1]), {'1','2','3'})
            result = pd.read_csv(output,sep='\t',dtype=str,keep_default_na=False)
            pd.testing.assert_frame_equal(result.drop(columns='articleType'),original)
            self.assertEqual(result.articleType.tolist(),['review','primary','review','',''])
            with patch.object(reviews, 'fetch_batch', return_value={'3':'primary'}) as fetch:
                reviews.annotate(source, output, cache, 1)
                self.assertEqual(fetch.call_args.args[1],['3'])
            with patch.object(reviews, 'fetch_batch') as fetch:
                reviews.annotate(source, output, cache, 2)
                fetch.assert_not_called()

    def test_missing_pubmed_records_are_not_primary(self):
        session = Mock()
        session.post.return_value.json.return_value = {'result':{
            '1': {'pubtype':['Review']}, '2':{'pubtype':['Journal Article']},
            '3': {'error':'Not found'}, '4': {'pubtype':[]}}}
        with patch.object(reviews.time,'sleep'):
            labels = reviews.fetch_batch(session,['1','2','3','4','5'])
        self.assertEqual(labels,{'1':'review','2':'primary'})

    def test_failure_preserves_previous_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            pd.DataFrame({'pmid':['1']}).to_csv(root/'in.tsv',sep='\t',index=False)
            (root/'out.tsv').write_text('previous output')
            with patch.object(reviews,'fetch_batch',return_value={'1':'review'}), patch.object(pd.DataFrame,'to_csv',side_effect=OSError('disk full')):
                with self.assertRaises(OSError):
                    reviews.annotate(root/'in.tsv',root/'out.tsv',root/'cache.sqlite',1)
            self.assertEqual((root/'out.tsv').read_text(),'previous output')
            self.assertEqual(list(root.glob('*.part')),[])

    def test_rate_limit_honours_retry_after_and_recovers(self):
        import requests
        session = Mock()
        limited = Mock(status_code=429, headers={'Retry-After': '90'})
        limited.raise_for_status.side_effect = requests.HTTPError('429', response=limited)
        success = Mock()
        success.json.return_value = {'result': {'1': {'pubtype': ['Review']}}}
        session.post.side_effect = [limited, success]
        with patch.object(reviews.time, 'sleep') as sleep:
            self.assertEqual(reviews.fetch_batch(session, ['1']), {'1': 'review'})
        sleep.assert_any_call(90.0)
        self.assertEqual(session.post.call_count, 2)

    def test_exhausted_rate_limit_keeps_unknown(self):
        import requests
        session = Mock()
        limited = Mock(status_code=429, headers={})
        limited.raise_for_status.side_effect = requests.HTTPError('429', response=limited)
        session.post.return_value = limited
        with patch.object(reviews.time, 'sleep') as sleep:
            self.assertEqual(reviews.fetch_batch(session, ['1']), {})
        self.assertEqual(session.post.call_count, 5)
        sleep.assert_any_call(480)

    def test_input_output_must_differ(self):
        with self.assertRaises(ValueError):
            reviews.annotate('same.tsv','same.tsv','cache.sqlite')
