"""Small filesystem checks; no cluster, network or running Spark session needed."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PIPELINE = Path(__file__).resolve().parents[1] / '2_probesLit'
sys.path.insert(0, str(PIPELINE))
from helpers.tools import write_merged_files


class PipelinePathsTest(unittest.TestCase):
    def test_triplets_from_unrelated_working_directory(self):
        with tempfile.TemporaryDirectory(prefix='probe paths ') as directory:
            base = Path(directory)
            probes = base / 'probes.tsv'
            probes.write_text('CHEMBLID\tSYNOMS\nCHEMBL1\tprobe\n')
            annotations = base / 'annotations.tsv'
            annotations.write_text(
                'pmid\tlabel\tlabel_id\tentity_type\tdate\tsection\tsentence\n'
                '1\tprobe\told-id\tCD\t2020\tRESULTS\tExample\n'
                '1\ttarget\tENSG1\tGP\t2020\tRESULTS\tExample\n'
                '1\tdisease\tEFO1\tDS\t2020\tRESULTS\tExample\n'
            )
            output = base / 'new results' / 'triplets.tsv'
            subprocess.run([
                sys.executable, str(PIPELINE / '2_get_triplets.py'),
                '--input', str(annotations), '--probes', str(probes),
                '--output', str(output),
            ], cwd=base, check=True, capture_output=True,
                env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'})
            self.assertEqual(output.read_text(),
                'pmid\tdate\tsection\tCD\tGP\tDS\tsentence\n'
                '1\t2020\tRESULTS\tCHEMBL1\tENSG1\tEFO1\tExample\n')
            self.assertFalse((base / 'data').exists())

    def test_merge_handles_spaces_and_preserves_output_on_failure(self):
        class Writer:
            fail = False

            def csv(self, destination, **kwargs):
                parts = Path(destination)
                parts.mkdir()
                (parts / 'part-00000.csv').write_text('one\ttwo\n')
                (parts / 'part-00001.csv').write_text('three\tfour\n')
                if self.fail:
                    raise RuntimeError('simulated write failure')

        class Frame:
            columns = ['a', 'b']
            write = Writer()

        with tempfile.TemporaryDirectory(prefix='probe merge ') as directory:
            output = Path(directory) / 'nested results' / 'output.tsv'
            write_merged_files(Frame(), str(output) + '.parts', str(output))
            expected = 'a\tb\none\ttwo\nthree\tfour\n'
            self.assertEqual(output.read_text(), expected)
            self.assertEqual(list(output.parent.iterdir()), [output])
            Frame.write.fail = True
            with self.assertRaisesRegex(RuntimeError, 'simulated'):
                write_merged_files(Frame(), str(output) + '.parts', str(output))
            self.assertEqual(output.read_text(), expected)
            self.assertEqual(list(output.parent.iterdir()), [output])


if __name__ == '__main__':
    unittest.main()
