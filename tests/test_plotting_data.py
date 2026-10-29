import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd

PIPELINE = Path(__file__).resolve().parents[1] / '2_probesLit'
sys.path.insert(0, str(PIPELINE))
from plotting_data import PLOT_COLUMNS, load_plotting_data, review_selection
spec = importlib.util.spec_from_file_location('prepare_plot', PIPELINE / '9_prepare_plotting_data.py')
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PlottingTests(unittest.TestCase):
    def fixture(self):
        rows = []
        for index in range(24):
            row = {c: '' for c in PLOT_COLUMNS}
            row.update(diseaseId=f'D{index % 3}', targetId=f'T{index % 2}',
                       probeId=f'CHEMBL{index % 4}', pmid=str(100 + index % 7),
                       sentence='Text with \\literal, "quote", tab\tand\nnewline',
                       section='results', date='2020-01-01', probe=f'probe{index % 4}',
                       gene=f'gene{index % 2}', name=f'disease{index % 3}',
                       datatypeId='genetic_association' if index % 2 else 'literature',
                       datasourceId='eva' if index % 2 else 'europepmc',
                       clinicalStageRank='6', maxClinicalStage='PHASE_2',
                       hasClinicalIndication='true', score='0.500', evidenceCount='2',
                       allcurationYears='[2001, 2003]', datedEvidenceStatus='matched',
                       articleType=['review','primary',''][index % 3],
                       control='UNUSED' * 1000, probeSyn='huge unused synonyms', targetSyn='unused')
            rows.append(row)
        # Preserve even rows differing only in unplotted metadata.
        rows.append(dict(rows[0], control='different control'))
        return pd.DataFrame(rows)

    def test_projection_preserves_rows_text_and_figure_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture().to_csv(root/'full.tsv',sep='\t',index=False)
            prepare.prepare(root/'full.tsv',root/'plot.tsv.gz',chunk_size=3)
            full = load_plotting_data(root/'full.tsv')
            reduced = load_plotting_data(root/'plot.tsv.gz')
            pd.testing.assert_frame_equal(full[PLOT_COLUMNS],reduced[PLOT_COLUMNS])
            manifest=json.loads((root/'plot.tsv.gz.manifest.json').read_text())
            self.assertEqual(manifest['rows'],25)
            self.assertIn('control',manifest['omitted_columns'])
            # Aggregations used by histogram, heatmaps and sentence-section counts.
            for keys in [['probeId'], ['targetId','diseaseId'], ['datasourceId']]:
                pd.testing.assert_series_equal(full.groupby(keys).pmid.nunique(),reduced.groupby(keys).pmid.nunique())
            subset=['diseaseId','targetId','pmid','section','probeId','sentence']
            pd.testing.assert_frame_equal(full[subset].drop_duplicates(),reduced[subset].drop_duplicates())

    def test_actual_histogram_cell_renders_identically(self):
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import seaborn as sns
        nb=json.loads((PIPELINE/'10_plotting_data.ipynb').read_text())
        code=next(''.join(c['source']) for c in nb['cells'] if c['cell_type']=='code' and 'sns.histplot(data=gr' in ''.join(c['source']))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            self.fixture().to_csv(root/'full.tsv',sep='\t',index=False)
            prepare.prepare(root/'full.tsv',root/'plot.tsv.gz',chunk_size=4)
            images=[]
            for source in [root/'full.tsv',root/'plot.tsv.gz']:
                plt.close('all')
                exec(code, {'ner_triplets_ptpairs':load_plotting_data(source), 'plt':plt,'sns':sns,'FIGURES_DIR':root})
                plt.gcf().canvas.draw()
                images.append(bytes(plt.gcf().canvas.buffer_rgba()))
            self.assertEqual(images[0],images[1])
            plt.close('all')

    def test_review_policies_keep_unknown_explicit(self):
        labels=pd.Series(['review','primary',None])
        self.assertEqual(review_selection(labels).tolist(),[False,True,True])
        self.assertEqual(review_selection(labels,'classified_non_reviews').tolist(),[False,True,False])

    def test_optional_counts_are_exact_across_chunks(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            frame=pd.DataFrame({'pmid':['1','2','1'], 'sentence':['a\nb','c','a\nb']})
            frame.to_csv(root/'triplets.tsv',sep='\t',index=False)
            report=prepare.triplet_summary(root/'triplets.tsv',1,root)
            self.assertEqual(report,{'rows':3,'nunique':{'pmid':2,'sentence':2}})
