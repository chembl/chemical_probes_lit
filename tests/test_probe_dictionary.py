"""Regression tests for the missing-probe investigation and dictionary supplementation."""
import sys
from pathlib import Path
import unittest
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / '2_probesLit'))
from helpers.probe_dictionary import synonym_lookup, reground_chemicals
from helpers.supplement_probe_dictionary import supplement


class GroundingTests(unittest.TestCase):
    def test_whitespace_ambiguity_and_entity_types(self):
        hq = pd.DataFrame({'CHEMBLID': ['CHEMBL1', 'CHEMBL2', 'None'],
                           'SYNOMS': ['primary, alias, conflict, kinase', 'conflict', 'unidentified']})
        lookup, conflicts = synonym_lookup(hq)
        self.assertEqual(lookup, synonym_lookup(hq.iloc[::-1])[0])
        self.assertEqual(conflicts['conflict'], ['CHEMBL1', 'CHEMBL2'])
        d = pd.DataFrame({'label': [' alias ', 'conflict', 'kinase', 'unidentified'],
                          'label_id': ['OLD', 'CHEMBL2', 'ENSG1', 'OT_ID'],
                          'entity_type': ['CD', 'CD', 'GP', 'CD']})
        reground_chemicals(d, lookup)
        self.assertEqual(d.label_id.tolist(), ['CHEMBL1', 'CHEMBL2', 'ENSG1', 'OT_ID'])

    def test_restore_repair_and_alias_resolution_preserve_other_fields(self):
        cols = ['CHEMBLID', 'INCHI', 'SYNOMS', 'GENE', 'PROBE']
        current = pd.DataFrame([
            ['CHEMBL1', 'K1', 'BAY-885, genuine', 'MAPK7', 'BAY-885'],
            ['CHEMBL2', 'K2', 'BAY-885, other', 'AXL', 'OTHER'],
            ['None', 'K3', 'MK-5108', 'AURKA', 'MK-5108'],
        ], columns=cols)
        old = pd.DataFrame([
            ['CHEMBL3', 'K3', 'MK-5108', 'AURKA', 'MK-5108'],
            ['CHEMBL4', 'K4', 'restored', 'TARGET1', 'restored'],
            ['CHEMBL4', 'K4', 'restored', 'TARGET2', 'restored'],
        ], columns=cols)
        snapshot = current.copy(deep=True)
        result, provenance, details = supplement(current, old, {
            'restore_ids': ['CHEMBL4'], 'repair_ids': ['CHEMBL3'],
            'alias_owners': {'BAY-885': 'CHEMBL1'}})
        pd.testing.assert_frame_equal(current, snapshot)
        self.assertEqual(result.GENE.tolist(), ['MAPK7', 'AXL', 'AURKA', 'TARGET1', 'TARGET2'])
        self.assertEqual(result.iloc[2].CHEMBLID, 'CHEMBL3')
        self.assertEqual(synonym_lookup(result)[0]['BAY-885'], 'CHEMBL1')
        self.assertEqual(provenance.source.tolist(), ['current']*3 + ['manuscript']*2)
        self.assertEqual(provenance.source_data_row.tolist(), [1,2,3,2,3])
        self.assertEqual(details['identifier_repairs'][0]['rows'], 1)

    def test_reject_missing_and_ambiguous_repairs(self):
        current = pd.DataFrame({'CHEMBLID': [''], 'INCHI': ['K'], 'SYNOMS': ['probe']})
        old = pd.DataFrame({'CHEMBLID': ['CHEMBL1', 'CHEMBL2'], 'INCHI': ['K', 'K'], 'SYNOMS': ['a', 'b']})
        with self.assertRaisesRegex(ValueError, 'Ambiguous structure'):
            supplement(current, old, {'restore_ids': [], 'repair_ids': ['CHEMBL1']})
        with self.assertRaisesRegex(ValueError, 'absent from manuscript'):
            supplement(current, old, {'restore_ids': ['CHEMBL3']})


if __name__ == '__main__':
    unittest.main()
