"""Exact-case, whitespace-normalized chemical-probe synonym lookup."""
from collections import defaultdict
import pandas as pd


def split_synonyms(value):
    if pd.isna(value):
        return []
    return [token.strip() for token in str(value).split(',') if token.strip()]


def identifier_or_empty(value):
    if pd.isna(value):
        return ''
    value = str(value).strip()
    return '' if value.lower() in {'', 'none', 'nan', 'null', '<na>'} else value


def synonym_lookup(probes):
    """Return unique mappings and conflicts; never choose a last-row winner."""
    candidates = defaultdict(set)
    for identifier, synonyms in probes[['CHEMBLID', 'SYNOMS']].itertuples(index=False, name=None):
        identifier = identifier_or_empty(identifier)
        if not identifier:
            continue
        for label in split_synonyms(synonyms):
            candidates[label].add(str(identifier).strip())
    unique = {label: next(iter(ids)) for label, ids in candidates.items() if len(ids) == 1}
    conflicts = {label: sorted(ids) for label, ids in candidates.items() if len(ids) > 1}
    return unique, conflicts


def reground_chemicals(dataset, lookup):
    """Only CD rows may be remapped; unresolved/ambiguous labels retain OT IDs."""
    chemical = dataset['entity_type'].eq('CD')
    labels = dataset.loc[chemical, 'label'].str.strip()
    replacement = labels.map(lookup)
    dataset.loc[chemical, 'label_id'] = replacement.fillna(dataset.loc[chemical, 'label_id'])
