"""Create a separately versioned dictionary from an explicit supplementation plan."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import os
import pandas as pd
from helpers.probe_dictionary import split_synonyms, synonym_lookup, identifier_or_empty


def supplement(current, manuscript, plan):
    if set(current.columns) != set(manuscript.columns):
        raise ValueError('Current and manuscript dictionary schemas differ')
    result = current.copy().reset_index(drop=True)
    origins = [{'source': 'current', 'source_data_row': i + 1, 'changes': []} for i in range(len(result))]
    for identifier in plan['restore_ids']:
        if result.CHEMBLID.eq(identifier).any():
            raise ValueError(f'Restore ID already present: {identifier}')
        selected = manuscript[manuscript.CHEMBLID.eq(identifier)]
        if selected.empty:
            raise ValueError(f'Restore ID absent from manuscript: {identifier}')
        result = pd.concat([result, selected[result.columns]], ignore_index=True)
        origins.extend({'source': 'manuscript', 'source_data_row': int(i) + 1,
                        'changes': ['manuscript_supplement']} for i in selected.index)
    repairs = []
    for identifier in plan.get('repair_ids', []):
        old = manuscript[manuscript.CHEMBLID.eq(identifier)]
        structures = set(old.INCHI) - {''}
        if not structures:
            raise ValueError(f'No manuscript structure for repair: {identifier}')
        mask = result.INCHI.isin(structures) & result.CHEMBLID.map(identifier_or_empty).eq('')
        if not mask.any():
            raise ValueError(f'No blank-ID exact-structure match for {identifier}')
        for key in structures:
            ids = set(manuscript.loc[manuscript.INCHI.eq(key), 'CHEMBLID'].map(identifier_or_empty)) - {''}
            ids |= set(result.loc[result.INCHI.eq(key), 'CHEMBLID'].map(identifier_or_empty)) - {''}
            if ids != {identifier}:
                raise ValueError(f'Ambiguous structure repair: {key}: {sorted(ids)}')
        result.loc[mask, 'CHEMBLID'] = identifier
        for i in result.index[mask]:
            origins[i]['changes'].append('restored_identifier_from_exact_manuscript_InChIKey')
        repairs.append({'CHEMBLID': identifier, 'rows': int(mask.sum())})
    removals = []
    for label, owner in plan.get('alias_owners', {}).items():
        tokens = result.SYNOMS.map(split_synonyms)
        has_label = tokens.map(lambda values: label in values)
        if not (has_label & result.CHEMBLID.eq(owner)).any():
            raise ValueError(f'Alias owner must already contain {label!r}: {owner}')
        for i in result.index[has_label & ~result.CHEMBLID.eq(owner)]:
            result.at[i, 'SYNOMS'] = ', '.join(s for s in tokens[i] if s != label)
            origins[i]['changes'].append(f'removed_conflicting_alias:{label}')
            removals.append({'data_row': int(i) + 1, 'CHEMBLID': result.at[i, 'CHEMBLID'],
                             'alias': label, 'owner': owner})
    _, conflicts = synonym_lookup(result)
    provenance = pd.DataFrame([{'output_data_row': i + 1, 'CHEMBLID': result.at[i, 'CHEMBLID'],
                               'source': o['source'], 'source_data_row': o['source_data_row'],
                               'changes': ';'.join(o['changes'])} for i, o in enumerate(origins)])
    return result, provenance, {'identifier_repairs': repairs, 'alias_removals': removals,
                                'unresolved_alias_conflicts': conflicts}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ('current', 'manuscript', 'plan', 'output'):
        parser.add_argument('--' + arg, required=True)
    args = parser.parse_args()
    paths = {key: Path(value).expanduser().resolve() for key, value in vars(args).items()}
    output = paths['output']
    provenance_path = Path(str(output) + '.provenance.tsv')
    manifest_path = Path(str(output) + '.manifest.json')
    for target in (output, provenance_path, manifest_path):
        if target.exists() or target in [paths[k] for k in ('current', 'manuscript', 'plan')]:
            raise ValueError(f'Refusing to overwrite {target}')
    sources = {key: {'path': str(paths[key]), 'sha256': sha256(paths[key])}
               for key in ('current', 'manuscript', 'plan')}
    current = pd.read_csv(paths['current'], sep='\t', dtype=str, keep_default_na=False)
    manuscript = pd.read_csv(paths['manuscript'], sep='\t', dtype=str, keep_default_na=False)
    plan = json.loads(paths['plan'].read_text())
    result, provenance, details = supplement(current, manuscript, plan)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=output.parent, prefix=output.name + '.', suffix='.part')
    try:
        with os.fdopen(fd, 'w', newline='') as handle:
            result.to_csv(handle, sep='\t', index=False)
        # No original inputs are changed. Publish manifest last as completion record.
        provenance.to_csv(provenance_path, sep='\t', index=False)
        os.rename(temporary, output)
        report = {'created_at_utc': datetime.now(timezone.utc).isoformat(),
                  'classification': 'manuscript-supplemented; not a fresh HQ certification',
                  'sources': sources, 'plan': plan, 'current_rows': len(current),
                  'output_rows': len(result), 'output_unique_ids': result.CHEMBLID.map(identifier_or_empty).replace('', pd.NA).nunique(),
                  'output_sha256': sha256(output), **details}
        manifest_path.write_text(json.dumps(report, indent=2) + '\n')
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    print(f'Created {output}: {len(result)} rows; {report["output_unique_ids"]} unique IDs')
    print(f'Unresolved aliases (not remapped by step 2): {len(details["unresolved_alias_conflicts"])}')


if __name__ == '__main__':
    main()
