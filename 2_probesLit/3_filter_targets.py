"""Filter triplets to accepted probe targets; fail on unresolved Ensembl requests."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import quote

import pandas as pd
import requests
from helpers.pipeline_paths import output_path

ENSEMBL_SERVER = 'https://rest.ensembl.org'


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=path.name + '.', suffix='.part')
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(value, handle, indent=2)
            handle.write('\n')
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def lookup_gene(session, symbol, attempts=5):
    url = f'{ENSEMBL_SERVER}/lookup/symbol/human/{quote(symbol, safe="")}'
    last_error = None
    for attempt in range(attempts):
        delay = min(2 ** (attempt + 1), 60)
        try:
            response = session.get(url, headers={'Accept': 'application/json'}, timeout=(20, 60))
            try:
                status = response.status_code
                if status == 200:
                    try:
                        data = response.json()
                    except ValueError as error:
                        raise requests.RequestException('Invalid Ensembl JSON response') from error
                    identifier = data.get('id') if isinstance(data, dict) else None
                    if isinstance(identifier, str) and re.fullmatch(r'ENSG\d+', identifier):
                        return identifier
                    raise requests.RequestException(f'Invalid human gene ID for {symbol}')
                # Only an explicit symbol-not-found response is a biological miss.
                if status in (400, 404):
                    try:
                        payload = response.json()
                        message = payload.get('error', '') if isinstance(payload, dict) else ''
                    except ValueError:
                        message = ''
                    if isinstance(message, str) and 'No valid lookup found for symbol' in message:
                        return None
                if status != 429 and not 500 <= status < 600:
                    raise RuntimeError(f'Ensembl lookup for {symbol}: HTTP {status}; refusing incomplete target mapping')
                try:
                    delay = min(max(delay, float(response.headers.get('Retry-After', ''))), 120)
                except (ValueError, TypeError):
                    pass
                last_error = f'HTTP {status}'
            finally:
                response.close()
        except requests.RequestException as error:
            last_error = str(error)
        if attempt + 1 < attempts:
            print(f'Ensembl {symbol}: {last_error}; retry {attempt + 2}/{attempts} in {delay:g}s', flush=True)
            time.sleep(delay)
    raise RuntimeError(f'Ensembl lookup failed for {symbol} after {attempts} attempts: {last_error}. '
                       'Step 3 stopped before replacing its output. Retry when the service is available; successful mappings are cached.')


def map_gene_ids_to_ensembl(gene_ids, cache_path, session):
    cache_path = Path(cache_path)
    cache = {'version': 1, 'server': ENSEMBL_SERVER, 'species': 'human', 'genes': {}}
    if cache_path.exists():
        cache = json.loads(cache_path.read_text())
        if (cache.get('version'), cache.get('server'), cache.get('species')) != (1, ENSEMBL_SERVER, 'human'):
            raise ValueError('Incompatible Ensembl cache; select a new --gene-cache file')
        if not isinstance(cache.get('genes'), dict) or any(
            not isinstance(entry, dict) or not isinstance(entry.get('id'), str)
            or not re.fullmatch(r'ENSG\d+', entry['id']) for entry in cache['genes'].values()
        ):
            raise ValueError('Invalid Ensembl cache; refusing partial mappings')
    # Preserve the existing query and exact GENE-to-ID matching policy.
    symbols = sorted({part.strip() for value in gene_ids if pd.notna(value)
                      for part in str(value).split(',') if part.strip()})
    mappings, not_found = {}, []
    for symbol in symbols:
        if symbol in cache['genes']:
            mappings[symbol] = cache['genes'][symbol]['id']
            continue
        identifier = lookup_gene(session, symbol)
        if identifier is None:
            not_found.append(symbol)
            continue
        mappings[symbol] = identifier
        cache['genes'][symbol] = {'id': identifier, 'retrieved_at_utc': datetime.now(timezone.utc).isoformat()}
        atomic_json(cache_path, cache)
        time.sleep(0.1)
    return mappings, not_found, len(symbols)


def main(input_path, probes_path, output, gene_cache=None):
    output = output_path(output)
    hqprobes = pd.read_csv(probes_path, sep='\t')
    cache_path = Path(gene_cache) if gene_cache else Path(output).parent / 'ensembl_gene_mappings.json'
    # Complete all external lookups before reading the large triplet file.
    with requests.Session() as session:
        mappings, not_found, requested = map_gene_ids_to_ensembl(hqprobes.GENE.unique(), cache_path, session)
    print(f'Ensembl symbols: {requested}; mapped: {len(mappings)}; confirmed not found: {len(not_found)}', flush=True)
    hqprobes['ENS_ID'] = hqprobes.GENE.map(mappings)
    usable = hqprobes.dropna(subset=['CHEMBLID', 'ENS_ID'])
    if usable.empty:
        raise RuntimeError('No valid probe-target mappings; output was not replaced')
    unmapped = hqprobes.loc[hqprobes.ENS_ID.isna(), 'GENE'].dropna().drop_duplicates().tolist()
    print('Unmapped dictionary gene values:', unmapped, flush=True)
    ner_data = pd.read_csv(input_path, sep='\t')
    subset = ner_data.merge(usable, left_on=['CD', 'GP'], right_on=['CHEMBLID', 'ENS_ID'], how='inner')
    subset = subset.drop(columns=['CHEMBLID', 'ENS_ID'])
    if subset.empty:
        raise RuntimeError('Target filtering produced zero rows; output was not replaced')
    fd, temporary = tempfile.mkstemp(dir=Path(output).parent, prefix=Path(output).name + '.', suffix='.part')
    try:
        with os.fdopen(fd, 'w', newline='') as handle:
            subset.to_csv(handle, sep='\t', index=False)
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    atomic_json(output + '.mapping_report.json', {
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'gene_cache': str(cache_path.resolve()), 'server': ENSEMBL_SERVER,
        'requested_symbols': requested, 'mapped_symbols': len(mappings),
        'confirmed_not_found': not_found, 'unmapped_dictionary_gene_values': unmapped,
        'dictionary_rows': len(hqprobes), 'mapped_dictionary_rows': len(usable),
        'input_rows': len(ner_data), 'output_rows': len(subset)})
    print(f'Target filtering: {len(ner_data):,} input rows -> {len(subset):,} output rows', flush=True)
    print(f'Wrote {output}; mapping report: {output}.mapping_report.json', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Step 2 triplet TSV.')
    parser.add_argument('--probes', required=True, help='HQ probes dictionary TSV.')
    parser.add_argument('--output', required=True, help='Destination TSV.')
    parser.add_argument('--gene-cache', help='Successful mappings JSON; default: ensembl_gene_mappings.json beside output. Reuse for retries; use a fresh cache for a new analysis.')
    args = parser.parse_args()
    main(args.input, args.probes, args.output, args.gene_cache)
