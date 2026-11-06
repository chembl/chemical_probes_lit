"""Annotate a large TSV in chunks, with resumable SQLite PubMed lookups."""
import argparse
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import time
import pandas as pd
import requests

URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'


def retry_after_seconds(value):
    """Parse Retry-After seconds or HTTP date; ignore malformed headers."""
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        try:
            return max(0.0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def fetch_batch(session, pmids):
    """Return successful classifications only; absent/invalid records stay unknown."""
    for attempt in range(5):
        try:
            response = session.post(URL, data={
                'db': 'pubmed', 'id': ','.join(pmids), 'retmode': 'json',
                'version': '2.0', 'tool': 'chemical_probes_lit',
            }, timeout=(20, 120))
            response.raise_for_status()
            result = response.json().get('result')
            if not isinstance(result, dict):
                raise ValueError('PubMed response has no result object')
            labels = {}
            for pmid in pmids:
                record = result.get(pmid)
                if not isinstance(record, dict) or record.get('error'):
                    continue
                types = record.get('pubtype')
                if not isinstance(types, list) or not types or not all(isinstance(t, str) for t in types):
                    continue
                labels[pmid] = 'review' if 'Review' in types else 'primary'
            return labels
        except (requests.RequestException, ValueError) as error:
            print(f'PubMed attempt {attempt + 1}/5 failed: {error}', flush=True)
            response = getattr(error, 'response', None)
            if response is not None and response.status_code == 429:
                delay = max(30 * (2 ** attempt), retry_after_seconds(response.headers.get('Retry-After')))
                print(f'Rate limited; waiting {delay:.0f} seconds before another request', flush=True)
                # Also wait on the last attempt to avoid hammering the next batch.
                time.sleep(delay)
            elif attempt < 4:
                time.sleep(2 ** attempt)
        finally:
            # Conservative pacing; shared cluster egress can still be rate limited.
            time.sleep(1.1)
    return {}


def chunks(path, size, **kwargs):
    with pd.read_csv(path, sep='\t', dtype=str, keep_default_na=False,
                     chunksize=size, **kwargs) as reader:
        yield from reader


def annotate(input_path, output_path, cache_path, chunk_size=10000):
    source, output, cache = [Path(p).expanduser().resolve() for p in (input_path, output_path, cache_path)]
    if len({source, output, cache}) != 3:
        raise ValueError('Input, output and cache must be different paths')
    if chunk_size < 1:
        raise ValueError('Chunk size must be positive')
    output.parent.mkdir(parents=True, exist_ok=True)
    cache.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(cache)
    temporary = None
    try:
        connection.execute('CREATE TABLE IF NOT EXISTS classifications (pmid TEXT PRIMARY KEY, article_type TEXT, checked_at TEXT)')
        connection.execute('PRAGMA temp_store=FILE')
        connection.execute('CREATE TEMP TABLE run_pmids (pmid TEXT PRIMARY KEY)')
        rows = 0
        # First pass parses only PMID into memory; distinct identifiers live on disk.
        for frame in chunks(source, chunk_size, usecols=['pmid']):
            ids = [(p,) for p in frame.pmid.unique() if p.isascii() and p.isdigit() and int(p) > 0]
            connection.executemany('INSERT OR IGNORE INTO run_pmids VALUES (?)', ids)
            connection.executemany('INSERT OR IGNORE INTO classifications(pmid) VALUES (?)', ids)
            connection.commit()
            rows += len(frame)
            if rows % (chunk_size * 100) == 0:
                print(f'Scanned {rows:,} input rows', flush=True)
        connection.execute('CREATE TEMP TABLE pending AS SELECT r.pmid FROM run_pmids r JOIN classifications c USING(pmid) WHERE c.article_type IS NULL')
        total = connection.execute('SELECT count(*) FROM run_pmids').fetchone()[0]
        pending = connection.execute('SELECT count(*) FROM pending').fetchone()[0]
        print(f'{rows:,} rows; {total:,} unique valid PMIDs; {pending:,} need lookup', flush=True)
        cursor = connection.execute('SELECT pmid FROM pending ORDER BY pmid')
        processed = 0
        with requests.Session() as session:
            while batch := cursor.fetchmany(200):
                ids = [r[0] for r in batch]
                labels = fetch_batch(session, ids)
                timestamp = datetime.now(timezone.utc).isoformat()
                connection.executemany('UPDATE classifications SET article_type=?, checked_at=? WHERE pmid=?',
                                       [(labels.get(p), timestamp, p) for p in ids])
                connection.commit()
                processed += len(ids)
                print(f'Looked up {processed:,}/{pending:,}; unresolved in batch: {len(ids) - len(labels)}', flush=True)
        unresolved = connection.execute('SELECT count(*) FROM run_pmids r JOIN classifications c USING(pmid) WHERE c.article_type IS NULL').fetchone()[0]
        # Second pass preserves field text (including NA literals, IDs and backslashes).
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', newline='',
                                         dir=output.parent, prefix=output.name + '.', suffix='.part', delete=False) as handle:
            temporary = Path(handle.name)
            first = True
            written = unknown_rows = 0
            for frame in chunks(source, chunk_size):
                ids = list(frame.pmid.unique())
                labels = {}
                for offset in range(0, len(ids), 900):
                    batch = ids[offset:offset + 900]
                    placeholders = ','.join('?' for _ in batch)
                    labels.update(connection.execute(f'SELECT pmid, article_type FROM classifications WHERE pmid IN ({placeholders})', batch))
                frame['articleType'] = frame.pmid.map(labels).fillna('')
                unknown_rows += (frame.articleType == '').sum()
                frame.to_csv(handle, sep='\t', index=False, header=first)
                first = False
                written += len(frame)
                print(f'Wrote {written:,}/{rows:,} rows', flush=True)
            if written != rows:
                raise ValueError('Input row count changed between passes; keep the input unchanged')
        temporary.replace(output)
        temporary = None
        print(f'Completed: {written:,} rows; {unresolved:,} unresolved valid PMIDs; {unknown_rows:,} rows without articleType. Cache: {cache}', flush=True)
    finally:
        connection.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Step 7 TSV file.')
    parser.add_argument('--output', required=True, help='Separate annotated TSV destination.')
    parser.add_argument('--cache', help='SQLite cache; defaults to OUTPUT.pubmed.sqlite. Successful lookups are reused.')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Rows per chunk (default: 10000). Reduce for very wide rows.')
    args = parser.parse_args()
    annotate(args.input, args.output, args.cache or args.output + '.pubmed.sqlite', args.chunk_size)
