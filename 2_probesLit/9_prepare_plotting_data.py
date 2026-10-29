"""Stream a row-preserving, column-reduced plotting TSV and optional triplet counts."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
import pandas as pd
from plotting_data import PLOT_COLUMNS


def triplet_summary(path, chunk_size, scratch):
    """Disk-backed exact distinct counts for the notebook's diagnostic cell."""
    with tempfile.TemporaryDirectory(dir=scratch, prefix='triplet-counts-') as directory:
        db = sqlite3.connect(str(Path(directory) / 'counts.sqlite'))
        try:
            columns = pd.read_csv(path, sep='\t', nrows=0).columns.tolist()
            for index in range(len(columns)):
                db.execute(f'CREATE TABLE c{index} (value TEXT PRIMARY KEY) WITHOUT ROWID')
            rows = 0
            with pd.read_csv(path, sep='\t', dtype=str, chunksize=chunk_size) as reader:
                for frame in reader:
                    rows += len(frame)
                    for index, column in enumerate(columns):
                        db.executemany(f'INSERT OR IGNORE INTO c{index} VALUES (?)',
                                       [(v,) for v in frame[column].dropna().unique()])
                    db.commit()
            return {'rows': rows, 'nunique': {
                column: db.execute(f'SELECT count(*) FROM c{i}').fetchone()[0]
                for i, column in enumerate(columns)}}
        finally:
            db.close()


def prepare(source, output, chunk_size=10000, triplets=None):
    source, output = Path(source).expanduser().resolve(), Path(output).expanduser().resolve()
    if source == output:
        raise ValueError('Plotting output must differ from the source')
    if chunk_size < 1:
        raise ValueError('Chunk size must be positive')
    if not str(output).endswith('.tsv.gz'):
        raise ValueError('Use a .tsv.gz output filename')
    output.parent.mkdir(parents=True, exist_ok=True)
    before = source.stat()
    columns = pd.read_csv(source, sep='\t', nrows=0).columns.tolist()
    missing = set(PLOT_COLUMNS) - set(columns)
    if missing:
        raise ValueError(f'Missing plotting columns: {sorted(missing)}')
    report_path = Path(str(output) + '.manifest.json')
    report_path.unlink(missing_ok=True)
    with tempfile.NamedTemporaryFile(dir=output.parent, suffix='.part', delete=False) as handle:
        temporary = Path(handle.name)
    rows = 0
    classifications = Counter()
    coverage = Counter()
    try:
        with gzip.open(temporary, 'wt', encoding='utf-8', newline='', compresslevel=1) as destination:
            with pd.read_csv(source, sep='\t', usecols=PLOT_COLUMNS, dtype=str,
                             keep_default_na=False, chunksize=chunk_size) as reader:
                first = True
                for frame in reader:
                    frame = frame[PLOT_COLUMNS]
                    frame.to_csv(destination, sep='\t', index=False, header=first)
                    first = False
                    rows += len(frame)
                    classifications.update(frame.articleType.replace('', 'unknown'))
                    coverage.update(frame.datedEvidenceStatus.replace('', 'unknown'))
                    print(f'Prepared {rows:,} plotting rows', flush=True)
        after = source.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError('Source changed during preparation; rerun with an immutable input')
        summary = triplet_summary(triplets, chunk_size, output.parent) if triplets else None
        temporary.replace(output)
        report = {
            'created_at_utc': datetime.now(timezone.utc).isoformat(),
            'source': str(source), 'source_bytes': before.st_size,
            'source_mtime_ns': before.st_mtime_ns, 'output_bytes': output.stat().st_size,
            'rows': rows, 'columns': PLOT_COLUMNS,
            'omitted_columns': [c for c in columns if c not in PLOT_COLUMNS],
            'row_policy': 'All rows, original order, no deduplication or sampling',
            'article_type_row_counts': dict(classifications),
            'dated_evidence_status_row_counts': dict(coverage),
            'triplets_source': str(Path(triplets).resolve()) if triplets else None,
            'triplet_summary': summary,
        }
        report_path.write_text(json.dumps(report, indent=2) + '\n')
        print(f'Complete: {rows:,} rows; {before.st_size:,} source bytes -> {output.stat().st_size:,} compressed bytes', flush=True)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='Step 8 TSV.')
    parser.add_argument('--output', required=True, help='New plotting .tsv.gz file.')
    parser.add_argument('--chunk-size', type=int, default=10000)
    parser.add_argument('--triplets', help='Optional step 2 TSV for exact diagnostic counts; adds a full scan and disk-backed distinct counting.')
    args = parser.parse_args()
    prepare(args.input, args.output, args.chunk_size, args.triplets)
