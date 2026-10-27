"""Download Open Targets inputs over HTTPS into an external directory."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from ot_download import make_session, list_parquet_files, download_files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, help='Destination for five dataset directories; use a fresh snapshot.')
    parser.add_argument('--release', default='26.09', help='Open Targets output release (default: 26.09).')
    parser.add_argument('--literature-release', default='latest', help='Literature release (default: latest; pin when available).')
    parser.add_argument('--literature-dir', '--literature-ftp-dir', dest='literature_dir',
                        help='Override literature server path (not URL); transferred over HTTPS.')
    parser.add_argument('--check-only', action='store_true', help='Validate all directory listings without downloading files.')
    args = parser.parse_args()
    destination = Path(args.output_dir).expanduser().resolve()
    base = 'pub/databases/opentargets/platform'
    paths = {'literature_match': args.literature_dir or f'{base}/{args.literature_release}/intermediate/literature/match'}
    for name in ('association_by_datasource_indirect', 'disease', 'clinical_target', 'clinical_indication'):
        paths[name] = f'{base}/{args.release}/output/{name}'
    sources = {name: f'https://ftp.ebi.ac.uk/{path.strip("/")}/' for name, path in paths.items()}
    with make_session() as session:
        # Check every dataset before spending hours downloading literature.
        listings = {}
        for name, url in sources.items():
            listings[name] = list_parquet_files(session, url)
            print(f'Validated {name}: {len(listings[name])} parquet files at {url}', flush=True)
        if args.check_only:
            return
        destination.mkdir(parents=True, exist_ok=True)
        manifest_path = destination / 'download_manifest.json'
        manifest_path.unlink(missing_ok=True)
        for name, files in listings.items():
            # Stale parquet files would silently contaminate Spark reads.
            expected = {filename for filename, _ in files}
            existing = {p.name for p in (destination / name).glob('*.parquet')}
            if existing - expected:
                raise ValueError(f'Stale parquet files in {destination / name}; choose a fresh snapshot directory.')
        for name, files in listings.items():
            download_files(session, files, destination / name)
        manifest = {
            'completed_at_utc': datetime.now(timezone.utc).isoformat(),
            'release': args.release, 'literature_release': args.literature_release,
            'sources': sources,
            'files': {name: [filename for filename, _ in files] for name, files in listings.items()},
        }
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
