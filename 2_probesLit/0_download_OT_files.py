"""Download Open Targets inputs into an explicitly selected external directory."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, help='Destination for the four dataset directories.')
    parser.add_argument('--release', default='25.09', help='Open Targets output release (default: 25.09).')
    parser.add_argument('--literature-release', default='latest', help='Literature release (default: latest; pin when available).')
    parser.add_argument('--literature-ftp-dir', help='Override the complete literature FTP directory if its layout differs by release.')
    args = parser.parse_args()

    from tools import download_files_from_ftp

    destination = Path(args.output_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    base = 'pub/databases/opentargets/platform'
    sources = {
        'literature_match': args.literature_ftp_dir or f'{base}/{args.literature_release}/intermediate/literature/match/',
        'association_by_datasource_indirect': f'{base}/{args.release}/output/association_by_datasource_indirect',
        'disease': f'{base}/{args.release}/output/disease',
        'known_drug': f'{base}/{args.release}/output/known_drug',
    }
    for name, remote in sources.items():
        print(f'Downloading {remote} to {destination / name}', flush=True)
        download_files_from_ftp(remote, str(destination / name))
    manifest = {
        'completed_at_utc': datetime.now(timezone.utc).isoformat(),
        'release': args.release,
        'literature_release': args.literature_release,
        'sources': {name: f'ftp://ftp.ebi.ac.uk/{path.lstrip("/")}' for name, path in sources.items()},
    }
    (destination / 'download_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
