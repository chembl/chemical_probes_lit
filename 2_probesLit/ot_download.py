"""Download flat Open Targets parquet listings over HTTPS with atomic files."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, unquote
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.hrefs.extend(value for key, value in attrs if key == 'href' and value)


def make_session():
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
    return session


def list_parquet_files(session, url):
    """Fail early on missing datasets, nested-only layouts, or error pages."""
    with session.get(url, timeout=(20, 120)) as response:
        response.raise_for_status()
        parser = Links()
        parser.feed(response.text)
    files = {}
    base = urlsplit(url)
    for href in parser.hrefs:
        parsed = urlsplit(urljoin(url, href))
        name = unquote(parsed.path.rsplit('/', 1)[-1])
        if (parsed.scheme == base.scheme and parsed.netloc == base.netloc
                and parsed.path.rsplit('/', 1)[0] == base.path.rstrip('/')
                and not parsed.query and not parsed.fragment
                and '/' not in name and '\\' not in name
                and name.endswith('.parquet')):
            files[name] = parsed.geturl()
    if not files:
        raise ValueError(f'No directly listed parquet files at {url}; check the release and directory layout.')
    return sorted(files.items())


def download_files(session, files, destination):
    """Restart interrupted files; never expose an incomplete file as parquet."""
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for index, (name, url) in enumerate(files, 1):
        output = destination / name
        partial = destination / (name + '.part')
        print(f'[{index}/{len(files)}] {url}', flush=True)
        for attempt in range(3):
            try:
                with session.get(url, stream=True, timeout=(20, 120)) as response:
                    response.raise_for_status()
                    size = 0
                    with partial.open('wb') as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            handle.write(chunk)
                            size += len(chunk)
                    expected = response.headers.get('Content-Length')
                    if expected is not None and not response.headers.get('Content-Encoding') and size != int(expected):
                        raise requests.RequestException(f'Incomplete download: {url}')
                partial.replace(output)
                break
            except requests.RequestException:
                partial.unlink(missing_ok=True)
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
