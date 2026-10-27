"""Filesystem helpers shared by the command-line pipeline steps."""
from pathlib import Path


def output_path(value):
    """Create the parent directory and return an absolute local output filename."""
    path = Path(value).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)
