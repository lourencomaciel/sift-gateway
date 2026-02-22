#!/usr/bin/env python3
"""Download all Tier 1 benchmark datasets to the data directory."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any
import urllib.error
import urllib.request

# Allow running as `python benchmarks/tier1/fetch_data.py` without
# manually setting PYTHONPATH.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from benchmarks.tier1.datasets import DATASETS, Dataset


def fetch_dataset(
    dataset: Dataset,
    *,
    data_dir: Path,
) -> int:
    """Fetch one dataset and write extracted array to disk.

    Returns the file size in bytes.
    """
    dest = data_dir / dataset.local_filename
    print(f"  Fetching {dataset.name} from {dataset.url} ...")

    request = urllib.request.Request(
        dataset.url,
        headers={"User-Agent": "sift-benchmark/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
    except urllib.error.URLError as exc:
        print(f"    FAILED: {exc}", file=sys.stderr)
        return 0

    parsed: Any = json.loads(raw.decode("utf-8", errors="replace"))

    if dataset.extraction_path is not None and isinstance(parsed, dict):
        extracted = parsed.get(dataset.extraction_path)
        if extracted is None:
            print(
                f"    WARN: extraction path "
                f"'{dataset.extraction_path}' not found, "
                f"saving full response",
                file=sys.stderr,
            )
        else:
            parsed = extracted

    out_bytes = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
    dest.write_bytes(out_bytes)
    size = len(out_bytes)
    print(f"    Saved {dest.name}: {size:,} bytes")
    return size


def fetch_all(data_dir: Path) -> None:
    """Fetch all datasets."""
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {len(DATASETS)} datasets to {data_dir}/\n")
    total = 0
    for dataset in DATASETS.values():
        total += fetch_dataset(dataset, data_dir=data_dir)
    print(f"\nTotal: {total:,} bytes across {len(DATASETS)} datasets")


def main() -> int:
    """CLI entrypoint."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Tier 1 benchmark datasets",
    )
    parser.add_argument(
        "--data-dir",
        default=str(Path(__file__).resolve().parent / "data"),
        help="Directory to save datasets (default: benchmarks/tier1/data)",
    )
    args = parser.parse_args()
    fetch_all(Path(args.data_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
