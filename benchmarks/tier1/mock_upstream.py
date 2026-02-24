#!/usr/bin/env python3
"""Mock upstream MCP server serving benchmark datasets.

Standalone FastMCP server launched by the gateway as a subprocess
via ``StdioTransport``.  Each of the 12 benchmark datasets is
exposed as a separate tool (e.g. ``get_earthquakes``).

The data directory is read from the ``BENCHMARK_DATA_DIR``
environment variable (set via ``UpstreamConfig.env``).  Datasets
are lazy-loaded on first call to avoid startup overhead during
tool discovery.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Dataset registry
# ---------------------------------------------------------------------------
# Maps tool-name suffix to local JSON filename.  Matches the
# 12 entries in ``datasets.py``.
_DATASET_FILES: dict[str, str] = {
    "earthquakes": "earthquakes.json",
    "products": "products.json",
    "users": "users.json",
    "comments": "comments.json",
    "photos": "photos.json",
    "countries": "countries.json",
    "laureates": "laureates.json",
    "weather": "weather.json",
    "github_repos": "github_repos.json",
    "pokemon": "pokemon.json",
    "openlibrary": "openlibrary.json",
    "airports": "airports.json",
}

_TOOL_DESCRIPTIONS: dict[str, str] = {
    "earthquakes": "Get earthquake data from USGS",
    "products": "Get product catalog",
    "users": "Get user profiles",
    "comments": "Get comments",
    "photos": "Get photo metadata",
    "countries": "Get country data",
    "laureates": "Get Nobel Prize laureates",
    "weather": "Get hourly weather observations",
    "github_repos": "Get popular GitHub repositories",
    "pokemon": "Get Pokemon data",
    "openlibrary": "Get Open Library works",
    "airports": "Get airport data",
}

# ---------------------------------------------------------------------------
# Lazy data loader
# ---------------------------------------------------------------------------
_cache: dict[str, Any] = {}


def _data_dir() -> Path:
    raw = os.environ.get("BENCHMARK_DATA_DIR", "")
    if not raw:
        msg = "BENCHMARK_DATA_DIR environment variable is required"
        raise RuntimeError(msg)
    return Path(raw)


def _load(filename: str) -> Any:
    if filename in _cache:
        return _cache[filename]
    path = _data_dir() / filename
    data = json.loads(path.read_text(encoding="utf-8"))
    _cache[filename] = data
    return data


# ---------------------------------------------------------------------------
# FastMCP application
# ---------------------------------------------------------------------------
app = FastMCP(name="benchmark-data-service")


def _register_dataset_tool(name: str, filename: str) -> None:
    """Register one dataset tool on the FastMCP app."""
    description = _TOOL_DESCRIPTIONS.get(name, f"Get {name} data")

    @app.tool(name=f"get_{name}", description=description)
    def _handler() -> Any:
        """Return the full dataset."""
        return _load(filename)


for _name, _filename in _DATASET_FILES.items():
    _register_dataset_tool(_name, _filename)


if __name__ == "__main__":
    app.run(transport="stdio")
