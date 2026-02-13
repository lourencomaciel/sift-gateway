"""Tests for sift_mcp.db package import behavior."""

from __future__ import annotations

import importlib
import sys


def test_db_package_import_does_not_eager_import_conn() -> None:
    """Importing ``sift_mcp.db`` should not require Postgres extras."""
    sys.modules.pop("sift_mcp.db", None)
    sys.modules.pop("sift_mcp.db.conn", None)

    module = importlib.import_module("sift_mcp.db")

    assert hasattr(module, "apply_migrations")
    assert "sift_mcp.db.conn" not in sys.modules
