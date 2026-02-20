"""Unit tests for lineage helpers."""

from __future__ import annotations

from typing import Any

from sift_gateway.mcp.lineage import (
    build_lineage_root_catalog,
    compute_related_set_hash,
    resolve_related_artifacts,
)


class _FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeConnection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.query: str | None = None
        self.params: tuple[Any, ...] | None = None

    def execute(self, query: str, params: tuple[Any, ...]) -> _FakeCursor:
        self.query = query
        self.params = params
        return _FakeCursor(self.rows)


def test_resolve_related_artifacts_maps_rows_and_binds_params() -> None:
    conn = _FakeConnection(
        [
            ("art_root", None, 0, 10, 1, "full", "ready"),
            ("art_child", "art_root", 1, 11, 1, "full", "ready"),
        ]
    )
    rows = resolve_related_artifacts(
        conn,
        session_id="sess_1",
        anchor_artifact_id="art_root",
    )
    assert [row["artifact_id"] for row in rows] == ["art_root", "art_child"]
    assert conn.query is not None
    assert "WITH RECURSIVE" in conn.query
    assert "related(artifact_id)" in conn.query
    assert "child.chain_seq IS NOT NULL" in conn.query
    assert "artifact_lineage_edges" not in conn.query
    assert conn.params is not None
    assert len(conn.params) == 2
    assert conn.params[0] == "local"
    assert conn.params[1] == "art_root"


def test_compute_related_set_hash_is_order_independent() -> None:
    rows_a = [
        {"artifact_id": "a", "generation": 2},
        {"artifact_id": "b", "generation": 1},
    ]
    rows_b = [
        {"artifact_id": "b", "generation": 1},
        {"artifact_id": "a", "generation": 2},
    ]
    assert compute_related_set_hash(rows_a) == compute_related_set_hash(rows_b)


def test_build_lineage_root_catalog_marks_compatibility_by_schema() -> None:
    entries = [
        {
            "artifact_id": "art_1",
            "root_path": "$.items",
            "root_shape": "array",
            "count_estimate": 2,
            "schema_hash": "sha256:" + ("a" * 64),
            "schema_mode": "exact",
            "schema_completeness": "complete",
        },
        {
            "artifact_id": "art_2",
            "root_path": "$.items",
            "root_shape": "array",
            "count_estimate": 100,
            "schema_hash": "sha256:" + ("a" * 64),
            "schema_mode": "exact",
            "schema_completeness": "complete",
        },
        {
            "artifact_id": "art_3",
            "root_path": "$.items",
            "root_shape": "array",
            "count_estimate": 1,
            "schema_hash": "sha256:" + ("b" * 64),
            "schema_mode": "exact",
            "schema_completeness": "complete",
        },
    ]
    roots = build_lineage_root_catalog(entries)
    assert len(roots) == 1
    assert roots[0]["root_path"] == "$.items"
    assert roots[0]["compatible_for_select"] is False
    assert len(roots[0]["signature_groups"]) == 2
