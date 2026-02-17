from __future__ import annotations

from unittest.mock import MagicMock

from sift_mcp.mcp.handlers.mirrored_tool import (
    _fetch_inline_describe,
    _minimal_describe,
)

# ---------------------------------------------------------------------------
# _minimal_describe
# ---------------------------------------------------------------------------


def test_minimal_describe_returns_tuple() -> None:
    desc, hint = _minimal_describe("art_test")
    assert isinstance(desc, dict)
    assert isinstance(hint, str)


def test_minimal_describe_has_artifact_id() -> None:
    desc, _ = _minimal_describe("art_abc")
    assert desc["artifact_id"] == "art_abc"


def test_minimal_describe_has_pending_status() -> None:
    desc, _ = _minimal_describe("art_xyz")
    assert desc["mapping"]["map_status"] == "pending"
    assert desc["mapping"]["map_kind"] == "none"


def test_minimal_describe_has_empty_roots() -> None:
    desc, _ = _minimal_describe("art_xyz")
    assert desc["roots"] == []


def test_minimal_describe_hint_mentions_mapping() -> None:
    _, hint = _minimal_describe("art_1")
    assert "Mapping in progress" in hint


def test_minimal_describe_hint_mentions_artifact_id() -> None:
    _, hint = _minimal_describe("art_custom_id")
    assert "art_custom_id" in hint


# ---------------------------------------------------------------------------
# _fetch_inline_describe — happy path with mock connection
# ---------------------------------------------------------------------------


def _mock_connection(
    artifact_row: tuple | None = None,
    schema_root_rows: list | None = None,
    schema_field_rows_by_root_key: dict[str, list[tuple]] | None = None,
) -> MagicMock:
    """Build a mock connection returning canned query results."""
    conn = MagicMock()

    def _execute(sql, params):
        cursor = MagicMock()
        if "FROM artifacts" in sql:
            cursor.fetchone.return_value = artifact_row
        elif "FROM artifact_schema_roots" in sql:
            cursor.fetchall.return_value = schema_root_rows or []
        elif "FROM artifact_schema_fields" in sql:
            root_key = params[2] if params and len(params) >= 3 else ""
            rows_by_key = schema_field_rows_by_root_key or {}
            cursor.fetchall.return_value = rows_by_key.get(str(root_key), [])
        else:
            cursor.fetchone.return_value = None
            cursor.fetchall.return_value = []
        return cursor

    conn.execute.side_effect = _execute
    return conn


def test_fetch_inline_describe_happy_path() -> None:
    artifact_row = (
        "art_1",  # artifact_id
        "full",  # map_kind
        "complete",  # map_status
        "v1",  # mapper_version
        None,  # map_budget_fingerprint
        None,  # map_backend_id
        None,  # prng_version
        0,  # mapped_part_index
        None,  # deleted_at
        1,  # generation
    )
    schema_root_rows = [
        (
            "rk1",
            "$.data",
            "schema_v1",
            "sha256:abc",
            "exact",
            "complete",
            10,
            "sha256:def",
            "traversal_v1",
            None,
        )
    ]
    schema_fields = {
        "rk1": [
            ("$.name", ["string"], False, True, 10, "alice"),
        ]
    }
    conn = _mock_connection(
        artifact_row=artifact_row,
        schema_root_rows=schema_root_rows,
        schema_field_rows_by_root_key=schema_fields,
    )
    desc, hint = _fetch_inline_describe(conn, "art_1")
    assert desc["artifact_id"] == "art_1"
    assert desc["mapping"]["map_kind"] == "full"
    assert desc["mapping"]["map_status"] == "complete"
    assert desc["roots"] == []
    assert len(desc["schemas"]) == 1
    assert desc["schemas"][0]["root_path"] == "$.data"
    assert "10 records" in hint
    assert 'artifact(action="query"' in hint


def test_fetch_inline_describe_no_artifact_row() -> None:
    conn = _mock_connection(artifact_row=None)
    desc, _hint = _fetch_inline_describe(conn, "art_missing")
    assert desc["artifact_id"] == "art_missing"
    assert desc["mapping"]["map_kind"] == "none"
    assert desc["roots"] == []


def test_fetch_inline_describe_db_error_falls_back() -> None:
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("DB gone")
    desc, hint = _fetch_inline_describe(conn, "art_err")
    assert desc["artifact_id"] == "art_err"
    assert desc["mapping"]["map_status"] == "pending"
    assert desc["roots"] == []
    assert "Mapping in progress" in hint


def test_fetch_inline_describe_cache_hit_with_schema_paths() -> None:
    artifact_row = (
        "art_cached",
        "full",
        "complete",
        "v1",
        None,
        None,
        None,
        0,
        None,
        2,
    )
    schema_root_rows = [
        (
            "rk1",
            "$.result.data",
            "schema_v1",
            "sha256:one",
            "exact",
            "complete",
            100,
            "sha256:dataset_one",
            "traversal_v1",
            None,
        ),
        (
            "rk2",
            "$.result.paging",
            "schema_v1",
            "sha256:two",
            "exact",
            "complete",
            1,
            "sha256:dataset_two",
            "traversal_v1",
            None,
        ),
    ]
    schema_fields = {
        "rk1": [("$.id", ["string"], False, True, 100, "1")],
        "rk2": [("$.next", ["string"], False, False, 1, "https://...")],
    }
    conn = _mock_connection(
        artifact_row=artifact_row,
        schema_root_rows=schema_root_rows,
        schema_field_rows_by_root_key=schema_fields,
    )
    desc, hint = _fetch_inline_describe(conn, "art_cached")
    assert desc["roots"] == []
    assert len(desc["schemas"]) == 1
    assert desc["schemas"][0]["root_path"] == "$.result.data"
    assert "100 records" in hint
    assert "Also available" not in hint


def test_fetch_inline_describe_keeps_all_schemas_when_primary_not_unique() -> (
    None
):
    artifact_row = (
        "art_tie",
        "full",
        "complete",
        "v1",
        None,
        None,
        None,
        0,
        None,
        2,
    )
    schema_root_rows = [
        (
            "rk1",
            "$.result.a",
            "schema_v1",
            "sha256:one",
            "exact",
            "complete",
            100,
            "sha256:dataset_one",
            "traversal_v1",
            None,
        ),
        (
            "rk2",
            "$.result.b",
            "schema_v1",
            "sha256:two",
            "exact",
            "complete",
            100,
            "sha256:dataset_two",
            "traversal_v1",
            None,
        ),
    ]
    schema_fields = {
        "rk1": [("$.id", ["string"], False, True, 100, "1")],
        "rk2": [("$.name", ["string"], False, True, 100, "n")],
    }
    conn = _mock_connection(
        artifact_row=artifact_row,
        schema_root_rows=schema_root_rows,
        schema_field_rows_by_root_key=schema_fields,
    )
    desc, hint = _fetch_inline_describe(conn, "art_tie")
    assert len(desc["schemas"]) == 2
    root_paths = {schema["root_path"] for schema in desc["schemas"]}
    assert root_paths == {"$.result.a", "$.result.b"}
    assert "Also available" in hint


def test_fetch_inline_describe_includes_schemas() -> None:
    artifact_row = (
        "art_schema",
        "full",
        "complete",
        "v1",
        None,
        None,
        None,
        0,
        None,
        2,
    )
    schema_root_rows = [
        (
            "rk1",
            "$.result.data",
            "schema_v1",
            "sha256:abc",
            "exact",
            "complete",
            2,
            "sha256:def",
            "traversal_v1",
            None,
        )
    ]
    schema_fields = {
        "rk1": [
            ("$.id", ["number"], False, True, 2, "1"),
        ]
    }
    conn = _mock_connection(
        artifact_row=artifact_row,
        schema_root_rows=schema_root_rows,
        schema_field_rows_by_root_key=schema_fields,
    )
    desc, _ = _fetch_inline_describe(conn, "art_schema")
    assert len(desc["schemas"]) == 1
    assert desc["schemas"][0]["root_path"] == "$.result.data"
    assert desc["roots"] == []
