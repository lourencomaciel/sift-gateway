from __future__ import annotations

from unittest.mock import MagicMock

from sift_gateway.config.settings import PaginationConfig
from sift_gateway.envelope.normalize import normalize_envelope
from sift_gateway.mcp.handlers.mirrored_tool import (
    _detect_duplicate_page_warning,
    _fetch_inline_describe,
    _minimal_describe,
    _sanitize_envelope_payload,
)

# ---------------------------------------------------------------------------
# _minimal_describe
# ---------------------------------------------------------------------------


def test_minimal_describe_returns_dict() -> None:
    desc = _minimal_describe("art_test")
    assert isinstance(desc, dict)


def test_minimal_describe_has_artifact_id() -> None:
    desc = _minimal_describe("art_abc")
    assert desc["artifact_id"] == "art_abc"


def test_minimal_describe_has_pending_status() -> None:
    desc = _minimal_describe("art_xyz")
    assert desc["mapping"]["map_status"] == "pending"
    assert desc["mapping"]["map_kind"] == "none"


def test_minimal_describe_has_empty_roots() -> None:
    desc = _minimal_describe("art_xyz")
    assert desc["roots"] == []


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
    desc = _fetch_inline_describe(conn, "art_1")
    assert desc["artifact_id"] == "art_1"
    assert desc["mapping"]["map_kind"] == "full"
    assert desc["mapping"]["map_status"] == "complete"
    assert desc["roots"] == []
    assert len(desc["schemas"]) == 1
    assert desc["schemas"][0]["root_path"] == "$.data"


def test_fetch_inline_describe_no_artifact_row() -> None:
    conn = _mock_connection(artifact_row=None)
    desc = _fetch_inline_describe(conn, "art_missing")
    assert desc["artifact_id"] == "art_missing"
    assert desc["mapping"]["map_kind"] == "none"
    assert desc["roots"] == []


def test_fetch_inline_describe_db_error_falls_back() -> None:
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("DB gone")
    desc = _fetch_inline_describe(conn, "art_err")
    assert desc["artifact_id"] == "art_err"
    assert desc["mapping"]["map_status"] == "pending"
    assert desc["roots"] == []


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
    desc = _fetch_inline_describe(conn, "art_cached")
    assert desc["roots"] == []
    assert len(desc["schemas"]) == 2
    assert desc["schemas"][0]["root_path"] == "$.result.data"
    assert desc["schemas"][1]["root_path"] == "$.result.paging"


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
    desc = _fetch_inline_describe(conn, "art_tie")
    assert len(desc["schemas"]) == 2
    root_paths = {schema["root_path"] for schema in desc["schemas"]}
    assert root_paths == {"$.result.a", "$.result.b"}


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
    desc = _fetch_inline_describe(conn, "art_schema")
    assert len(desc["schemas"]) == 1
    assert desc["schemas"][0]["root_path"] == "$.result.data"
    assert desc["roots"] == []


def test_detect_duplicate_page_warning_emits_warning() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = ("art_prev", "hash_same")
    conn.execute.return_value = cursor
    warning = _detect_duplicate_page_warning(
        connection=conn,
        artifact_id="art_new",
        payload_hash_full="hash_same",
        created_seq=11,
        session_id="sess_1",
        source_tool="meta-ads.get_insights",
        forwarded_args={"after": "CURSOR_2"},
        pagination_config=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path="$.paging.next",
        ),
    )
    assert warning is not None
    assert warning["code"] == "PAGINATION_DUPLICATE_PAGE"
    assert warning["previous_artifact_id"] == "art_prev"
    assert warning["cursor_param"] == "after"
    assert warning["cursor_value"] == "CURSOR_2"


def test_detect_duplicate_page_warning_skips_when_hash_differs() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = ("art_prev", "hash_prev")
    conn.execute.return_value = cursor
    warning = _detect_duplicate_page_warning(
        connection=conn,
        artifact_id="art_new",
        payload_hash_full="hash_current",
        created_seq=11,
        session_id="sess_1",
        source_tool="meta-ads.get_insights",
        forwarded_args={"after": "CURSOR_2"},
        pagination_config=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path="$.paging.next",
        ),
    )
    assert warning is None


def test_fetch_inline_describe_dedupes_exact_duplicate_schema_roots() -> None:
    artifact_row = (
        "art_dup",
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
            "rk1_dup",
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
    ]
    schema_fields = {
        "rk1": [("$.id", ["string"], False, True, 100, "1")],
        "rk1_dup": [("$.id", ["string"], False, True, 100, "1")],
    }
    conn = _mock_connection(
        artifact_row=artifact_row,
        schema_root_rows=schema_root_rows,
        schema_field_rows_by_root_key=schema_fields,
    )
    desc = _fetch_inline_describe(conn, "art_dup")
    assert len(desc["schemas"]) == 1
    assert desc["schemas"][0]["root_path"] == "$.result.data"


def test_fetch_inline_describe_drops_parent_root_when_children_exist() -> None:
    artifact_row = (
        "art_parent",
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
            "rk_parent",
            "$.result",
            "schema_v1",
            "sha256:parent",
            "exact",
            "complete",
            1,
            "sha256:dataset",
            "traversal_v1",
            None,
        ),
        (
            "rk_data",
            "$.result.data",
            "schema_v1",
            "sha256:data",
            "exact",
            "complete",
            100,
            "sha256:dataset",
            "traversal_v1",
            None,
        ),
        (
            "rk_paging",
            "$.result.paging",
            "schema_v1",
            "sha256:paging",
            "exact",
            "complete",
            1,
            "sha256:dataset",
            "traversal_v1",
            None,
        ),
    ]
    schema_fields = {
        "rk_parent": [("$.data", ["array"], False, True, 1, "[]")],
        "rk_data": [("$.id", ["string"], False, True, 100, "1")],
        "rk_paging": [("$.next", ["string"], False, True, 1, "https://...")],
    }
    conn = _mock_connection(
        artifact_row=artifact_row,
        schema_root_rows=schema_root_rows,
        schema_field_rows_by_root_key=schema_fields,
    )
    desc = _fetch_inline_describe(conn, "art_parent")
    root_paths = [schema["root_path"] for schema in desc["schemas"]]
    assert "$.result" not in root_paths
    assert set(root_paths) == {"$.result.data", "$.result.paging"}


def test_sanitize_envelope_payload_preserves_gateway_pagination_state() -> None:
    pagination_state = {
        "upstream_prefix": "demo",
        "tool_name": "echo",
        "original_args": {"message": "hello"},
        "next_params": {"after": "CURSOR_2"},
        "page_number": 1,
    }
    envelope = normalize_envelope(
        upstream_instance_id="inst_demo",
        upstream_prefix="demo",
        tool="echo",
        status="ok",
        content=[{"type": "json", "value": {"ok": True}}],
        meta={
            "trace_token": "ghp_1234567890abcdef",
            "_gateway_pagination": pagination_state,
        },
    )

    def _fake_sanitize(result: dict[str, object]) -> dict[str, object]:
        payload = result["payload"]
        assert isinstance(payload, dict)
        meta = payload.get("meta")
        assert isinstance(meta, dict)
        redacted_payload = dict(payload)
        redacted_payload["meta"] = {
            **meta,
            "trace_token": "[REDACTED_SECRET]",
            "_gateway_pagination": {
                **pagination_state,
                "next_params": {"after": "[REDACTED_SECRET]"},
            },
        }
        return {"payload": redacted_payload}

    ctx = MagicMock()
    ctx._sanitize_tool_result.side_effect = _fake_sanitize

    sanitized = _sanitize_envelope_payload(
        ctx=ctx,
        envelope=envelope,
    )
    assert sanitized.meta["trace_token"] == "[REDACTED_SECRET]"
    gateway_pagination = sanitized.meta.get("_gateway_pagination")
    assert isinstance(gateway_pagination, dict)
    next_params = gateway_pagination.get("next_params")
    assert isinstance(next_params, dict)
    assert next_params.get("after") == "CURSOR_2"
