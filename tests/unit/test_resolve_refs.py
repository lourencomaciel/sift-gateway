from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from sift_gateway.mcp.resolve_refs import (
    ParsedRef,
    ResolveError,
    is_artifact_ref,
    parse_artifact_ref,
    resolve_artifact_refs,
)

# ---------------------------------------------------------------------------
# is_artifact_ref
# ---------------------------------------------------------------------------


def test_is_artifact_ref_valid() -> None:
    assert is_artifact_ref("art_a1b2c3d4e5f67890a1b2c3d4e5f67890")


def test_is_artifact_ref_wrong_prefix() -> None:
    assert not is_artifact_ref("foo_a1b2c3d4e5f67890a1b2c3d4e5f67890")


def test_is_artifact_ref_too_short() -> None:
    assert not is_artifact_ref("art_a1b2c3d4")


def test_is_artifact_ref_too_long() -> None:
    assert not is_artifact_ref("art_a1b2c3d4e5f67890a1b2c3d4e5f678901")


def test_is_artifact_ref_uppercase_hex_rejected() -> None:
    assert not is_artifact_ref("art_A1B2C3D4E5F67890A1B2C3D4E5F67890")


def test_is_artifact_ref_non_hex_chars() -> None:
    assert not is_artifact_ref("art_g1b2c3d4e5f67890a1b2c3d4e5f67890")


def test_is_artifact_ref_non_string() -> None:
    assert not is_artifact_ref(42)
    assert not is_artifact_ref(None)
    assert not is_artifact_ref(["art_a1b2c3d4e5f67890a1b2c3d4e5f67890"])


def test_is_artifact_ref_empty_string() -> None:
    assert not is_artifact_ref("")


def test_is_artifact_ref_just_prefix() -> None:
    assert not is_artifact_ref("art_")


# ---------------------------------------------------------------------------
# resolve_artifact_refs — no refs present (fast path)
# ---------------------------------------------------------------------------


def test_resolve_no_refs_returns_args_unchanged() -> None:
    conn = MagicMock()
    args = {"query": "hello", "limit": 10}
    result = resolve_artifact_refs(conn, args)
    assert result == args
    # No DB calls should have been made.
    conn.execute.assert_not_called()


def test_resolve_empty_args() -> None:
    conn = MagicMock()
    result = resolve_artifact_refs(conn, {})
    assert result == {}
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# resolve_artifact_refs — happy path (JSON artifact)
# ---------------------------------------------------------------------------

_VALID_ART_ID = "art_a1b2c3d4e5f67890a1b2c3d4e5f67890"


def _mock_connection_for_artifact(
    *,
    envelope: dict | None = None,
    canonical_encoding: str = "none",
    payload_fs_path: str | None = None,
    payload_hash: str = "fakehash",
    deleted_at: object = None,
    mapped_part_index: int | None = 0,
    contains_binary_refs: bool = False,
) -> MagicMock:
    """Build a mock connection that returns a canned artifact row."""
    conn = MagicMock()

    row = (
        _VALID_ART_ID,  # artifact_id
        payload_hash,  # payload_hash_full
        deleted_at,  # deleted_at
        "full",  # map_kind
        "ready",  # map_status
        1,  # generation
        mapped_part_index,  # mapped_part_index
        None,  # map_budget_fingerprint
        envelope,  # envelope (JSONB)
        canonical_encoding,  # envelope_canonical_encoding
        payload_fs_path,  # payload_fs_path
        contains_binary_refs,  # contains_binary_refs
    )

    cursor_mock = MagicMock()
    cursor_mock.fetchone.return_value = row
    conn.execute.return_value = cursor_mock
    return conn


def test_resolve_json_artifact_happy_path() -> None:
    payload = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    args = {"records": _VALID_ART_ID, "limit": 5}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["records"] == payload
    assert result["limit"] == 5


def test_resolve_text_artifact() -> None:
    envelope = {
        "content": [{"type": "text", "text": "hello world"}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(
        envelope=envelope, mapped_part_index=None
    )
    args = {"input": _VALID_ART_ID}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["input"] == "hello world"


def test_resolve_multiple_refs() -> None:
    payload_a = {"data": [1, 2, 3]}
    envelope_a = {
        "content": [{"type": "json", "value": payload_a}],
        "status": "ok",
    }
    payload_b = {"summary": "ok"}
    envelope_b = {
        "content": [{"type": "json", "value": payload_b}],
        "status": "ok",
    }

    art_id_a = "art_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    art_id_b = "art_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    call_count = 0

    def _execute(sql, params):
        nonlocal call_count
        cursor = MagicMock()
        art_id = params[1]
        if art_id == art_id_a:
            cursor.fetchone.return_value = (
                art_id_a,
                "h1",
                None,
                "full",
                "ready",
                1,
                0,
                None,
                envelope_a,
                "none",
                None,
                False,
            )
        else:
            cursor.fetchone.return_value = (
                art_id_b,
                "h2",
                None,
                "full",
                "ready",
                1,
                0,
                None,
                envelope_b,
                "none",
                None,
                False,
            )
        call_count += 1
        return cursor

    conn = MagicMock()
    conn.execute.side_effect = _execute
    args = {"left": art_id_a, "right": art_id_b, "mode": "compare"}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["left"] == payload_a
    assert result["right"] == payload_b
    assert result["mode"] == "compare"
    assert call_count == 2


# ---------------------------------------------------------------------------
# resolve_artifact_refs — nested values NOT resolved
# ---------------------------------------------------------------------------


def test_resolve_nested_ref_not_touched() -> None:
    conn = MagicMock()
    nested_id = _VALID_ART_ID
    args = {"config": {"ref": nested_id}, "items": [nested_id]}
    result = resolve_artifact_refs(conn, args)
    assert result == args
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# resolve_artifact_refs — error cases
# ---------------------------------------------------------------------------


def test_resolve_artifact_not_found() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn.execute.return_value = cursor
    args = {"data": _VALID_ART_ID}
    result = resolve_artifact_refs(conn, args)
    assert isinstance(result, ResolveError)
    assert result.code == "NOT_FOUND"
    assert _VALID_ART_ID in result.message


def test_resolve_deleted_artifact() -> None:
    conn = _mock_connection_for_artifact(
        envelope={"content": [], "status": "ok"},
        deleted_at="2025-01-01T00:00:00",
    )
    args = {"data": _VALID_ART_ID}
    result = resolve_artifact_refs(conn, args)
    assert isinstance(result, ResolveError)
    assert result.code == "GONE"


def test_resolve_binary_only_artifact() -> None:
    envelope = {
        "content": [
            {
                "type": "binary_ref",
                "blob_id": "bin_abc",
                "binary_hash": "deadbeef",
                "mime": "image/png",
                "byte_count": 1024,
            }
        ],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(
        envelope=envelope,
        contains_binary_refs=True,
        mapped_part_index=None,
    )
    args = {"image": _VALID_ART_ID}
    result = resolve_artifact_refs(conn, args)
    assert isinstance(result, ResolveError)
    assert result.code == "INVALID_ARGUMENT"
    assert "binary" in result.message


def test_resolve_binary_with_json_part_succeeds() -> None:
    """An artifact with both binary and JSON parts resolves OK."""
    payload = {"metadata": "image info"}
    envelope = {
        "content": [
            {"type": "json", "value": payload},
            {
                "type": "binary_ref",
                "blob_id": "bin_abc",
                "binary_hash": "deadbeef",
                "mime": "image/png",
                "byte_count": 1024,
            },
        ],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(
        envelope=envelope,
        contains_binary_refs=True,
    )
    args = {"data": _VALID_ART_ID}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["data"] == payload


def test_resolve_missing_payload_file_path() -> None:
    conn = _mock_connection_for_artifact(
        envelope=None,
        payload_fs_path=None,
    )
    args = {"data": _VALID_ART_ID}
    result = resolve_artifact_refs(conn, args)
    assert isinstance(result, ResolveError)
    assert result.code == "INTERNAL"
    assert "missing payload file path" in result.message


def test_resolve_missing_payload_file_returns_resolve_error(
    tmp_path: Path,
) -> None:
    conn = _mock_connection_for_artifact(
        envelope=None,
        payload_fs_path="aa/bb/missing.zst",
    )
    args = {"data": _VALID_ART_ID}
    result = resolve_artifact_refs(
        conn,
        args,
        blobs_payload_dir=tmp_path,
    )
    assert isinstance(result, ResolveError)
    assert result.code == "INTERNAL"
    assert "envelope reconstruction failed" in result.message


# ---------------------------------------------------------------------------
# is_artifact_ref — query references
# ---------------------------------------------------------------------------


def test_is_artifact_ref_query_ref() -> None:
    ref = _VALID_ART_ID + ":$.items[0].name"
    assert is_artifact_ref(ref)


def test_is_artifact_ref_query_ref_wildcard() -> None:
    ref = _VALID_ART_ID + ":$.items[*].email"
    assert is_artifact_ref(ref)


def test_is_artifact_ref_query_ref_root_only() -> None:
    ref = _VALID_ART_ID + ":$"
    assert is_artifact_ref(ref)


def test_is_artifact_ref_colon_no_dollar() -> None:
    ref = _VALID_ART_ID + ":items"
    assert not is_artifact_ref(ref)


def test_is_artifact_ref_trailing_garbage() -> None:
    ref = _VALID_ART_ID + "extra"
    assert not is_artifact_ref(ref)


# ---------------------------------------------------------------------------
# parse_artifact_ref
# ---------------------------------------------------------------------------


def test_parse_bare_ref() -> None:
    parsed = parse_artifact_ref(_VALID_ART_ID)
    assert parsed == ParsedRef(artifact_id=_VALID_ART_ID, jsonpath=None)


def test_parse_query_ref() -> None:
    ref = _VALID_ART_ID + ":$.items[0].name"
    parsed = parse_artifact_ref(ref)
    assert parsed == ParsedRef(
        artifact_id=_VALID_ART_ID,
        jsonpath="$.items[0].name",
    )


def test_parse_query_ref_wildcard() -> None:
    ref = _VALID_ART_ID + ":$.items[*].email"
    parsed = parse_artifact_ref(ref)
    assert parsed is not None
    assert parsed.jsonpath == "$.items[*].email"


def test_parse_non_ref() -> None:
    assert parse_artifact_ref("hello") is None
    assert parse_artifact_ref(42) is None
    assert parse_artifact_ref(None) is None


def test_parse_trailing_garbage() -> None:
    assert parse_artifact_ref(_VALID_ART_ID + "x") is None


# ---------------------------------------------------------------------------
# resolve_artifact_refs — query resolution (JSONPath)
# ---------------------------------------------------------------------------


def test_resolve_query_single_field() -> None:
    payload = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    ref = _VALID_ART_ID + ":$.users[0].name"
    args = {"name": ref, "other": "keep"}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["name"] == "Alice"
    assert result["other"] == "keep"


def test_resolve_query_wildcard_returns_list() -> None:
    payload = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    ref = _VALID_ART_ID + ":$.users[*].name"
    args = {"names": ref}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["names"] == ["Alice", "Bob"]


def test_resolve_query_nested_scalar() -> None:
    payload = {"config": {"db": {"host": "localhost"}}}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    ref = _VALID_ART_ID + ":$.config.db.host"
    args = {"host": ref}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["host"] == "localhost"


def test_resolve_query_no_matches_returns_error() -> None:
    payload = {"users": [{"name": "Alice"}]}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    ref = _VALID_ART_ID + ":$.nonexistent"
    args = {"val": ref}
    result = resolve_artifact_refs(conn, args)
    assert isinstance(result, ResolveError)
    assert result.code == "NOT_FOUND"
    assert "matched no values" in result.message


def test_resolve_query_invalid_jsonpath_returns_error() -> None:
    payload = {"users": []}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    ref = _VALID_ART_ID + ":$.users[?(@.active)]"
    args = {"val": ref}
    result = resolve_artifact_refs(conn, args)
    assert isinstance(result, ResolveError)
    assert result.code == "INVALID_ARGUMENT"
    assert "JSONPath" in result.message


def test_resolve_query_root_dollar_returns_full_payload() -> None:
    payload = {"a": 1, "b": 2}
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    ref = _VALID_ART_ID + ":$"
    args = {"val": ref}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    # "$" matches the root document — single match, unwrapped.
    assert result["val"] == payload


def test_resolve_mixed_bare_and_query_refs() -> None:
    payload = {
        "items": [{"id": 1, "name": "Widget"}],
        "meta": {"total": 1},
    }
    envelope = {
        "content": [{"type": "json", "value": payload}],
        "status": "ok",
    }
    conn = _mock_connection_for_artifact(envelope=envelope)
    bare_ref = _VALID_ART_ID
    query_ref = _VALID_ART_ID + ":$.items[0].name"
    args = {"full": bare_ref, "name": query_ref, "flag": True}
    result = resolve_artifact_refs(conn, args)
    assert not isinstance(result, ResolveError)
    assert result["full"] == payload
    assert result["name"] == "Widget"
    assert result["flag"] is True
