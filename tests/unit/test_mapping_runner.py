"""Tests for mapping runner orchestration."""

from __future__ import annotations

import io
import json
from pathlib import Path

from mcp_artifact_gateway.config.settings import GatewayConfig
from mcp_artifact_gateway.mapping.runner import (
    MappingInput,
    run_mapping,
    select_json_part,
)


def _config(tmp_path: Path, **overrides: object) -> GatewayConfig:
    defaults: dict[str, object] = {"data_dir": tmp_path}
    defaults.update(overrides)
    return GatewayConfig(**defaults)


def test_select_json_part_prefers_largest_and_stable_tiebreak() -> None:
    envelope = {
        "content": [
            {"type": "json", "value": {"x": 1}},
            {"type": "json", "value": {"a": 1, "b": 2}},
            {"type": "json", "value": {"a": 1, "b": 2}},
        ]
    }
    selected = select_json_part(envelope)
    assert selected is not None
    assert selected.part_index == 1


def test_run_mapping_uses_binary_ref_stream_for_json_payload(
    tmp_path: Path,
) -> None:
    payload = [{"id": 1}, {"id": 2}, {"id": 3}]
    payload_bytes = json.dumps(
        payload, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    envelope = {
        "content": [
            {
                "type": "binary_ref",
                "mime": "application/json",
                "binary_hash": "hash_json_blob",
                "byte_count": len(payload_bytes),
            }
        ]
    }

    mapping_input = MappingInput(
        artifact_id="art_1",
        payload_hash_full="payload_hash_full_1",
        envelope=envelope,
        config=_config(tmp_path, max_full_map_bytes=1_000_000),
        open_binary_stream=lambda _binary_hash: io.BytesIO(payload_bytes),
    )
    result = run_mapping(mapping_input)

    assert result.map_kind == "partial"
    assert result.map_status == "ready"
    assert result.mapped_part_index == 0
    assert result.map_backend_id is not None
    assert result.prng_version is not None
    assert len(result.roots) == 1


def test_run_mapping_fails_for_json_binary_ref_without_stream_support(
    tmp_path: Path,
) -> None:
    envelope = {
        "content": [
            {
                "type": "binary_ref",
                "mime": "application/json+zstd",
                "binary_hash": "hash_json_blob",
                "byte_count": 1024,
            }
        ]
    }

    mapping_input = MappingInput(
        artifact_id="art_1",
        payload_hash_full="payload_hash_full_1",
        envelope=envelope,
        config=_config(tmp_path),
    )
    result = run_mapping(mapping_input)

    assert result.map_kind == "partial"
    assert result.map_status == "failed"
    assert result.map_error is not None
    assert "binary stream" in result.map_error


def test_mapped_part_index_none_when_no_json_part(tmp_path: Path) -> None:
    """mapped_part_index is None when no JSON content part exists."""
    envelope = {"content": [{"type": "text", "text": "hello"}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a1",
            payload_hash_full="p1",
            envelope=envelope,
            config=_config(tmp_path),
        )
    )
    assert result.map_status == "failed"
    assert result.mapped_part_index is None


def test_small_json_triggers_full_mapping(tmp_path: Path) -> None:
    """JSON below max_full_map_bytes triggers full mapping."""
    data = [{"id": 1}, {"id": 2}]
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_full",
            payload_hash_full="p_full",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.map_status == "ready"
    assert result.mapped_part_index == 0
    assert len(result.roots) == 1
    assert result.roots[0].count_estimate == 2
    assert result.map_budget_fingerprint is None
    assert result.samples is None


def test_large_json_triggers_partial_mapping(tmp_path: Path) -> None:
    """JSON value exceeding max_full_map_bytes triggers partial mapping."""
    data = [{"id": i, "v": "x" * 100} for i in range(100)]
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_p",
            payload_hash_full="p_p",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=100),
        )
    )
    assert result.map_kind == "partial"
    assert result.map_status == "ready"
    assert result.map_budget_fingerprint is not None
    assert result.map_backend_id is not None
    assert result.samples is not None


def test_select_json_part_binary_ref_json_mime() -> None:
    """select_json_part recognizes binary_ref with application/json mime."""
    envelope = {
        "content": [
            {
                "type": "binary_ref",
                "mime": "application/json",
                "binary_hash": "abc",
                "byte_count": 5000,
            }
        ]
    }
    sel = select_json_part(envelope)
    assert sel is not None
    assert sel.binary_hash == "abc"
    assert sel.byte_size == 5000


def test_select_json_part_ignores_non_json_binary() -> None:
    """binary_ref with non-JSON mime is ignored."""
    envelope = {
        "content": [
            {
                "type": "binary_ref",
                "mime": "image/png",
                "binary_hash": "abc",
                "byte_count": 5000,
            }
        ]
    }
    assert select_json_part(envelope) is None


def test_select_json_part_none_for_empty() -> None:
    """Empty content returns None."""
    assert select_json_part({"content": []}) is None
    assert select_json_part({}) is None


def test_select_json_part_mixed_picks_largest() -> None:
    """Largest part wins across json and binary_ref."""
    envelope = {
        "content": [
            {"type": "json", "value": {"a": 1}},
            {
                "type": "binary_ref",
                "mime": "application/json",
                "binary_hash": "big",
                "byte_count": 1_000_000,
            },
        ]
    }
    sel = select_json_part(envelope)
    assert sel is not None
    assert sel.part_index == 1
    assert sel.binary_hash == "big"


def test_run_mapping_closes_binary_stream(tmp_path: Path) -> None:
    """Binary stream is closed after partial mapping."""
    payload_bytes = json.dumps([{"id": 1}], separators=(",", ":")).encode()
    closed = []

    class TS(io.BytesIO):
        def close(self):
            closed.append(True)
            super().close()

    envelope = {
        "content": [
            {
                "type": "binary_ref",
                "mime": "application/json",
                "binary_hash": "h1",
                "byte_count": len(payload_bytes),
            }
        ]
    }
    result = run_mapping(
        MappingInput(
            artifact_id="a_c",
            payload_hash_full="p_c",
            envelope=envelope,
            config=_config(tmp_path),
            open_binary_stream=lambda _h: TS(payload_bytes),
        )
    )
    assert result.map_status == "ready"
    assert len(closed) == 1


def test_full_mapping_object_discovers_roots(tmp_path: Path) -> None:
    """Full mapping of object discovers multiple roots sorted by score."""
    data = {
        "users": [{"id": 1}, {"id": 2}],
        "orders": [{"oid": 1}, {"oid": 2}, {"oid": 3}],
    }
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_o",
            payload_hash_full="p_o",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.map_status == "ready"
    assert len(result.roots) == 2
    assert result.roots[0].root_key == "orders"
    assert result.roots[1].root_key == "users"
