"""Tests for mapping runner orchestration."""

from __future__ import annotations

import io
import json
from pathlib import Path

from sift_gateway.config.settings import GatewayConfig
from sift_gateway.mapping.runner import (
    MappingInput,
    RecordRow,
    RootInventory,
    _extract_full_records,
    _navigate_to_root,
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
                "mime": "application/json+gzip",
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


def test_text_part_falls_back_to_scalar_mapping(tmp_path: Path) -> None:
    """Text-only payload still maps deterministically as scalar JSON string."""
    envelope = {"content": [{"type": "text", "text": "hello"}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a1",
            payload_hash_full="p1",
            envelope=envelope,
            config=_config(tmp_path),
        )
    )
    assert result.map_status == "ready"
    assert result.map_kind == "full"
    assert result.mapped_part_index == 0
    assert result.schemas is not None
    assert len(result.schemas) == 1
    assert result.schemas[0].root_path == "$"
    assert result.schemas[0].fields == []


def test_text_part_json_string_is_parsed_for_mapping(tmp_path: Path) -> None:
    """JSON encoded as text is parsed and mapped as structured JSON."""
    envelope = {"content": [{"type": "text", "text": '{"users":[{"id":1}]}'}]}
    selected = select_json_part(envelope)
    assert selected is not None
    assert selected.part_index == 0
    assert isinstance(selected.value, dict)

    result = run_mapping(
        MappingInput(
            artifact_id="a_txt_json",
            payload_hash_full="p_txt_json",
            envelope=envelope,
            config=_config(tmp_path),
        )
    )
    assert result.map_status == "ready"
    assert result.schemas is not None
    assert any(schema.root_path == "$" for schema in result.schemas)


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
    assert result.schemas is not None
    assert len(result.schemas) == 1
    assert result.schemas[0].mode == "exact"
    assert result.schemas[0].version == "schema_v1"


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
    assert result.schemas is not None
    assert len(result.schemas) == 1
    assert result.schemas[0].mode == "sampled"


def test_run_mapping_fails_when_in_memory_budget_exceeded(
    tmp_path: Path,
) -> None:
    data = [{"id": i, "v": "x" * 200} for i in range(200)]
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_mem_guard",
            payload_hash_full="p_mem_guard",
            envelope=envelope,
            config=_config(
                tmp_path,
                max_full_map_bytes=100,
                max_in_memory_mapping_bytes=5_000,
            ),
        )
    )
    assert result.map_kind == "partial"
    assert result.map_status == "failed"
    assert result.map_error is not None
    assert "max_in_memory_mapping_bytes" in result.map_error


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


def test_full_mapping_object_maps_canonical_root(tmp_path: Path) -> None:
    """Full mapping of object keeps a single canonical root at ``$``."""
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
    assert len(result.roots) == 1
    assert result.roots[0].root_key == "$"
    assert result.roots[0].root_path == "$"


def test_full_schema_resolves_json_encoded_strings(tmp_path: Path) -> None:
    """Exact schema extraction resolves JSON-encoded strings like full mapping."""
    data = {
        "result": json.dumps(
            {"data": [{"id": 1}, {"id": 2}], "paging": {"next": "token"}},
            separators=(",", ":"),
            sort_keys=True,
        )
    }
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_json_str",
            payload_hash_full="p_json_str",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.map_status == "ready"
    assert result.schemas is not None
    by_path = {schema.root_path: schema for schema in result.schemas}
    assert "$" in by_path
    data_schema = by_path["$"]
    assert data_schema.observed_records == 1
    field_paths = {field.path for field in data_schema.fields}
    assert "$.result.data[*].id" in field_paths
    id_field = next(
        field
        for field in data_schema.fields
        if field.path == "$.result.data[*].id"
    )
    assert id_field.example_value == "1"


def test_schema_field_example_value_truncates_long_values(
    tmp_path: Path,
) -> None:
    data = {
        "items": [
            {"description": "abcdefghijklmnopqrstuvwxyz1234567890"},
        ]
    }
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_example",
            payload_hash_full="p_example",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.schemas is not None
    by_path = {schema.root_path: schema for schema in result.schemas}
    item_schema = by_path["$"]
    desc_field = next(
        field
        for field in item_schema.fields
        if field.path == "$.items[*].description"
    )
    assert (
        desc_field.example_value
        == "[abcdefghijklmnopqrstuvwxyz1234](6 more chars truncated)"
    )


def test_schema_field_distinct_values_are_capped_with_cardinality(
    tmp_path: Path,
) -> None:
    data = {"items": [{"action_type": f"type_{idx}"} for idx in range(12)]}
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_distinct",
            payload_hash_full="p_distinct",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.schemas is not None
    by_path = {schema.root_path: schema for schema in result.schemas}
    item_schema = by_path["$"]
    action_field = next(
        field
        for field in item_schema.fields
        if field.path == "$.items[*].action_type"
    )
    assert action_field.distinct_values is not None
    assert len(action_field.distinct_values) == 1
    assert action_field.cardinality == 1


def test_sampled_schema_distinct_values_reflect_sampled_records(
    tmp_path: Path,
) -> None:
    data = {"items": [{"action_type": f"type_{idx % 8}"} for idx in range(80)]}
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_sampled_distinct",
            payload_hash_full="p_sampled_distinct",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=100),
        )
    )
    assert result.map_kind == "partial"
    assert result.schemas is not None
    by_path = {schema.root_path: schema for schema in result.schemas}
    item_schema = by_path["$.items"]
    action_field = next(
        field for field in item_schema.fields if field.path == "$.action_type"
    )
    assert action_field.distinct_values is not None
    assert len(action_field.distinct_values) <= 1
    assert action_field.cardinality is not None


def test_schema_distinct_values_skips_unhashable_scalars() -> None:
    from sift_gateway.mapping.schema import _build_fields

    fields, observed = _build_fields([{"raw": {1, 2, 3}}])
    assert observed == 1
    raw_field = next(field for field in fields if field.path == "$.raw")
    assert raw_field.distinct_values is None
    assert raw_field.cardinality is None


def test_schema_distinct_values_preserve_boolean_vs_number_identity() -> None:
    from sift_gateway.mapping.schema import _build_fields

    fields, observed = _build_fields(
        [
            {"mixed": True},
            {"mixed": 1},
            {"mixed": False},
            {"mixed": 0},
        ]
    )
    assert observed == 4
    mixed_field = next(field for field in fields if field.path == "$.mixed")
    assert mixed_field.distinct_values == [True]
    assert mixed_field.cardinality == 1


def test_schema_distinct_values_handle_large_integers() -> None:
    from sift_gateway.mapping.schema import _build_fields

    huge = 10**309
    fields, observed = _build_fields([{"id": huge}, {"id": 1}])
    assert observed == 2
    id_field = next(field for field in fields if field.path == "$.id")
    assert id_field.distinct_values == [huge]
    assert id_field.cardinality == 1


def test_schema_distinct_values_handle_float_values(tmp_path: Path) -> None:
    data = {"items": [{"spend": 12.5}, {"spend": 9.25}]}
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_float_distinct",
            payload_hash_full="p_float_distinct",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.map_status == "ready"
    assert result.map_error is None
    assert result.schemas is not None
    by_path = {schema.root_path: schema for schema in result.schemas}
    item_schema = by_path["$"]
    spend_field = next(
        field for field in item_schema.fields if field.path == "$.items[*].spend"
    )
    assert spend_field.distinct_values is not None
    assert spend_field.distinct_values == [12.5]
    assert spend_field.cardinality == 1


def test_full_mapping_populates_record_rows(tmp_path: Path) -> None:
    """Full mapping populates record_rows from discovered roots."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_records",
            payload_hash_full="p_records",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=10_000_000),
        )
    )
    assert result.map_kind == "full"
    assert result.record_rows is not None
    assert len(result.record_rows) == 3
    assert all(isinstance(r, RecordRow) for r in result.record_rows)
    assert result.record_rows[0].root_path == "$"
    assert result.record_rows[0].idx == 0
    assert result.record_rows[0].record == {"id": 1}
    assert result.record_rows[2].idx == 2
    assert result.record_rows[2].record == {"id": 3}


def test_partial_mapping_populates_record_rows(tmp_path: Path) -> None:
    """Partial mapping populates record_rows from sampled records."""
    data = [{"id": i, "v": "x" * 100} for i in range(100)]
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_prec",
            payload_hash_full="p_prec",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=100),
        )
    )
    assert result.map_kind == "partial"
    assert result.record_rows is not None
    assert len(result.record_rows) > 0
    # Each record_row should match a sample
    assert result.samples is not None
    for row, sample in zip(result.record_rows, result.samples, strict=True):
        assert row.root_path == sample.root_path
        assert row.idx == sample.sample_index
        assert row.record == sample.record


def test_navigate_to_root_top_level() -> None:
    """_navigate_to_root returns full value for '$' root path."""
    data = [1, 2, 3]
    assert _navigate_to_root(data, "$") is data


def test_navigate_to_root_nested_path() -> None:
    """_navigate_to_root navigates JSONPath segments."""
    data = {"a": {"b": {"c": [1, 2]}}}
    assert _navigate_to_root(data, "$.a.b.c") == [1, 2]


def test_navigate_to_root_missing_key() -> None:
    """_navigate_to_root returns None for missing keys."""
    assert _navigate_to_root({"a": 1}, "$.b") is None
    assert _navigate_to_root({"a": 1}, "$.a.b") is None


def test_navigate_to_root_dotted_key() -> None:
    """_navigate_to_root handles keys containing literal dots."""
    data = {"a.b": [{"id": 1}]}
    assert _navigate_to_root(data, "$['a.b']") == [{"id": 1}]


def test_extract_full_records_array_root() -> None:
    """_extract_full_records extracts records from array root."""
    data = [{"id": 1}, {"id": 2}]
    roots = [
        RootInventory(
            root_key="$",
            root_path="$",
            count_estimate=2,
            root_shape="array",
            fields_top=None,
            root_summary=None,
            inventory_coverage=1.0,
            root_score=2.0,
        )
    ]
    rows = _extract_full_records(data, roots)
    assert len(rows) == 2
    assert rows[0] == RecordRow("$", 0, {"id": 1})
    assert rows[1] == RecordRow("$", 1, {"id": 2})


def test_extract_full_records_nested_object_root() -> None:
    """_extract_full_records handles nested object roots."""
    data = {"users": [{"id": 1}], "meta": {"total": 1}}
    roots = [
        RootInventory(
            root_key="users",
            root_path="$.users",
            count_estimate=1,
            root_shape="array",
            fields_top=None,
            root_summary=None,
            inventory_coverage=1.0,
            root_score=1.0,
        ),
        RootInventory(
            root_key="meta",
            root_path="$.meta",
            count_estimate=1,
            root_shape="object",
            fields_top=None,
            root_summary=None,
            inventory_coverage=1.0,
            root_score=0.5,
        ),
    ]
    rows = _extract_full_records(data, roots)
    assert len(rows) == 2
    assert rows[0] == RecordRow("$.users", 0, {"id": 1})
    assert rows[1] == RecordRow("$.meta", 0, {"total": 1})


def test_extract_full_records_skips_missing_root() -> None:
    """_extract_full_records skips roots that don't exist in data."""
    data = {"a": [{"id": 1}]}
    roots = [
        RootInventory(
            root_key="missing",
            root_path="$.missing",
            count_estimate=0,
            root_shape="array",
            fields_top=None,
            root_summary=None,
            inventory_coverage=1.0,
            root_score=1.0,
        )
    ]
    rows = _extract_full_records(data, roots)
    assert rows == []


def test_extract_full_records_preserves_non_dict_array_elements() -> None:
    """_extract_full_records materializes all element types."""
    data = [1, {"id": 2}, "three", {"id": 4}]
    roots = [
        RootInventory(
            root_key="$",
            root_path="$",
            count_estimate=4,
            root_shape="array",
            fields_top=None,
            root_summary=None,
            inventory_coverage=1.0,
            root_score=4.0,
        )
    ]
    rows = _extract_full_records(data, roots)
    assert len(rows) == 4
    assert rows[0] == RecordRow("$", 0, 1)
    assert rows[1] == RecordRow("$", 1, {"id": 2})
    assert rows[2] == RecordRow("$", 2, "three")
    assert rows[3] == RecordRow("$", 3, {"id": 4})


def test_extract_full_records_scalar_root() -> None:
    """_extract_full_records materializes scalar root values."""
    data = {"count": 42}
    roots = [
        RootInventory(
            root_key="count",
            root_path="$.count",
            count_estimate=1,
            root_shape=None,
            fields_top=None,
            root_summary=None,
            inventory_coverage=1.0,
            root_score=1.0,
        )
    ]
    rows = _extract_full_records(data, roots)
    assert len(rows) == 1
    assert rows[0] == RecordRow("$.count", 0, 42)


def test_partial_mapping_resolves_json_encoded_strings_for_schema(
    tmp_path: Path,
) -> None:
    data = {
        "result": json.dumps(
            {"data": [{"id": i} for i in range(40)]},
            separators=(",", ":"),
            sort_keys=True,
        )
    }
    envelope = {"content": [{"type": "json", "value": data}]}
    result = run_mapping(
        MappingInput(
            artifact_id="a_partial_json_str",
            payload_hash_full="p_partial_json_str",
            envelope=envelope,
            config=_config(tmp_path, max_full_map_bytes=100),
        )
    )
    assert result.map_kind == "partial"
    assert result.map_status == "ready"
    assert result.schemas is not None
    by_path = {schema.root_path: schema for schema in result.schemas}
    assert "$.result.data" in by_path
    data_schema = by_path["$.result.data"]
    field_paths = {field.path for field in data_schema.fields}
    assert "$.id" in field_paths
