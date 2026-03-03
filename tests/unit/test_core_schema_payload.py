"""Tests for schema payload normalization helpers."""

from __future__ import annotations

from sift_gateway.core.schema_payload import build_schema_payload


def test_build_schema_payload_truncates_long_distinct_strings() -> None:
    schema_root = {
        "schema_version": "schema_v1",
        "schema_hash": "sha256:test",
        "root_path": "$",
        "mode": "exact",
        "completeness": "complete",
        "observed_records": 1,
        "dataset_hash": "sha256:data",
        "traversal_contract_version": "traversal_v1",
        "map_budget_fingerprint": None,
    }
    field_rows = [
        {
            "field_path": "$.text",
            "types": ["string"],
            "nullable": False,
            "required": True,
            "observed_count": 1,
            "example_value": "abc",
            "distinct_values": ["abcdefghijklmnopqrstuvwxyz1234567890"],
            "cardinality": 1,
        }
    ]

    payload = build_schema_payload(
        schema_root=schema_root,
        field_rows=field_rows,
    )

    assert payload["fields"][0]["distinct_values"] == [
        "[abcdefghijklmnopqrstuvwxyz1234](6 more chars truncated)"
    ]


def test_build_schema_payload_preserves_pre_truncated_distinct_strings() -> None:
    schema_root = {
        "schema_version": "schema_v1",
        "schema_hash": "sha256:test",
        "root_path": "$",
        "mode": "exact",
        "completeness": "complete",
        "observed_records": 1,
        "dataset_hash": "sha256:data",
        "traversal_contract_version": "traversal_v1",
        "map_budget_fingerprint": None,
    }
    field_rows = [
        {
            "field_path": "$.text",
            "types": ["string"],
            "nullable": False,
            "required": True,
            "observed_count": 1,
            "example_value": "abc",
            "distinct_values": [
                "[abcdefghijklmnopqrstuvwxyz1234](6 more chars truncated)"
            ],
            "cardinality": 1,
        }
    ]

    payload = build_schema_payload(
        schema_root=schema_root,
        field_rows=field_rows,
    )

    assert payload["fields"][0]["distinct_values"] == [
        "[abcdefghijklmnopqrstuvwxyz1234](6 more chars truncated)"
    ]
