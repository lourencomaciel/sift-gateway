from __future__ import annotations

from sift_mcp.schema_compact import SCHEMA_LEGEND, compact_schema_payload


def _schema(fields: list[dict]) -> dict:
    return {
        "version": "schema_v1",
        "schema_hash": "sha256:test",
        "root_path": "$.items",
        "mode": "exact",
        "coverage": {"completeness": "complete", "observed_records": 2},
        "fields": fields,
        "determinism": {
            "dataset_hash": "sha256:data",
            "traversal_contract_version": "traversal_v1",
            "map_budget_fingerprint": None,
        },
    }


def test_compact_schema_payload_uses_short_keys_and_defaults() -> None:
    compacted = compact_schema_payload(
        [
            _schema(
                [
                    {
                        "path": "$.id",
                        "types": ["number"],
                        "nullable": False,
                        "required": True,
                        "observed_count": 2,
                        "example_value": "1",
                    },
                    {
                        "path": "$.optional",
                        "types": ["string"],
                        "nullable": True,
                        "required": False,
                        "observed_count": 1,
                        "example_value": "x",
                    },
                ]
            )
        ]
    )
    assert compacted[0]["rp"] == "$.items"
    assert compacted[0]["fd"]["oc"] == 2
    # oc omitted when equal to default
    assert "oc" not in compacted[0]["f"][0]
    # oc kept when different from default
    assert compacted[0]["f"][1]["oc"] == 1


def test_compact_schema_payload_extracts_example_truncation() -> None:
    compacted = compact_schema_payload(
        [
            _schema(
                [
                    {
                        "path": "$.description",
                        "types": ["string"],
                        "nullable": False,
                        "required": True,
                        "observed_count": 2,
                        "example_value": "[abcdefghijklmnopqrstuvwxyz1234](6 more chars truncated)",
                    }
                ]
            )
        ]
    )
    field = compacted[0]["f"][0]
    assert field["e"] == "abcdefghijklmnopqrstuvwxyz1234"
    assert field["tr"] == 6


def test_schema_legend_declares_field_aliases() -> None:
    assert SCHEMA_LEGEND["field"]["oc"] == "observed_count"
    assert SCHEMA_LEGEND["field"]["tr"] == "example_truncated_chars"
