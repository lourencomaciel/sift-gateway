"""Compact schema presentation helpers for response payloads.

These helpers keep full schema fidelity while reducing repeated
token overhead in mirrored and describe responses.
"""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any

SCHEMA_LEGEND: dict[str, Any] = {
    "schema": {
        "v": "version",
        "h": "schema_hash",
        "rp": "root_path",
        "m": "mode",
        "cv": "coverage",
        "fd": "field_defaults",
        "f": "fields",
        "d": "determinism",
    },
    "coverage": {
        "c": "completeness",
        "or": "observed_records",
    },
    "field": {
        "p": "path",
        "t": "types",
        "n": "nullable",
        "r": "required",
        "oc": "observed_count",
        "e": "example_value",
        "tr": "example_truncated_chars",
        "dv": "distinct_values",
        "cd": "cardinality",
    },
    "determinism": {
        "dh": "dataset_hash",
        "tv": "traversal_contract_version",
        "bf": "map_budget_fingerprint",
    },
    "rules": {
        "field_oc_default": ("field.oc omitted when equal to schema.fd.oc")
    },
}

_TRUNCATED_EXAMPLE_RE = re.compile(
    r"^\[(?P<value>.*)\]\((?P<trunc>\d+) more chars truncated\)$",
    re.DOTALL,
)


def _compact_example(example_value: str) -> tuple[str, int | None]:
    """Return compact example payload without verbose truncation text."""
    match = _TRUNCATED_EXAMPLE_RE.match(example_value)
    if match is None:
        return example_value, None
    value = match.group("value")
    trunc_raw = match.group("trunc")
    trunc = int(trunc_raw) if trunc_raw.isdigit() else None
    return value, trunc


def compact_schema_entry(schema: dict[str, Any]) -> dict[str, Any]:
    """Convert a verbose schema object to compact key form."""
    coverage = schema.get("coverage")
    coverage_completeness = (
        coverage.get("completeness") if isinstance(coverage, dict) else None
    )
    observed_records_raw = (
        coverage.get("observed_records") if isinstance(coverage, dict) else None
    )
    observed_records = (
        int(observed_records_raw)
        if isinstance(observed_records_raw, int)
        else 0
    )

    fields_raw = schema.get("fields")
    compact_fields: list[dict[str, Any]] = []
    if isinstance(fields_raw, list):
        for field in fields_raw:
            if not isinstance(field, dict):
                continue
            raw_types = field.get("types")
            compact_field: dict[str, Any] = {
                "p": field.get("path"),
                "t": (list(raw_types) if isinstance(raw_types, list) else []),
                "n": bool(field.get("nullable")),
                "r": bool(field.get("required")),
            }
            observed_count_raw = field.get("observed_count")
            if (
                isinstance(observed_count_raw, int)
                and observed_count_raw != observed_records
            ):
                compact_field["oc"] = observed_count_raw

            example_value = field.get("example_value")
            if isinstance(example_value, str):
                value, trunc = _compact_example(example_value)
                compact_field["e"] = value
                if isinstance(trunc, int) and trunc > 0:
                    compact_field["tr"] = trunc

            distinct_values = field.get("distinct_values")
            if isinstance(distinct_values, list) and distinct_values:
                compact_field["dv"] = list(distinct_values)

            cardinality = field.get("cardinality")
            if isinstance(cardinality, int):
                compact_field["cd"] = cardinality

            compact_fields.append(compact_field)

    determinism = schema.get("determinism")
    compact_determinism: dict[str, Any] = {
        "dh": (
            determinism.get("dataset_hash")
            if isinstance(determinism, dict)
            else None
        ),
        "tv": (
            determinism.get("traversal_contract_version")
            if isinstance(determinism, dict)
            else None
        ),
        "bf": (
            determinism.get("map_budget_fingerprint")
            if isinstance(determinism, dict)
            else None
        ),
    }

    return {
        "v": schema.get("version"),
        "h": schema.get("schema_hash"),
        "rp": schema.get("root_path"),
        "m": schema.get("mode"),
        "cv": {
            "c": coverage_completeness,
            "or": observed_records,
        },
        "fd": {"oc": observed_records},
        "f": compact_fields,
        "d": compact_determinism,
    }


def compact_schema_payload(
    schemas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Compact every schema entry in a payload."""
    compacted: list[dict[str, Any]] = []
    for schema in schemas:
        if not isinstance(schema, dict):
            continue
        compacted.append(compact_schema_entry(schema))
    return compacted


def normalize_compact_schema_payload(
    schemas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return compact schemas, avoiding double-compaction.

    ``artifact.describe(scope="single")`` already returns compact
    schema entries.  Downstream callers that always compact again
    can accidentally erase fields (yielding null-heavy placeholders).
    """
    normalized: list[dict[str, Any]] = []
    for schema in schemas:
        if not isinstance(schema, Mapping):
            continue
        if {"rp", "f", "cv"}.issubset(schema.keys()):
            normalized.append(dict(schema))
            continue
        normalized.append(compact_schema_entry(dict(schema)))
    return normalized
