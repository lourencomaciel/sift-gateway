"""Build deterministic schema metadata from mapped roots and records.

Provides two schema extraction modes:

- ``exact``: derive field paths/types from full in-memory JSON.
- ``sampled``: derive field paths/types from sampled records.

Both modes emit the same stable schema structure, including a
deterministic ``schema_hash`` and per-field ``required`` /
``nullable`` signals.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
import math
from typing import Any, Sequence

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.constants import TRAVERSAL_CONTRACT_VERSION
from sift_mcp.query.jsonpath import JsonPathError, evaluate_jsonpath
from sift_mcp.util.hashing import sha256_hex

SCHEMA_VERSION = "schema_v1"


@dataclass(frozen=True)
class SchemaFieldInventory:
    """Observed schema details for one field path under a root."""

    path: str
    types: list[str]
    nullable: bool
    required: bool
    observed_count: int
    example_value: str | None = None
    distinct_values: list[Any] | None = None
    cardinality: int | None = None


@dataclass(frozen=True)
class SchemaInventory:
    """Schema summary for one mapped root."""

    root_key: str
    version: str
    schema_hash: str
    root_path: str
    mode: str
    completeness: str
    observed_records: int
    fields: list[SchemaFieldInventory]
    dataset_hash: str
    traversal_contract_version: str
    map_budget_fingerprint: str | None


@dataclass
class _PathStats:
    """Mutable aggregation state for one field path."""

    types: set[str]
    observed_count: int
    example_value: str | None
    distinct_values: dict[tuple[str, Any], Any]


def _json_type_name(value: Any) -> str:
    """Return a JSON-style type label for a Python value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _normalize_path_segment(key: str) -> str:
    """Encode an object key as a canonical JSONPath segment."""
    import re

    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
        return f".{key}"
    escaped = (
        key.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f"['{escaped}']"


def _type_sort_key(type_name: str) -> tuple[int, str]:
    """Sort JSON types in a stable, human-readable order."""
    preferred_order = {
        "null": 0,
        "boolean": 1,
        "number": 2,
        "string": 3,
        "array": 4,
        "object": 5,
    }
    return preferred_order.get(type_name, 99), type_name


def _distinct_sort_key(value: Any) -> tuple[int, int, Any]:
    """Sort distinct values deterministically across simple scalar types."""
    type_name = _json_type_name(value)
    order = {
        "null": 0,
        "boolean": 1,
        "number": 2,
        "string": 3,
    }.get(type_name, 99)
    if type_name == "null":
        return order, 0, 0
    if type_name == "boolean":
        return order, 0, 1 if bool(value) else 0
    if type_name == "number":
        # Keep numeric ordering deterministic without lossy float casting.
        if isinstance(value, int) and not isinstance(value, bool):
            return order, 0, value
        if isinstance(value, float):
            if math.isnan(value):
                return order, 1, "nan"
            if math.isinf(value):
                return order, 1, "+inf" if value > 0 else "-inf"
            return order, 1, repr(value)
        return order, 2, repr(value)
    if type_name == "string":
        return order, 0, str(value)
    return order, 0, repr(value)


def _distinct_identity_key(value: Any) -> tuple[str, Any] | None:
    """Build a hashable key that preserves scalar type identity."""
    type_name = _json_type_name(value)
    if type_name == "null":
        return "null", None
    if type_name == "boolean":
        return "boolean", bool(value)
    if type_name == "number":
        if isinstance(value, bool):
            return "boolean", bool(value)
        if isinstance(value, int):
            return "number:int", value
        if isinstance(value, float):
            if math.isnan(value):
                return "number:float", "nan"
            if math.isinf(value):
                return "number:float", "+inf" if value > 0 else "-inf"
            return "number:float", repr(value)
        return "number:other", repr(value)
    if type_name == "string":
        return "string", str(value)
    try:
        hash(value)
    except TypeError:
        return None
    return type_name, value


def _truncate_example(text: str, *, max_chars: int = 30) -> str:
    """Return a max-length example preview with truncation metadata."""
    if len(text) <= max_chars:
        return text
    remaining = len(text) - max_chars
    return f"[{text[:max_chars]}]({remaining} more chars truncated)"


def _format_example_value(value: Any) -> str:
    """Format one deterministic example value for schema output."""
    if isinstance(value, str):
        return _truncate_example(value)
    try:
        rendered = canonical_bytes(value).decode("utf-8")
    except (TypeError, ValueError):
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    return _truncate_example(rendered)


def _walk_value(
    value: Any,
    *,
    path: str,
    stats: dict[str, _PathStats],
    seen_paths: set[str],
) -> None:
    """Collect types for a value and recurse through nested structures."""
    existing = stats.get(path)
    if existing is None:
        existing = _PathStats(
            types=set(),
            observed_count=0,
            example_value=_format_example_value(value),
            distinct_values={},
        )
        stats[path] = existing
    elif existing.example_value is None:
        existing.example_value = _format_example_value(value)
    existing.types.add(_json_type_name(value))
    seen_paths.add(path)

    if not isinstance(value, (dict, list)) and len(existing.distinct_values) < 10:
        distinct_key = _distinct_identity_key(value)
        if distinct_key is not None:
            existing.distinct_values.setdefault(distinct_key, value)

    if isinstance(value, dict):
        for key in sorted(value.keys()):
            child_path = f"{path}{_normalize_path_segment(str(key))}"
            _walk_value(
                value[key],
                path=child_path,
                stats=stats,
                seen_paths=seen_paths,
            )
        return

    if isinstance(value, list):
        child_path = f"{path}[*]"
        for item in value:
            _walk_value(
                item,
                path=child_path,
                stats=stats,
                seen_paths=seen_paths,
            )


def _build_fields(records: Sequence[Any]) -> tuple[list[SchemaFieldInventory], int]:
    """Build field inventories and observed record count from records."""
    observed_records = len(records)
    stats: dict[str, _PathStats] = {}

    for record in records:
        seen_paths: set[str] = set()
        _walk_value(record, path="$", stats=stats, seen_paths=seen_paths)
        for path in seen_paths:
            stats[path].observed_count += 1

    fields: list[SchemaFieldInventory] = []
    for path in sorted(path for path in stats if path != "$"):
        path_stats = stats[path]
        types = sorted(path_stats.types, key=_type_sort_key)
        fields.append(
            SchemaFieldInventory(
                path=path,
                types=types,
                nullable="null" in path_stats.types,
                required=(
                    observed_records > 0
                    and path_stats.observed_count == observed_records
                ),
                observed_count=path_stats.observed_count,
                example_value=path_stats.example_value,
                distinct_values=(
                    sorted(
                        path_stats.distinct_values.values(),
                        key=_distinct_sort_key,
                    )[:10]
                    if path_stats.distinct_values
                    else None
                ),
                cardinality=(
                    len(path_stats.distinct_values)
                    if path_stats.distinct_values
                    else None
                ),
            )
        )
    return fields, observed_records


def _schema_hash_payload(schema: SchemaInventory) -> dict[str, Any]:
    """Build canonical payload for schema hashing."""
    return {
        "version": schema.version,
        "root_path": schema.root_path,
        "mode": schema.mode,
        "coverage": {
            "completeness": schema.completeness,
            "observed_records": schema.observed_records,
        },
        "fields": [
            {
                "path": field.path,
                "types": field.types,
                "nullable": field.nullable,
                "required": field.required,
                "observed_count": field.observed_count,
                "example_value": field.example_value,
                "distinct_values": field.distinct_values,
                "cardinality": field.cardinality,
            }
            for field in schema.fields
        ],
        "determinism": {
            "dataset_hash": schema.dataset_hash,
            "traversal_contract_version": schema.traversal_contract_version,
            "map_budget_fingerprint": schema.map_budget_fingerprint,
        },
    }


def _coerce_floats_for_canonical_hash(value: Any) -> Any:
    """Recursively coerce floats to Decimal for canonical hashing."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {
            str(key): _coerce_floats_for_canonical_hash(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_coerce_floats_for_canonical_hash(item) for item in value]
    return value


def _with_schema_hash(schema: SchemaInventory) -> SchemaInventory:
    """Return a schema inventory with a deterministic hash attached."""
    payload = _coerce_floats_for_canonical_hash(_schema_hash_payload(schema))
    schema_hash = f"sha256:{sha256_hex(canonical_bytes(payload))}"
    return SchemaInventory(
        root_key=schema.root_key,
        version=schema.version,
        schema_hash=schema_hash,
        root_path=schema.root_path,
        mode=schema.mode,
        completeness=schema.completeness,
        observed_records=schema.observed_records,
        fields=schema.fields,
        dataset_hash=schema.dataset_hash,
        traversal_contract_version=schema.traversal_contract_version,
        map_budget_fingerprint=schema.map_budget_fingerprint,
    )


def build_exact_schema(
    *,
    json_target: Any,
    roots: Sequence[Any],
    payload_hash_full: str,
) -> list[SchemaInventory]:
    """Build exact schemas for mapped roots from full JSON."""
    from sift_mcp.mapping.json_strings import resolve_json_strings

    json_target = resolve_json_strings(json_target)
    dataset_hash = f"sha256:{payload_hash_full}"
    out: list[SchemaInventory] = []
    for root in roots:
        root_path = getattr(root, "root_path", "$")
        root_key = str(getattr(root, "root_key", root_path))
        try:
            matches = evaluate_jsonpath(json_target, str(root_path))
        except JsonPathError:
            matches = []
        value = matches[0] if matches else None
        if isinstance(value, list):
            records: Sequence[Any] = value
        elif isinstance(value, dict):
            records = [value]
        elif value is None:
            records = []
        else:
            records = [value]
        fields, observed_records = _build_fields(records)
        schema = SchemaInventory(
            root_key=root_key,
            version=SCHEMA_VERSION,
            schema_hash="",
            root_path=str(root_path),
            mode="exact",
            completeness="complete",
            observed_records=observed_records,
            fields=fields,
            dataset_hash=dataset_hash,
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
            map_budget_fingerprint=None,
        )
        out.append(_with_schema_hash(schema))
    return out


def build_sampled_schema(
    *,
    roots: Sequence[Any],
    samples: Sequence[Any],
    payload_hash_full: str,
    map_budget_fingerprint: str | None,
) -> list[SchemaInventory]:
    """Build sampled schemas from partial mapping sample rows."""
    from sift_mcp.mapping.json_strings import resolve_json_strings

    def _build_fields_from_path_stats(
        path_stats_raw: Any,
        *,
        observed_records: int,
    ) -> list[SchemaFieldInventory]:
        if not isinstance(path_stats_raw, dict):
            return []
        fields_out: list[SchemaFieldInventory] = []
        for path in sorted(path for path in path_stats_raw if path != "$"):
            raw = path_stats_raw.get(path)
            if not isinstance(raw, dict):
                continue
            raw_types = raw.get("types")
            if isinstance(raw_types, list):
                types = sorted(
                    {str(type_name) for type_name in raw_types},
                    key=_type_sort_key,
                )
            else:
                types = []
            observed_count_raw = raw.get("observed_count")
            observed_count = (
                int(observed_count_raw)
                if isinstance(observed_count_raw, int) and observed_count_raw >= 0
                else 0
            )
            example_raw = raw.get("example_value")
            example_value = (
                _format_example_value(example_raw)
                if example_raw is not None
                else None
            )
            raw_distinct_values = raw.get("distinct_values")
            distinct_values: list[Any] | None = None
            if isinstance(raw_distinct_values, list) and raw_distinct_values:
                distinct_values = sorted(
                    list(raw_distinct_values),
                    key=_distinct_sort_key,
                )[:10]
            cardinality_raw = raw.get("cardinality")
            cardinality = (
                int(cardinality_raw)
                if isinstance(cardinality_raw, int) and cardinality_raw >= 0
                else (
                    len(distinct_values)
                    if isinstance(distinct_values, list)
                    else None
                )
            )
            type_set = set(types)
            fields_out.append(
                SchemaFieldInventory(
                    path=str(path),
                    types=types,
                    nullable="null" in type_set,
                    required=(
                        observed_records > 0
                        and observed_count == observed_records
                    ),
                    observed_count=observed_count,
                    example_value=example_value,
                    distinct_values=distinct_values,
                    cardinality=cardinality,
                )
            )
        return fields_out

    dataset_hash = f"sha256:{payload_hash_full}"
    samples_by_root: dict[str, list[Any]] = {}
    for sample in samples:
        root_key = str(getattr(sample, "root_key", ""))
        samples_by_root.setdefault(root_key, []).append(sample)

    out: list[SchemaInventory] = []
    for root in roots:
        root_key = str(getattr(root, "root_key", ""))
        root_path = str(getattr(root, "root_path", "$"))
        root_samples = sorted(
            samples_by_root.get(root_key, []),
            key=lambda sample: int(getattr(sample, "sample_index", 0)),
        )
        records: list[Any] = [
            resolve_json_strings(getattr(sample, "record"))
            for sample in root_samples
            if hasattr(sample, "record")
        ]
        observed_records_raw = getattr(root, "sampled_prefix_len", None)
        observed_records = (
            int(observed_records_raw)
            if isinstance(observed_records_raw, int) and observed_records_raw >= 0
            else len(records)
        )
        fields_from_paths = _build_fields_from_path_stats(
            getattr(root, "path_stats", None),
            observed_records=observed_records,
        )
        if fields_from_paths:
            sampled_fields, _ = _build_fields(records)
            sampled_by_path = {
                field.path: field for field in sampled_fields
            }
            merged_fields: dict[str, SchemaFieldInventory] = {}
            for field in fields_from_paths:
                sampled_field = sampled_by_path.get(field.path)
                merged_fields[field.path] = SchemaFieldInventory(
                    path=field.path,
                    types=field.types or (
                        sampled_field.types if sampled_field is not None else []
                    ),
                    nullable=field.nullable
                    or (
                        sampled_field.nullable
                        if sampled_field is not None
                        else False
                    ),
                    required=field.required,
                    observed_count=field.observed_count,
                    example_value=field.example_value
                    or (
                        sampled_field.example_value
                        if sampled_field is not None
                        else None
                    ),
                    distinct_values=field.distinct_values
                    or (
                        sampled_field.distinct_values
                        if sampled_field is not None
                        else None
                    ),
                    cardinality=field.cardinality
                    if field.cardinality is not None
                    else (
                        sampled_field.cardinality
                        if sampled_field is not None
                        else None
                    ),
                )
            for sampled_field in sampled_fields:
                merged_fields.setdefault(sampled_field.path, sampled_field)
            fields = [
                merged_fields[path] for path in sorted(merged_fields.keys())
            ]
        else:
            fields, observed_records = _build_fields(records)
        schema = SchemaInventory(
            root_key=root_key,
            version=SCHEMA_VERSION,
            schema_hash="",
            root_path=root_path,
            mode="sampled",
            completeness="partial",
            observed_records=observed_records,
            fields=fields,
            dataset_hash=dataset_hash,
            traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
            map_budget_fingerprint=map_budget_fingerprint,
        )
        out.append(_with_schema_hash(schema))
    return out
