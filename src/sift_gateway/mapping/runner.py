"""Orchestrate artifact mapping by selecting a JSON part and routing.

Select the best JSON content part from an envelope, decide
whether to use full (in-memory) or partial (streaming)
mapping based on size thresholds, and dispatch to the
appropriate mapper.  Key exports are ``run_mapping``,
``select_json_part``, and the data-transfer objects
``MappingInput``, ``MappingResult``, ``RootInventory``,
``SampleRecord``, and ``SelectedJsonPart``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import tempfile
from typing import Any, BinaryIO, cast

from sift_gateway.config.settings import GatewayConfig
from sift_gateway.mapping.schema import (
    SchemaInventory,
    build_exact_schema,
    build_sampled_schema,
)

_PARTIAL_MAP_SPOOL_MAX_MEMORY_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class RootInventory:
    """Inventory of a discovered collection root in mapped data.

    Describe a single root (array or object) found during
    full or partial mapping, including its location, shape,
    field type distribution, and sampling metadata.

    Attributes:
        root_key: Key identifying this root (or "$" for top).
        root_path: Canonical JSONPath to the root.
        count_estimate: Element count, or None if unknown.
        root_shape: "array", "object", or None for scalars.
        fields_top: Field name to type distribution map.
        root_summary: Summary statistics dict for the root.
        inventory_coverage: Fraction of elements sampled.
        root_score: Relevance score (higher is better).
        sample_indices: Sampled element indices, or None.
        prefix_coverage: True if stopped before full scan.
        stop_reason: Why scanning stopped, or None.
        sampled_prefix_len: Elements seen before stopping.
        path_stats: Optional per-path stats discovered during
            mapping, keyed by canonical JSONPath.
    """

    root_key: str
    root_path: str
    count_estimate: int | None
    root_shape: str | None
    fields_top: dict[str, Any] | None
    root_summary: dict[str, Any] | None
    inventory_coverage: float | None
    root_score: float
    sample_indices: list[int] | None = None
    prefix_coverage: bool = False
    stop_reason: str | None = None
    sampled_prefix_len: int | None = None
    path_stats: dict[str, Any] | None = None


@dataclass(frozen=True)
class SampleRecord:
    """Single sampled record collected during partial mapping.

    Attributes:
        root_key: Key of the root this sample belongs to.
        root_path: Canonical JSONPath of the parent root.
        sample_index: Zero-based index within the root array.
        record: The sampled JSON object.
        record_bytes: Canonical byte size of the record.
        record_hash: SHA-256 hex digest of canonical record.
    """

    root_key: str
    root_path: str
    sample_index: int
    record: dict[str, Any]
    record_bytes: int
    record_hash: str


@dataclass(frozen=True)
class RecordRow:
    """Single materialized record for SQL-based filtering.

    Stored in the ``artifact_records`` table during mapping so
    handlers can query with ``json_extract`` instead of in-memory
    traversal.

    Attributes:
        root_path: Canonical JSONPath to the parent root.
        idx: Zero-based index within the root collection.
        record: The extracted JSON value (object, array,
            or scalar).
    """

    root_path: str
    idx: int
    record: Any


@dataclass(frozen=True)
class MappingInput:
    """Immutable input bundle for the mapping pipeline.

    Attributes:
        artifact_id: Artifact being mapped.
        payload_hash_full: SHA-256 hex of canonical payload.
        envelope: Raw envelope dict for part selection.
        config: Gateway configuration with budget limits.
        open_binary_stream: Optional callback to open a blob
            stream by binary hash for partial mapping of
            oversized JSON stored as binary refs.
    """

    artifact_id: str
    payload_hash_full: str
    envelope: dict[str, Any]
    config: GatewayConfig
    open_binary_stream: Callable[[str], BinaryIO] | None = None


@dataclass(frozen=True)
class SelectedJsonPart:
    """Best JSON-compatible content part chosen for mapping.

    Attributes:
        part_index: Index of the part in envelope content.
        byte_size: Estimated serialized size in bytes.
        value: Parsed JSON value, or None for binary refs.
        binary_hash: Blob hash for binary ref parts, or None.
    """

    part_index: int
    byte_size: int
    value: Any | None = None
    binary_hash: str | None = None


@dataclass(frozen=True)
class MappingResult:
    """Output of a full or partial mapping run.

    Attributes:
        map_kind: "full" or "partial".
        map_status: "ready", "failed", or "pending".
        mapped_part_index: Index of the mapped content part.
        roots: Discovered root inventories.
        map_budget_fingerprint: Budget config fingerprint.
        map_backend_id: Runtime backend identifier hash.
        prng_version: Deterministic PRNG version string.
        map_error: Error message if failed, else None.
        samples: Sampled records (partial mapping only).
        schemas: Extracted schema inventories per root.
    """

    map_kind: str
    map_status: str
    mapped_part_index: int | None
    roots: list[RootInventory]
    map_budget_fingerprint: str | None
    map_backend_id: str | None
    prng_version: str | None
    map_error: str | None
    samples: list[SampleRecord] | None = None
    schemas: list[SchemaInventory] | None = None
    record_rows: list[RecordRow] | None = None


def _is_json_binary_mime(raw_mime: object) -> bool:
    """Check if a MIME type indicates JSON content.

    Args:
        raw_mime: MIME type value, or non-string to reject.

    Returns:
        True if the MIME type is ``application/json``, uses
        the ``+json`` structured syntax suffix (RFC 6838),
        or uses the ``application/json+*`` prefix convention.
    """
    if not isinstance(raw_mime, str):
        return False
    mime = raw_mime.split(";", 1)[0].strip().lower()
    return (
        mime == "application/json"
        or mime.endswith("+json")
        or mime.startswith("application/json+")
    )


def _score_json_part(
    i: int,
    part: dict[str, Any],
) -> SelectedJsonPart | None:
    """Score a JSON-typed content part for mapping selection.

    Args:
        i: Index of the part in envelope content.
        part: Raw content part dict with type "json".

    Returns:
        A SelectedJsonPart with byte_size, or None if the
        part has no value.
    """
    value = part.get("value")
    if value is None:
        return None
    byte_size = _json_size_bytes(value)
    return SelectedJsonPart(
        part_index=i,
        byte_size=byte_size,
        value=value,
    )


def _score_binary_ref_part(
    i: int,
    part: dict[str, Any],
) -> SelectedJsonPart | None:
    """Score a binary_ref content part with JSON MIME type.

    Args:
        i: Index of the part in envelope content.
        part: Raw content part dict with type "binary_ref".

    Returns:
        A SelectedJsonPart with binary_hash, or None if the
        part has non-JSON MIME or invalid fields.
    """
    if not _is_json_binary_mime(part.get("mime")):
        return None
    binary_hash = part.get("binary_hash")
    byte_count = part.get("byte_count")
    if not isinstance(binary_hash, str) or not binary_hash:
        return None
    if not isinstance(byte_count, int) or byte_count < 0:
        return None
    return SelectedJsonPart(
        part_index=i,
        byte_size=byte_count,
        binary_hash=binary_hash,
    )


def _score_text_part(
    i: int,
    part: dict[str, Any],
) -> SelectedJsonPart | None:
    """Score a text part by parsing JSON when possible.

    Fallback behavior for schema-first reliability:
    - If text parses to JSON, map that parsed value.
    - If text is plain/non-JSON, map the string scalar so mapping
      still completes deterministically.
    """
    text = part.get("text")
    if not isinstance(text, str):
        return None

    parsed: Any = text
    trimmed = text.strip()
    if trimmed:
        try:
            parsed = json.loads(trimmed)
            # Handle double-encoded JSON strings, e.g. "\"{...}\"".
            if isinstance(parsed, str):
                nested = parsed.strip()
                if nested:
                    try:
                        parsed = json.loads(nested)
                    except Exception:
                        parsed = parsed
        except Exception:
            parsed = text

    if isinstance(parsed, str):
        # Plain text scalar: force full mapping path.
        return SelectedJsonPart(
            part_index=i,
            byte_size=0,
            value=parsed,
        )

    byte_size = _json_size_bytes(parsed)
    return SelectedJsonPart(
        part_index=i,
        byte_size=byte_size,
        value=parsed,
    )


def _json_size_bytes(value: Any) -> int:
    """Compute UTF-8 JSON size without building one giant string."""
    encoder = json.JSONEncoder(
        separators=(",", ":"),
        sort_keys=True,
    )
    size = 0
    for chunk in encoder.iterencode(value):
        size += len(chunk.encode("utf-8"))
    return size


def select_json_part(
    envelope: dict[str, Any],
) -> SelectedJsonPart | None:
    """Select the best JSON-compatible content part for mapping.

    Score explicit JSON and JSON-mime binary_ref parts by byte size,
    preferring larger parts.  Break ties by ascending index.
    If none exist, fall back to text parts by attempting JSON parse;
    plain text is treated as a scalar JSON string for deterministic
    schema generation.

    Args:
        envelope: Raw envelope dict with a ``content`` list.

    Returns:
        The highest-scoring SelectedJsonPart, or None if no
        JSON-compatible part is found.
    """
    content = envelope.get("content", [])
    best: SelectedJsonPart | None = None

    for i, part in enumerate(content):
        if not isinstance(part, dict):
            continue
        part_type = part.get("type")
        if part_type == "json":
            candidate = _score_json_part(i, part)
        elif part_type == "binary_ref":
            candidate = _score_binary_ref_part(i, part)
        else:
            candidate = None

        if candidate is None:
            continue
        # Prefer larger; tie-break by ascending index
        if best is None or candidate.byte_size > best.byte_size:
            best = candidate

    if best is not None:
        return best

    # Fallback: text-only upstream payloads.
    for i, part in enumerate(content):
        if not isinstance(part, dict):
            continue
        if part.get("type") != "text":
            continue
        candidate = _score_text_part(i, part)
        if candidate is None:
            continue
        if best is None or candidate.byte_size > best.byte_size:
            best = candidate

    return best


def _failed_result(
    *,
    map_kind: str,
    mapped_part_index: int | None,
    map_error: str,
    map_budget_fingerprint: str | None = None,
    map_backend_id: str | None = None,
    prng_version: str | None = None,
) -> MappingResult:
    """Build a MappingResult with status "failed".

    Args:
        map_kind: "full" or "partial".
        mapped_part_index: Index of the part that failed.
        map_error: Human-readable error description.
        map_budget_fingerprint: Budget fingerprint, if known.
        map_backend_id: Backend identifier, if known.
        prng_version: PRNG version string, if known.

    Returns:
        A MappingResult with empty roots and failed status.
    """
    return MappingResult(
        map_kind=map_kind,
        map_status="failed",
        mapped_part_index=mapped_part_index,
        roots=[],
        map_budget_fingerprint=map_budget_fingerprint,
        map_backend_id=map_backend_id,
        prng_version=prng_version,
        map_error=map_error,
        schemas=[],
    )


def _extract_full_records(
    json_value: Any,
    roots: list[RootInventory],
) -> list[RecordRow]:
    """Extract all records from a fully-parsed JSON value.

    Navigate to each discovered root and collect its elements
    as ``RecordRow`` objects for storage in ``artifact_records``.
    All JSON value types (objects, arrays, scalars) are
    materialized so that SQL-based handlers can query them.

    Args:
        json_value: The parsed JSON value that was mapped.
        roots: Root inventories discovered by full mapping.

    Returns:
        A list of RecordRow, one per element per root.
    """
    rows: list[RecordRow] = []
    for root in roots:
        value = _navigate_to_root(json_value, root.root_path)
        if value is None:
            continue
        if isinstance(value, list):
            for idx, elem in enumerate(value):
                rows.append(RecordRow(root.root_path, idx, elem))
        else:
            rows.append(RecordRow(root.root_path, 0, value))
    return rows


def _navigate_to_root(json_value: Any, root_path: str) -> Any:
    """Navigate a parsed JSON value to a root by JSONPath.

    Uses ``evaluate_jsonpath`` so that keys containing dots or
    other special characters (bracket-quoted in the path) are
    resolved correctly.

    Args:
        json_value: The top-level parsed JSON value.
        root_path: Canonical JSONPath string (e.g. ``"$"``,
            ``"$.users"``, ``"$.data.items"``,
            ``"$['a.b']"``).

    Returns:
        The value at the root location, or None if not found.
    """
    if root_path == "$":
        return json_value
    from sift_gateway.query.jsonpath import (
        JsonPathError,
        evaluate_jsonpath,
    )

    try:
        results = evaluate_jsonpath(json_value, root_path)
    except JsonPathError:
        return None
    return results[0] if results else None


def _run_full_mapping(
    selected: SelectedJsonPart,
    config: GatewayConfig,
    payload_hash_full: str,
) -> MappingResult:
    """Execute full in-memory mapping on the selected part.

    Args:
        selected: The JSON part chosen for mapping.
        config: Gateway configuration with root discovery limit.
        payload_hash_full: Canonical payload SHA-256 hex.

    Returns:
        A MappingResult with kind "full" and status "ready"
        or "failed".
    """
    from sift_gateway.mapping.full import (
        run_full_mapping,
    )

    part_index = selected.part_index
    if selected.value is None:
        return _failed_result(
            map_kind="full",
            mapped_part_index=part_index,
            map_error=(
                "selected JSON part is not available as structured JSON"
            ),
        )
    try:
        roots = run_full_mapping(
            selected.value,
            max_roots=config.max_root_discovery_k,
        )
        schemas = build_exact_schema(
            json_target=selected.value,
            roots=roots,
            payload_hash_full=payload_hash_full,
        )
        # resolve_json_strings must match what run_full_mapping
        # does internally so _extract_full_records navigates the
        # same structure where roots were discovered.
        from sift_gateway.mapping.json_strings import (
            resolve_json_strings,
        )

        resolved_value = resolve_json_strings(selected.value)
        record_rows = _extract_full_records(resolved_value, roots)
    except Exception as exc:
        return _failed_result(
            map_kind="full",
            mapped_part_index=part_index,
            map_error=f"full mapping error: {exc}",
        )
    return MappingResult(
        map_kind="full",
        map_status="ready",
        mapped_part_index=part_index,
        roots=roots,
        map_budget_fingerprint=None,
        map_backend_id=None,
        prng_version=None,
        map_error=None,
        schemas=schemas,
        record_rows=record_rows,
    )


def _build_partial_config(
    mapping_input: MappingInput,
) -> tuple[Any, Any, str, str]:
    """Build partial mapping configuration from gateway settings.

    Args:
        mapping_input: Mapping input with config and payload hash.

    Returns:
        A tuple of (partial_config, budgets, backend_id,
        fingerprint).
    """
    from sift_gateway.mapping.partial import (
        PartialMappingBudgets,
        PartialMappingConfig,
        compute_map_backend_id,
        compute_map_budget_fingerprint,
    )

    cfg = mapping_input.config
    budgets = PartialMappingBudgets(
        max_bytes_read=cfg.max_bytes_read_partial_map,
        max_compute_steps=cfg.max_compute_steps_partial_map,
        max_depth=cfg.max_depth_partial_map,
        max_records_sampled=cfg.max_records_sampled_partial,
        max_record_bytes=cfg.max_record_bytes_partial,
        max_leaf_paths=cfg.max_leaf_paths_partial,
        max_root_discovery_depth=cfg.max_root_discovery_depth,
    )
    backend_id = compute_map_backend_id()
    fingerprint = compute_map_budget_fingerprint(budgets, backend_id)
    partial_config = PartialMappingConfig(
        payload_hash_full=mapping_input.payload_hash_full,
        budgets=budgets,
        map_budget_fingerprint=fingerprint,
    )
    return partial_config, budgets, backend_id, fingerprint


def _open_partial_stream(
    mapping_input: MappingInput,
    selected: SelectedJsonPart,
) -> tuple[BinaryIO, bool] | MappingResult:
    """Open the byte stream for partial mapping.

    For binary_ref parts, delegate to the open_binary_stream
    callback.  For in-memory JSON values, serialize to a
    BytesIO.

    Args:
        mapping_input: Mapping input with stream callback.
        selected: The chosen JSON part to stream.

    Returns:
        A tuple of (stream, should_close) on success, or a
        failed MappingResult on error.
    """
    from sift_gateway.constants import PRNG_VERSION
    from sift_gateway.mapping.json_strings import resolve_json_strings

    _, _, backend_id, fingerprint = _build_partial_config(
        mapping_input,
    )
    binary_hash = selected.binary_hash
    value = selected.value
    part_index = selected.part_index

    if binary_hash is not None:
        if mapping_input.open_binary_stream is None:
            return _failed_result(
                map_kind="partial",
                mapped_part_index=part_index,
                map_budget_fingerprint=fingerprint,
                map_backend_id=backend_id,
                prng_version=PRNG_VERSION,
                map_error=(
                    "partial mapping requires binary stream"
                    " support for JSON binary_ref"
                ),
            )
        stream = mapping_input.open_binary_stream(binary_hash)
        return stream, True

    if value is None:
        return _failed_result(
            map_kind="partial",
            mapped_part_index=part_index,
            map_budget_fingerprint=fingerprint,
            map_backend_id=backend_id,
            prng_version=PRNG_VERSION,
            map_error=(
                "selected JSON part is missing structured"
                " value for partial mapping"
            ),
        )
    normalized_value = resolve_json_strings(value)
    spool = tempfile.SpooledTemporaryFile(  # noqa: SIM115
        mode="w+b",
        max_size=_PARTIAL_MAP_SPOOL_MAX_MEMORY_BYTES,
    )
    try:
        encoder = json.JSONEncoder(
            separators=(",", ":"),
            sort_keys=True,
        )
        for chunk in encoder.iterencode(normalized_value):
            spool.write(chunk.encode("utf-8"))
        spool.seek(0)
        return cast(BinaryIO, spool), True
    except Exception:
        spool.close()
        raise


def _run_partial_mapping(
    mapping_input: MappingInput,
    selected: SelectedJsonPart,
) -> MappingResult:
    """Execute streaming partial mapping on the selected part.

    Args:
        mapping_input: Mapping input with config and stream
            callback.
        selected: The chosen JSON part to map.

    Returns:
        A MappingResult with kind "partial" and status "ready"
        or "failed".
    """
    from sift_gateway.constants import PRNG_VERSION
    from sift_gateway.mapping.partial import (
        run_partial_mapping,
    )

    partial_config, _, backend_id, fingerprint = _build_partial_config(
        mapping_input
    )
    part_index = selected.part_index

    stream_or_err = _open_partial_stream(mapping_input, selected)
    if isinstance(stream_or_err, MappingResult):
        return stream_or_err
    stream, close_stream = stream_or_err

    try:
        roots, samples = run_partial_mapping(stream, partial_config)
    except Exception as exc:
        return _failed_result(
            map_kind="partial",
            mapped_part_index=part_index,
            map_budget_fingerprint=fingerprint,
            map_backend_id=backend_id,
            prng_version=PRNG_VERSION,
            map_error=f"partial mapping error: {exc}",
        )
    finally:
        if close_stream:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

    roots, samples = _collapse_partial_to_canonical_root(
        roots=roots,
        samples=samples,
    )
    record_rows = [
        RecordRow(s.root_path, s.sample_index, s.record) for s in samples
    ]
    return MappingResult(
        map_kind="partial",
        map_status="ready",
        mapped_part_index=part_index,
        roots=roots,
        map_budget_fingerprint=fingerprint,
        map_backend_id=backend_id,
        prng_version=PRNG_VERSION,
        map_error=None,
        samples=samples,
        schemas=build_sampled_schema(
            roots=roots,
            samples=samples,
            payload_hash_full=mapping_input.payload_hash_full,
            map_budget_fingerprint=fingerprint,
        ),
        record_rows=record_rows,
    )


def _collapse_partial_to_canonical_root(
    *,
    roots: list[RootInventory],
    samples: list[SampleRecord],
) -> tuple[list[RootInventory], list[SampleRecord]]:
    """Collapse partial-mapped roots into one canonical ``$`` root.

    Partial mapping discovers sample-bearing array roots. For
    downstream query consistency, present a single canonical
    root at ``$`` while preserving source index signal.

    Args:
        roots: Partial mapper discovered roots.
        samples: Partial mapper sampled records across roots.

    Returns:
        A tuple with exactly one canonical root and its
        canonicalized samples.
    """
    sorted_samples = sorted(
        samples,
        key=lambda sample: (
            str(sample.root_path),
            int(sample.sample_index),
            str(sample.record_hash),
            int(sample.record_bytes),
        ),
    )
    root_sample_widths: dict[str, int] = {}
    for sample in sorted_samples:
        root_path = str(sample.root_path)
        sample_index = int(sample.sample_index)
        if sample_index < 0:
            continue
        root_sample_widths[root_path] = max(
            root_sample_widths.get(root_path, 0),
            sample_index + 1,
        )
    root_offsets: dict[str, int] = {}
    running_offset = 0
    for root_path in sorted(root_sample_widths.keys()):
        root_offsets[root_path] = running_offset
        running_offset += root_sample_widths[root_path]

    collapsed_samples = [
        SampleRecord(
            root_key="$",
            root_path="$",
            sample_index=(
                root_offsets.get(str(sample.root_path), 0)
                + int(sample.sample_index)
            ),
            record=sample.record,
            record_bytes=sample.record_bytes,
            record_hash=sample.record_hash,
        )
        for sample in sorted_samples
    ]
    sample_indices = [sample.sample_index for sample in collapsed_samples]

    elements_seen_total = 0
    skipped_oversize_total = 0
    source_root_paths: list[str] = []
    prefix_coverage = False
    stop_reason: str | None = None
    count_estimate_sum = 0
    count_estimate_complete = bool(roots)
    merged_fields_top: dict[str, dict[str, int]] = {}
    merged_path_stats: dict[str, dict[str, Any]] = {}
    for root in roots:
        source_root_paths.append(root.root_path)
        prefix_coverage = prefix_coverage or bool(root.prefix_coverage)
        if (
            stop_reason is None
            and isinstance(root.stop_reason, str)
            and root.stop_reason
            and root.stop_reason != "none"
        ):
            stop_reason = root.stop_reason
        if isinstance(root.count_estimate, int) and root.count_estimate >= 0:
            count_estimate_sum += root.count_estimate
        else:
            count_estimate_complete = False
        fields_top = root.fields_top
        if isinstance(fields_top, dict):
            for field_name, raw_counts in fields_top.items():
                if not isinstance(raw_counts, dict):
                    continue
                merged_counts = merged_fields_top.setdefault(
                    str(field_name),
                    {},
                )
                for type_name, raw_count in raw_counts.items():
                    if not isinstance(raw_count, int) or raw_count < 0:
                        continue
                    merged_counts[str(type_name)] = (
                        merged_counts.get(str(type_name), 0) + raw_count
                    )

        summary = root.root_summary if isinstance(root.root_summary, dict) else None
        seen_added = False
        if summary is not None:
            seen_raw = summary.get("elements_seen")
            if isinstance(seen_raw, int) and seen_raw >= 0:
                elements_seen_total += seen_raw
                seen_added = True
            skipped_raw = summary.get("skipped_oversize_records")
            if not isinstance(skipped_raw, int):
                skipped_raw = summary.get("skipped_oversize")
            if isinstance(skipped_raw, int) and skipped_raw >= 0:
                skipped_oversize_total += skipped_raw

        if not seen_added and isinstance(
            root.sampled_prefix_len, int
        ) and root.sampled_prefix_len >= 0:
            elements_seen_total += root.sampled_prefix_len

        path_stats = root.path_stats
        if isinstance(path_stats, dict):
            for path in sorted(path_stats.keys()):
                raw_stats = path_stats.get(path)
                if not isinstance(raw_stats, dict):
                    continue
                merged = merged_path_stats.setdefault(
                    str(path),
                    {
                        "types": set(),
                        "observed_count": 0,
                        "example_value": None,
                    },
                )
                raw_types = raw_stats.get("types")
                if isinstance(raw_types, list):
                    merged_types = merged["types"]
                    if isinstance(merged_types, set):
                        merged_types.update(
                            str(type_name) for type_name in raw_types
                        )
                raw_observed_count = raw_stats.get("observed_count")
                if (
                    isinstance(raw_observed_count, int)
                    and raw_observed_count >= 0
                ):
                    merged["observed_count"] = int(merged["observed_count"]) + (
                        raw_observed_count
                    )
                raw_example = raw_stats.get("example_value")
                if merged["example_value"] is None and raw_example is not None:
                    merged["example_value"] = raw_example

    if not roots:
        canonical_shape: str | None = None
    elif len(roots) == 1 and roots[0].root_path == "$":
        canonical_shape = roots[0].root_shape
    else:
        canonical_shape = "object"
    canonical_count_estimate: int | None = (
        count_estimate_sum if count_estimate_complete else None
    )
    canonical_fields_top: dict[str, Any] | None = (
        dict(sorted(merged_fields_top.items()))
        if merged_fields_top
        else None
    )

    inventory_coverage: float | None = None
    if elements_seen_total > 0:
        inventory_coverage = len(sample_indices) / float(elements_seen_total)

    canonical_path_stats: dict[str, Any] | None = None
    if merged_path_stats:
        canonical_path_stats = {}
        for path in sorted(merged_path_stats.keys()):
            merged = merged_path_stats[path]
            raw_types = merged.get("types")
            types: list[str] = []
            if isinstance(raw_types, set):
                types = sorted(str(type_name) for type_name in raw_types)
            observed_count_raw = merged.get("observed_count")
            observed_count = (
                int(observed_count_raw)
                if isinstance(observed_count_raw, int) and observed_count_raw >= 0
                else 0
            )
            canonical_path_stats[path] = {
                "types": types,
                "observed_count": observed_count,
                "example_value": merged.get("example_value"),
            }

    root_summary: dict[str, Any] = {
        "elements_seen": elements_seen_total,
        "sampled_record_count": len(sample_indices),
        "sampled_prefix_len": elements_seen_total,
        "prefix_coverage": prefix_coverage,
        "stop_reason": stop_reason or "none",
        "skipped_oversize": skipped_oversize_total,
        "skipped_oversize_records": skipped_oversize_total,
        "collapsed_from_partial_roots": True,
        "source_root_count": len(roots),
        "source_root_paths": sorted(set(source_root_paths)),
    }
    canonical_root = RootInventory(
        root_key="$",
        root_path="$",
        count_estimate=canonical_count_estimate,
        root_shape=canonical_shape,
        fields_top=canonical_fields_top,
        root_summary=root_summary,
        inventory_coverage=inventory_coverage,
        root_score=float(elements_seen_total),
        sample_indices=sample_indices,
        prefix_coverage=prefix_coverage,
        stop_reason=(stop_reason if prefix_coverage else None),
        sampled_prefix_len=elements_seen_total,
        path_stats=canonical_path_stats,
    )
    return [canonical_root], collapsed_samples


def run_mapping(
    mapping_input: MappingInput,
) -> MappingResult:
    """Route an artifact to full or partial mapping.

    Select the best JSON part, then dispatch to full mapping
    for small payloads or partial mapping for large/binary-ref
    payloads based on ``max_full_map_bytes``.

    Args:
        mapping_input: Complete mapping input with envelope,
            config, and optional binary stream callback.

    Returns:
        A MappingResult from the selected mapping strategy.
    """
    config = mapping_input.config
    envelope = mapping_input.envelope

    selected = select_json_part(envelope)
    if selected is None:
        return _failed_result(
            map_kind="full",
            mapped_part_index=None,
            map_error=("no JSON content part found in envelope"),
        )

    binary_hash = selected.binary_hash
    byte_size = selected.byte_size
    if binary_hash is None and byte_size > config.max_in_memory_mapping_bytes:
        return _failed_result(
            map_kind="partial",
            mapped_part_index=selected.part_index,
            map_error=(
                "selected JSON part exceeds max_in_memory_mapping_bytes "
                f"({byte_size} > {config.max_in_memory_mapping_bytes})"
            ),
        )
    use_partial = (
        binary_hash is not None or byte_size > config.max_full_map_bytes
    )

    if not use_partial:
        return _run_full_mapping(
            selected,
            config,
            mapping_input.payload_hash_full,
        )
    return _run_partial_mapping(mapping_input, selected)
