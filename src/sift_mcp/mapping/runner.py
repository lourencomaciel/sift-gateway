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

from dataclasses import dataclass
import json
from typing import Any, BinaryIO, Callable

from sift_mcp.config.settings import GatewayConfig


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
    serialized = json.dumps(value, separators=(",", ":"), sort_keys=True)
    byte_size = len(serialized.encode("utf-8"))
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


def select_json_part(
    envelope: dict[str, Any],
) -> SelectedJsonPart | None:
    """Select the best JSON-compatible content part for mapping.

    Score all JSON and JSON-mime binary_ref parts by byte size,
    preferring larger parts.  Break ties by ascending index.

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
    )


def _run_full_mapping(
    selected: SelectedJsonPart,
    config: GatewayConfig,
) -> MappingResult:
    """Execute full in-memory mapping on the selected part.

    Args:
        selected: The JSON part chosen for mapping.
        config: Gateway configuration with root discovery limit.

    Returns:
        A MappingResult with kind "full" and status "ready"
        or "failed".
    """
    from sift_mcp.mapping.full import (
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
    from sift_mcp.mapping.partial import (
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
    import io

    from sift_mcp.constants import PRNG_VERSION

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
    serialized_bytes = json.dumps(
        value, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return io.BytesIO(serialized_bytes), False


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
    from sift_mcp.constants import PRNG_VERSION
    from sift_mcp.mapping.partial import (
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
    )


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
    use_partial = (
        binary_hash is not None or byte_size > config.max_full_map_bytes
    )

    if not use_partial:
        return _run_full_mapping(selected, config)
    return _run_partial_mapping(mapping_input, selected)
