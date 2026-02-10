"""Mapping orchestrator: picks JSON part, decides full vs partial."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable

from mcp_artifact_gateway.config.settings import GatewayConfig


@dataclass(frozen=True)
class RootInventory:
    """A discovered root in the mapped data."""

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
    """A sampled record from partial mapping."""

    root_key: str
    root_path: str
    sample_index: int
    record: dict[str, Any]
    record_bytes: int
    record_hash: str


@dataclass(frozen=True)
class MappingInput:
    """Input for the mapping system."""

    artifact_id: str
    payload_hash_full: str
    envelope: dict[str, Any]
    config: GatewayConfig
    open_binary_stream: Callable[[str], BinaryIO] | None = None


@dataclass(frozen=True)
class SelectedJsonPart:
    """A selected JSON-compatible content part for mapping."""

    part_index: int
    byte_size: int
    value: Any | None = None
    binary_hash: str | None = None


@dataclass(frozen=True)
class MappingResult:
    """Result of running mapping on an artifact."""

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
    if not isinstance(raw_mime, str):
        return False
    mime = raw_mime.split(";", 1)[0].strip().lower()
    return mime == "application/json" or mime.startswith("application/json+")


def select_json_part(envelope: dict[str, Any]) -> SelectedJsonPart | None:
    """Deterministic scoring to pick the best JSON part from envelope content.

    Returns a selected part or None.
    Scoring: prefer larger JSON parts. Tie-break by part index ascending.
    """
    content = envelope.get("content", [])
    best: SelectedJsonPart | None = None

    for i, part in enumerate(content):
        if not isinstance(part, dict):
            continue
        part_type = part.get("type")
        candidate: SelectedJsonPart | None = None
        if part_type == "json":
            value = part.get("value")
            if value is None:
                continue
            serialized = json.dumps(value, separators=(",", ":"), sort_keys=True)
            byte_size = len(serialized.encode("utf-8"))
            candidate = SelectedJsonPart(
                part_index=i,
                byte_size=byte_size,
                value=value,
            )
        elif part_type == "binary_ref" and _is_json_binary_mime(part.get("mime")):
            binary_hash = part.get("binary_hash")
            byte_count = part.get("byte_count")
            if not isinstance(binary_hash, str) or not binary_hash:
                continue
            if not isinstance(byte_count, int) or byte_count < 0:
                continue
            candidate = SelectedJsonPart(
                part_index=i,
                byte_size=byte_count,
                binary_hash=binary_hash,
            )

        if candidate is None:
            continue

        # Prefer larger; tie-break by ascending index (first wins)
        if best is None or candidate.byte_size > best.byte_size:
            best = candidate

    return best


def run_mapping(mapping_input: MappingInput) -> MappingResult:
    """Decide full vs partial based on size vs max_full_map_bytes, call the right mapper."""
    config = mapping_input.config
    envelope = mapping_input.envelope

    selected = select_json_part(envelope)
    if selected is None:
        return MappingResult(
            map_kind="full",
            map_status="failed",
            mapped_part_index=None,
            roots=[],
            map_budget_fingerprint=None,
            map_backend_id=None,
            prng_version=None,
            map_error="no JSON content part found in envelope",
        )

    value = selected.value
    part_index = selected.part_index
    byte_size = selected.byte_size
    binary_hash = selected.binary_hash
    use_partial = binary_hash is not None or byte_size > config.max_full_map_bytes

    if not use_partial:
        from mcp_artifact_gateway.mapping.full import run_full_mapping

        if value is None:
            return MappingResult(
                map_kind="full",
                map_status="failed",
                mapped_part_index=part_index,
                roots=[],
                map_budget_fingerprint=None,
                map_backend_id=None,
                prng_version=None,
                map_error="selected JSON part is not available as structured JSON",
            )
        try:
            roots = run_full_mapping(value, max_roots=config.max_root_discovery_k)
        except Exception as exc:
            return MappingResult(
                map_kind="full",
                map_status="failed",
                mapped_part_index=part_index,
                roots=[],
                map_budget_fingerprint=None,
                map_backend_id=None,
                prng_version=None,
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

    # Partial mapping for large payloads
    import io

    from mcp_artifact_gateway.constants import PRNG_VERSION
    from mcp_artifact_gateway.mapping.partial import (
        PartialMappingBudgets,
        PartialMappingConfig,
        compute_map_backend_id,
        compute_map_budget_fingerprint,
        run_partial_mapping,
    )

    budgets = PartialMappingBudgets(
        max_bytes_read=config.max_bytes_read_partial_map,
        max_compute_steps=config.max_compute_steps_partial_map,
        max_depth=config.max_depth_partial_map,
        max_records_sampled=config.max_records_sampled_partial,
        max_record_bytes=config.max_record_bytes_partial,
        max_leaf_paths=config.max_leaf_paths_partial,
        max_root_discovery_depth=config.max_root_discovery_depth,
    )

    backend_id = compute_map_backend_id()
    fingerprint = compute_map_budget_fingerprint(budgets, backend_id)
    partial_config = PartialMappingConfig(
        payload_hash_full=mapping_input.payload_hash_full,
        budgets=budgets,
        map_budget_fingerprint=fingerprint,
    )

    stream: BinaryIO
    close_stream = False
    if binary_hash is not None:
        if mapping_input.open_binary_stream is None:
            return MappingResult(
                map_kind="partial",
                map_status="failed",
                mapped_part_index=part_index,
                roots=[],
                map_budget_fingerprint=fingerprint,
                map_backend_id=backend_id,
                prng_version=PRNG_VERSION,
                map_error="partial mapping requires binary stream support for JSON binary_ref",
            )
        stream = mapping_input.open_binary_stream(binary_hash)
        close_stream = True
    else:
        if value is None:
            return MappingResult(
                map_kind="partial",
                map_status="failed",
                mapped_part_index=part_index,
                roots=[],
                map_budget_fingerprint=fingerprint,
                map_backend_id=backend_id,
                prng_version=PRNG_VERSION,
                map_error="selected JSON part is missing structured value for partial mapping",
            )
        serialized_bytes = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
        stream = io.BytesIO(serialized_bytes)

    try:
        roots, samples = run_partial_mapping(stream, partial_config)
    except Exception as exc:
        return MappingResult(
            map_kind="partial",
            map_status="failed",
            mapped_part_index=part_index,
            roots=[],
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
