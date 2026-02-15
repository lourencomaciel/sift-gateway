"""Perform streaming partial mapping with budget enforcement.

Stream-parse large JSON payloads via ijson, discover array
roots up to a configurable depth, and collect representative
records using deterministic reservoir sampling (Xoshiro256**
PRNG).  Enforce byte, compute-step, and depth budgets.  Key
exports are ``run_partial_mapping``, ``PartialMappingConfig``,
``PartialMappingBudgets``, and fingerprint helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import heapq
import json
import struct
import sys
from typing import Any, BinaryIO

import ijson  # type: ignore[import-untyped]

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.constants import (
    MAPPER_VERSION,
    PRNG_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
)
from sift_mcp.mapping.runner import RootInventory, SampleRecord
from sift_mcp.util.hashing import (
    map_budget_fingerprint as _compute_mbf,
)
from sift_mcp.util.hashing import sha256_hex

# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PartialMappingBudgets:
    """Resource budgets enforced during streaming partial mapping.

    Attributes:
        max_bytes_read: Maximum raw bytes to read from stream.
        max_compute_steps: Maximum ijson parse events.
        max_depth: Maximum JSON nesting depth allowed.
        max_records_sampled: Reservoir capacity per root.
        max_record_bytes: Maximum canonical bytes per record.
        max_leaf_paths: Maximum leaf paths to track.
        max_root_discovery_depth: Max depth for root search.
    """

    max_bytes_read: int
    max_compute_steps: int
    max_depth: int
    max_records_sampled: int
    max_record_bytes: int
    max_leaf_paths: int
    max_root_discovery_depth: int


@dataclass(frozen=True)
class PartialMappingConfig:
    """Immutable configuration for a single partial mapping run.

    Attributes:
        payload_hash_full: SHA-256 hex of the canonical payload
            (used to seed the deterministic PRNG).
        budgets: Resource budget limits for the run.
        map_budget_fingerprint: Deterministic fingerprint of
            the budget configuration and runtime.
    """

    payload_hash_full: str
    budgets: PartialMappingBudgets
    map_budget_fingerprint: str


@dataclass
class StreamingState:
    """Mutable progress counters during streaming parse.

    Attributes:
        bytes_read: Total raw bytes consumed so far.
        compute_steps: Total ijson events processed.
        current_depth: Current JSON nesting depth.
        max_depth_seen: Deepest nesting depth encountered.
        stop_reason: Why parsing stopped: "none",
            "max_bytes", "max_compute", "max_depth",
            or "parse_error".
        elements_recognized: Total elements parsed so far.
        skipped_oversize_records: Records exceeding budget.
    """

    bytes_read: int = 0
    compute_steps: int = 0
    current_depth: int = 0
    max_depth_seen: int = 0
    stop_reason: str = (
        "none"  # none|max_bytes|max_compute|max_depth|parse_error
    )
    elements_recognized: int = 0  # sampled_prefix_len
    skipped_oversize_records: int = 0


# ---------------------------------------------------------------------------
# Deterministic PRNG: Xoshiro256** variant
# ---------------------------------------------------------------------------


class DeterministicPRNG:
    """Seeded Xoshiro256** PRNG for deterministic reservoir sampling.

    Initialize from seed bytes via SplitMix64 expansion into
    four uint64 state words, then generate uniform floats for
    reservoir key assignment.  Version: prng_xoshiro256ss_v1.
    """

    def __init__(self, seed_bytes: bytes) -> None:
        """Initialize state from seed bytes via SplitMix64 expansion.

        Args:
            seed_bytes: At least 8 bytes of seed material.
                Shorter inputs are zero-padded.
        """
        # Take first 8 bytes of seed as the SplitMix64 seed
        if len(seed_bytes) < 8:
            seed_bytes = seed_bytes + b"\x00" * (8 - len(seed_bytes))
        sm_state = struct.unpack("<Q", seed_bytes[:8])[0]
        # Expand to 4 x uint64 state via SplitMix64
        self._s: list[int] = []
        mask64 = (1 << 64) - 1
        for _ in range(4):
            sm_state = (sm_state + 0x9E3779B97F4A7C15) & mask64
            z = sm_state
            z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & mask64
            z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & mask64
            z = z ^ (z >> 31)
            self._s.append(z & mask64)
        # Deterministic fallback: all-zero state is invalid.
        if all(s == 0 for s in self._s):
            self._s[0] = 1

    def _rotl(self, x: int, k: int) -> int:
        """Rotate a 64-bit integer left by k bits.

        Args:
            x: 64-bit unsigned integer value.
            k: Number of bit positions to rotate.

        Returns:
            The rotated 64-bit value.
        """
        mask64 = (1 << 64) - 1
        return ((x << k) | (x >> (64 - k))) & mask64

    def next_u64(self) -> int:
        """Generate next uint64 via Xoshiro256** algorithm.

        Returns:
            A 64-bit unsigned integer.
        """
        mask64 = (1 << 64) - 1
        s = self._s
        result = (self._rotl((s[1] * 5) & mask64, 7) * 9) & mask64

        t = (s[1] << 17) & mask64

        s[2] ^= s[0]
        s[3] ^= s[1]
        s[1] ^= s[2]
        s[0] ^= s[3]

        s[2] ^= t
        s[3] = self._rotl(s[3], 45)

        # Mask all to 64-bit
        for i in range(4):
            s[i] = s[i] & mask64

        return result

    def next_float(self) -> float:
        """Return a uniform float in [0, 1).

        Returns:
            A double-precision float in the half-open interval.
        """
        return (self.next_u64() >> 11) * (1.0 / (1 << 53))


# ---------------------------------------------------------------------------
# Backend and fingerprint computation
# ---------------------------------------------------------------------------


def compute_map_backend_id() -> str:
    """Compute a runtime backend identifier hash.

    Combine the Python version, ijson backend name, and ijson
    version into a truncated SHA-256 hex digest.

    Returns:
        A 16-char hex string identifying the runtime backend.
    """
    vi = sys.version_info
    py_ver = f"{vi.major}.{vi.minor}.{vi.micro}"
    backend = ijson.backend
    ijson_ver = ijson.__version__
    identity = f"py={py_ver}|ijson={backend}|ijson_ver={ijson_ver}"
    return sha256_hex(identity.encode("utf-8"))[:16]


def compute_map_budget_fingerprint(
    budgets: PartialMappingBudgets,
    map_backend_id: str,
) -> str:
    """Compute a deterministic fingerprint of the mapping config.

    Combine all budget fields with version strings and the
    backend ID, then delegate to the canonical hashing
    implementation for consistent 32-char hex truncation.

    Args:
        budgets: Resource budget limits for the mapping run.
        map_backend_id: Runtime backend identifier hash.

    Returns:
        A 32-char truncated SHA-256 hex digest.
    """
    budget_dict = {
        "max_bytes_read": budgets.max_bytes_read,
        "max_compute_steps": budgets.max_compute_steps,
        "max_depth": budgets.max_depth,
        "max_records_sampled": budgets.max_records_sampled,
        "max_record_bytes": budgets.max_record_bytes,
        "max_leaf_paths": budgets.max_leaf_paths,
        "max_root_discovery_depth": budgets.max_root_discovery_depth,
    }
    return _compute_mbf(
        mapper_version=MAPPER_VERSION,
        traversal_contract_version=TRAVERSAL_CONTRACT_VERSION,
        map_backend_id=map_backend_id,
        prng_version=PRNG_VERSION,
        budgets=budget_dict,
    )


# ---------------------------------------------------------------------------
# Reservoir sampling helpers
# ---------------------------------------------------------------------------


def _make_reservoir_seed(
    payload_hash_full: str,
    root_path: str,
    map_budget_fingerprint: str,
) -> bytes:
    """Compute a deterministic PRNG seed for reservoir sampling.

    Args:
        payload_hash_full: SHA-256 hex of canonical payload.
        root_path: Canonical JSONPath of the root.
        map_budget_fingerprint: Budget configuration hash.

    Returns:
        32 bytes of SHA-256 seed material.
    """
    seed_input = f"{payload_hash_full}|{root_path}|{map_budget_fingerprint}"
    return hashlib.sha256(seed_input.encode("utf-8")).digest()


@dataclass
class _ReservoirEntry:
    """Single candidate entry in the reservoir sample.

    Attributes:
        random_key: PRNG-assigned key for heap ordering.
        index: Zero-based element index in the root array.
        record: The parsed JSON object.
        record_bytes: Canonical byte size of the record.
        record_hash: SHA-256 hex digest of canonical record.
    """

    random_key: float
    index: int
    record: dict[str, Any]
    record_bytes: int
    record_hash: str


_ReservoirHeapEntry = tuple[float, int, _ReservoirEntry]


@dataclass
class _RootState:
    """Mutable tracking state for one discovered root during streaming.

    Attributes:
        root_key: Key identifying this root.
        root_path: Canonical JSONPath to the root.
        root_shape: "array" or "object".
        prng: Seeded PRNG for reservoir sampling.
        reservoir: Min-heap of negative-keyed entries for
            O(log n) eviction of the largest random key.
        max_reservoir_size: Maximum reservoir capacity.
        elements_seen: Total elements encountered so far.
        array_closed: True when end_array event received.
        field_types: Field name to type count distribution.
        leaf_paths_seen: Number of leaf paths tracked.
    """

    root_key: str
    root_path: str
    root_shape: str  # "array" | "object"
    prng: DeterministicPRNG
    # Max-heap encoded via negative random_key so we can evict in O(log n).
    reservoir: list[_ReservoirHeapEntry] = field(default_factory=list)
    max_reservoir_size: int = 100
    elements_seen: int = 0
    array_closed: bool = False
    field_types: dict[str, dict[str, int]] = field(default_factory=dict)
    path_stats: dict[str, _PathStats] = field(default_factory=dict)
    leaf_paths_seen: int = 0
    skipped_oversize_records: int = 0


# ---------------------------------------------------------------------------
# JSON type name helper
# ---------------------------------------------------------------------------


def _json_type_name(value: Any) -> str:
    """Return a JSON-style type name for a Python value.

    Args:
        value: Any Python value to classify.

    Returns:
        One of "null", "boolean", "number", "string",
        "array", "object", or the Python type name.
    """
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


def _update_field_types(
    field_types: dict[str, dict[str, int]],
    record: dict[str, Any],
) -> None:
    """Update field type distribution from a record.

    Args:
        field_types: Mutable field-to-type-count mapping to
            update in place.
        record: A parsed JSON object whose fields are counted.
    """
    for key, val in record.items():
        type_name = _json_type_name(val)
        if key not in field_types:
            field_types[key] = {}
        field_types[key][type_name] = field_types[key].get(type_name, 0) + 1


@dataclass
class _PathStats:
    """Aggregated per-path observations across streamed records."""

    types: set[str]
    observed_count: int
    example_value: Any | None


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


def _walk_value(
    value: Any,
    *,
    path: str,
    stats: dict[str, _PathStats],
    seen_paths: set[str],
) -> None:
    """Collect path/type observations for one record value."""
    existing = stats.get(path)
    if existing is None:
        existing = _PathStats(
            types=set(),
            observed_count=0,
            example_value=value,
        )
        stats[path] = existing
    elif existing.example_value is None:
        existing.example_value = value
    existing.types.add(_json_type_name(value))
    seen_paths.add(path)

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


# ---------------------------------------------------------------------------
# Byte-counted stream wrapper
# ---------------------------------------------------------------------------


class _CountingStream:
    """Binary stream wrapper that tracks cumulative bytes read.

    Attributes:
        _stream: The underlying binary stream.
        _state: Shared streaming state for byte accounting.
    """

    def __init__(self, stream: BinaryIO, state: StreamingState) -> None:
        """Wrap a binary stream with byte accounting.

        Args:
            stream: The underlying binary stream.
            state: Shared streaming state for byte tracking.
        """
        self._stream = stream
        self._state = state

    def read(self, size: int = -1) -> bytes:
        """Read bytes and update the cumulative byte counter.

        Args:
            size: Maximum bytes to read, or -1 for all.

        Returns:
            The bytes read from the underlying stream.
        """
        data = self._stream.read(size)
        self._state.bytes_read += len(data)
        return data


# ---------------------------------------------------------------------------
# Streaming partial mapping implementation
# ---------------------------------------------------------------------------


def run_partial_mapping(
    stream: BinaryIO,
    config: PartialMappingConfig,
) -> tuple[list[RootInventory], list[SampleRecord]]:
    """Run streaming partial mapping with budget enforcement.

    Stream-parse JSON via ijson, discover array roots up to
    the configured depth, and collect representative records
    using deterministic reservoir sampling.  Enforce byte,
    compute-step, and depth budgets during parsing.

    Args:
        stream: Binary stream of JSON content to parse.
        config: Partial mapping configuration with budgets,
            payload hash, and budget fingerprint.

    Returns:
        A tuple of (root_inventories, sample_records) sorted
        by root score descending.
    """
    budgets = config.budgets
    state = StreamingState()
    counting_stream = _CountingStream(stream, state)

    # Root discovery state
    roots: dict[str, _RootState] = {}
    # Track the current element being built for each root
    current_elements: dict[
        str, list[Any]
    ] = {}  # root_key -> stack of partial values
    current_element_keys: dict[
        str, list[str | None]
    ] = {}  # root_key -> stack of keys
    # Track which root is active and at what depth
    active_root: str | None = None
    element_depth: int = 0
    in_element: bool = False

    # Phase 1: Use ijson.parse for streaming event processing
    try:
        parser = ijson.parse(counting_stream, use_float=True)

        for prefix, event, value in parser:
            state.compute_steps += 1

            # Check budget limits
            if state.bytes_read > budgets.max_bytes_read:
                state.stop_reason = "max_bytes"
                break
            if state.compute_steps > budgets.max_compute_steps:
                state.stop_reason = "max_compute"
                break

            # Track depth from prefix
            depth = prefix.count(".") + (1 if prefix else 0)
            if depth > state.max_depth_seen:
                state.max_depth_seen = depth

            if depth > budgets.max_depth:
                state.stop_reason = "max_depth"
                break

            # Root discovery: look for arrays at configurable depth
            if (
                not in_element
                and active_root is None
                and depth <= budgets.max_root_discovery_depth
            ):
                if event == "start_array":
                    root_path = _prefix_to_jsonpath(prefix)
                    root_key = prefix if prefix else "$"
                    if root_key not in roots:
                        seed = _make_reservoir_seed(
                            config.payload_hash_full,
                            root_path,
                            config.map_budget_fingerprint,
                        )
                        root_state = _RootState(
                            root_key=root_key,
                            root_path=root_path,
                            root_shape="array",
                            prng=DeterministicPRNG(seed),
                            max_reservoir_size=budgets.max_records_sampled,
                        )
                        roots[root_key] = root_state
                        active_root = root_key
                    continue

            # If we have an active root, process elements within it
            if active_root is not None and active_root in roots:
                root_state = roots[active_root]

                if not in_element:
                    # We are at the root array level, looking for element starts
                    if (
                        event == "end_array"
                        and _prefix_to_jsonpath(prefix) == root_state.root_path
                    ):
                        root_state.array_closed = True
                        active_root = None
                        continue

                    if event in ("start_map", "start_array"):
                        # Start of an element
                        in_element = True
                        element_depth = 1
                        if event == "start_map":
                            current_elements[active_root] = [{}]
                            current_element_keys[active_root] = [None]
                        else:
                            current_elements[active_root] = [[]]
                            current_element_keys[active_root] = [None]
                        continue

                    if event in ("string", "number", "boolean", "null"):
                        # Scalar element in array
                        state.elements_recognized += 1
                        root_state.elements_seen += 1
                        continue

                elif in_element:
                    # Inside an element, building it up
                    stack = current_elements.get(active_root, [])
                    key_stack = current_element_keys.get(active_root, [])

                    if event == "map_key":
                        if key_stack:
                            key_stack[-1] = value
                        continue

                    if event == "start_map":
                        element_depth += 1
                        new_obj: dict[str, Any] = {}
                        stack.append(new_obj)
                        key_stack.append(None)
                        continue

                    if event == "start_array":
                        element_depth += 1
                        new_arr: list[Any] = []
                        stack.append(new_arr)
                        key_stack.append(None)
                        continue

                    if event == "end_map":
                        element_depth -= 1
                        if element_depth == 0:
                            # Element complete
                            record = stack[0] if stack else {}
                            _finalize_element(
                                root_state, record, state, budgets
                            )
                            in_element = False
                            current_elements.pop(active_root, None)
                            current_element_keys.pop(active_root, None)
                            continue
                        # Nested object closed: pop and assign to parent
                        if len(stack) > 1:
                            completed = stack.pop()
                            key_stack.pop()
                            parent = stack[-1]
                            parent_key = key_stack[-1]
                            if (
                                isinstance(parent, dict)
                                and parent_key is not None
                            ):
                                parent[parent_key] = completed
                            elif isinstance(parent, list):
                                parent.append(completed)
                        continue

                    if event == "end_array":
                        element_depth -= 1
                        if element_depth == 0:
                            # Element complete (array element)
                            record_val = stack[0] if stack else []
                            if isinstance(record_val, dict):
                                _finalize_element(
                                    root_state, record_val, state, budgets
                                )
                            else:
                                state.elements_recognized += 1
                                root_state.elements_seen += 1
                            in_element = False
                            current_elements.pop(active_root, None)
                            current_element_keys.pop(active_root, None)
                            continue
                        # Nested array closed
                        if len(stack) > 1:
                            completed = stack.pop()
                            key_stack.pop()
                            parent = stack[-1]
                            parent_key = key_stack[-1]
                            if (
                                isinstance(parent, dict)
                                and parent_key is not None
                            ):
                                parent[parent_key] = completed
                            elif isinstance(parent, list):
                                parent.append(completed)
                        continue

                    # Scalar value inside element
                    if stack:
                        parent = stack[-1]
                        parent_key = key_stack[-1] if key_stack else None
                        if isinstance(parent, dict) and parent_key is not None:
                            parent[parent_key] = value
                        elif isinstance(parent, list):
                            parent.append(value)
                    continue

    except Exception:
        if state.stop_reason == "none":
            state.stop_reason = "parse_error"

    # Build results
    result_roots: list[RootInventory] = []
    result_samples: list[SampleRecord] = []

    for root_key, root_state in roots.items():
        # Determine count_estimate
        count_estimate: int | None = None
        if state.stop_reason == "none" and root_state.array_closed:
            count_estimate = root_state.elements_seen

        prefix_coverage = state.stop_reason != "none"

        # Build fields_top from reservoir samples
        fields_top: dict[str, Any] | None = None
        if root_state.field_types:
            fields_top = dict(root_state.field_types)

        # Sort reservoir by index for deterministic output
        reservoir_entries = [entry for _, _, entry in root_state.reservoir]
        reservoir_entries.sort(key=lambda e: e.index)
        sample_indices = [e.index for e in reservoir_entries]

        # Compute inventory coverage
        inventory_coverage: float | None = None
        if root_state.elements_seen > 0:
            sampled_count = len(root_state.reservoir)
            inventory_coverage = sampled_count / root_state.elements_seen

        root_score = float(root_state.elements_seen)

        # Per-root stop_reason: attribute the global stop to this
        # root only if the root's array was still open (being
        # streamed) when the stop occurred.
        root_stop_reason: str = "none"
        if prefix_coverage and not root_state.array_closed:
            root_stop_reason = state.stop_reason

        root_summary: dict[str, Any] = {
            "elements_seen": root_state.elements_seen,
            "sampled_record_count": len(sample_indices),
            "sampled_prefix_len": root_state.elements_seen,
            "prefix_coverage": prefix_coverage,
            "stop_reason": root_stop_reason,
            "skipped_oversize": root_state.skipped_oversize_records,
            "skipped_oversize_records": (root_state.skipped_oversize_records),
        }

        root_inv = RootInventory(
            root_key=root_key,
            root_path=root_state.root_path,
            count_estimate=count_estimate,
            root_shape=root_state.root_shape,
            fields_top=fields_top,
            root_summary=root_summary,
            inventory_coverage=inventory_coverage,
            root_score=root_score,
            sample_indices=sample_indices,
            prefix_coverage=prefix_coverage,
            stop_reason=(root_stop_reason if prefix_coverage else None),
            sampled_prefix_len=root_state.elements_seen,
            path_stats={
                path: {
                    "types": sorted(stats.types),
                    "observed_count": stats.observed_count,
                    "example_value": stats.example_value,
                }
                for path, stats in sorted(root_state.path_stats.items())
            },
        )
        result_roots.append(root_inv)

        # Build SampleRecords from reservoir
        for entry in reservoir_entries:
            sample = SampleRecord(
                root_key=root_key,
                root_path=root_state.root_path,
                sample_index=entry.index,
                record=entry.record,
                record_bytes=entry.record_bytes,
                record_hash=entry.record_hash,
            )
            result_samples.append(sample)

    # Sort roots by score descending for consistency
    result_roots.sort(key=lambda r: (-r.root_score, r.root_key))

    return result_roots, result_samples


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _prefix_to_jsonpath(prefix: str) -> str:
    """Convert an ijson prefix string to a canonical JSONPath.

    Args:
        prefix: Dot-delimited ijson prefix (e.g. "data.item").

    Returns:
        A canonical JSONPath string (e.g. ``$.data[*]``).
    """
    if not prefix:
        return "$"
    parts = ["$"]
    for segment in prefix.split("."):
        if segment == "item":
            parts.append("[*]")
        elif segment.isdigit():
            parts.append(f"[{segment}]")
        else:
            # Use dot notation for valid identifiers
            import re

            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", segment):
                parts.append(f".{segment}")
            else:
                escaped = (
                    segment.replace("\\", "\\\\")
                    .replace("'", "\\'")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t")
                )
                parts.append(f"['{escaped}']")
    return "".join(parts)


def _finalize_element(
    root_state: _RootState,
    record: dict[str, Any],
    state: StreamingState,
    budgets: PartialMappingBudgets,
) -> None:
    """Finalize a parsed element and offer it to the reservoir.

    Canonicalize the record, check it against the byte budget,
    update field type distributions, and insert or replace in
    the reservoir via random-key heap ordering.

    Args:
        root_state: Mutable state for the containing root.
        record: Parsed JSON object to finalize.
        state: Shared streaming state for counters.
        budgets: Budget limits (max_record_bytes checked here).
    """
    from sift_mcp.mapping.json_strings import resolve_json_strings

    normalized_record = resolve_json_strings(record)
    state.elements_recognized += 1
    root_state.elements_seen += 1
    element_index = root_state.elements_seen - 1

    seen_paths: set[str] = set()
    _walk_value(
        normalized_record,
        path="$",
        stats=root_state.path_stats,
        seen_paths=seen_paths,
    )
    for path in seen_paths:
        stats = root_state.path_stats.get(path)
        if stats is not None:
            stats.observed_count += 1

    # Serialize and check size
    try:
        record_canonical = canonical_bytes(normalized_record)
    except (TypeError, ValueError):
        # Cannot canonicalize -- use JSON fallback for size check
        record_json = json.dumps(
            normalized_record, separators=(",", ":"), sort_keys=True
        )
        record_bytes_data = record_json.encode("utf-8")
        record_canonical = record_bytes_data

    record_byte_count = len(record_canonical)

    if record_byte_count > budgets.max_record_bytes:
        state.skipped_oversize_records += 1
        root_state.skipped_oversize_records += 1
        return

    record_hash = sha256_hex(record_canonical)

    # Update field type distribution
    _update_field_types(root_state.field_types, normalized_record)

    # Reservoir sampling: keep the N items with smallest random keys
    random_key = root_state.prng.next_float()
    entry = _ReservoirEntry(
        random_key=random_key,
        index=element_index,
        record=normalized_record,
        record_bytes=record_byte_count,
        record_hash=record_hash,
    )

    heap_entry: _ReservoirHeapEntry = (-random_key, entry.index, entry)
    if len(root_state.reservoir) < root_state.max_reservoir_size:
        heapq.heappush(root_state.reservoir, heap_entry)
    else:
        # Heap head stores the entry with the largest random key.
        largest_random_key = -root_state.reservoir[0][0]
        if random_key < largest_random_key:
            heapq.heapreplace(root_state.reservoir, heap_entry)
