"""Partial mapping: streaming JSON analysis with budgets and reservoir sampling."""

from __future__ import annotations

import heapq
import hashlib
import json
import struct
import sys
from dataclasses import dataclass, field
from typing import Any, BinaryIO

import ijson

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.constants import (
    MAPPER_VERSION,
    PRNG_VERSION,
    TRAVERSAL_CONTRACT_VERSION,
)
from mcp_artifact_gateway.mapping.runner import RootInventory, SampleRecord
from mcp_artifact_gateway.util.hashing import map_budget_fingerprint as _compute_mbf
from mcp_artifact_gateway.util.hashing import sha256_hex


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PartialMappingBudgets:
    """All budgets that must be enforced during streaming."""

    max_bytes_read: int
    max_compute_steps: int
    max_depth: int
    max_records_sampled: int
    max_record_bytes: int
    max_leaf_paths: int
    max_root_discovery_depth: int


@dataclass(frozen=True)
class PartialMappingConfig:
    """Configuration for partial mapping run."""

    payload_hash_full: str
    budgets: PartialMappingBudgets
    map_budget_fingerprint: str


@dataclass
class StreamingState:
    """Mutable state during streaming parse."""

    bytes_read: int = 0
    compute_steps: int = 0
    current_depth: int = 0
    max_depth_seen: int = 0
    stop_reason: str = "none"  # none|max_bytes|max_compute|max_depth|parse_error
    elements_recognized: int = 0  # sampled_prefix_len
    skipped_oversize_records: int = 0


# ---------------------------------------------------------------------------
# Deterministic PRNG: Xoshiro256** variant
# ---------------------------------------------------------------------------


class DeterministicPRNG:
    """Simple seeded PRNG for reservoir sampling. Version: prng_xoshiro256ss_v1.

    Uses a SplitMix64-seeded Xoshiro256** generator for deterministic
    uniform float generation.
    """

    def __init__(self, seed_bytes: bytes) -> None:
        """Initialize state from seed bytes via SplitMix64 expansion."""
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
        # Deterministic fallback for the all-zero state, which is invalid for xoshiro.
        if all(s == 0 for s in self._s):
            self._s[0] = 1

    def _rotl(self, x: int, k: int) -> int:
        mask64 = (1 << 64) - 1
        return ((x << k) | (x >> (64 - k))) & mask64

    def next_u64(self) -> int:
        """Generate next uint64 via Xoshiro256**."""
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
        """Return uniform float in [0, 1)."""
        return (self.next_u64() >> 11) * (1.0 / (1 << 53))


# ---------------------------------------------------------------------------
# Backend and fingerprint computation
# ---------------------------------------------------------------------------


def compute_map_backend_id() -> str:
    """Compute map_backend_id: sha256("py="+py_ver+"|ijson="+backend+"|ijson_ver="+ver)[:16]."""
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    backend = ijson.backend
    ijson_ver = ijson.__version__
    identity = f"py={py_ver}|ijson={backend}|ijson_ver={ijson_ver}"
    return sha256_hex(identity.encode("utf-8"))[:16]


def compute_map_budget_fingerprint(
    budgets: PartialMappingBudgets,
    map_backend_id: str,
) -> str:
    """Compute deterministic fingerprint of mapping configuration.

    Delegates to the canonical implementation in util.hashing to ensure
    consistent truncation (32 hex chars) across all code paths.
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
    """Compute deterministic PRNG seed for a root's reservoir sampling."""
    seed_input = f"{payload_hash_full}|{root_path}|{map_budget_fingerprint}"
    return hashlib.sha256(seed_input.encode("utf-8")).digest()


@dataclass
class _ReservoirEntry:
    """A candidate in the reservoir: stores the random key and the record."""

    random_key: float
    index: int
    record: dict[str, Any]
    record_bytes: int
    record_hash: str


_ReservoirHeapEntry = tuple[float, int, _ReservoirEntry]


@dataclass
class _RootState:
    """Mutable state for one discovered root during streaming."""

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
    leaf_paths_seen: int = 0


# ---------------------------------------------------------------------------
# JSON type name helper
# ---------------------------------------------------------------------------


def _json_type_name(value: Any) -> str:
    """Return a JSON-style type name for a Python value."""
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
    """Update field type distribution from a record."""
    for key, val in record.items():
        type_name = _json_type_name(val)
        if key not in field_types:
            field_types[key] = {}
        field_types[key][type_name] = field_types[key].get(type_name, 0) + 1


# ---------------------------------------------------------------------------
# Byte-counted stream wrapper
# ---------------------------------------------------------------------------


class _CountingStream:
    """Wrapper around a binary stream that counts bytes read."""

    def __init__(self, stream: BinaryIO, state: StreamingState) -> None:
        self._stream = stream
        self._state = state

    def read(self, size: int = -1) -> bytes:
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
    """Run partial mapping on a byte stream.

    Uses ijson for streaming parse with budget enforcement and deterministic
    reservoir sampling for record collection.
    """
    budgets = config.budgets
    state = StreamingState()
    counting_stream = _CountingStream(stream, state)

    # Root discovery state
    roots: dict[str, _RootState] = {}
    # Track the current element being built for each root
    current_elements: dict[str, list[Any]] = {}  # root_key -> stack of partial values
    current_element_keys: dict[str, list[str | None]] = {}  # root_key -> stack of keys
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
            if not in_element and active_root is None and depth <= budgets.max_root_discovery_depth:
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
                    if event == "end_array" and _prefix_to_jsonpath(prefix) == root_state.root_path:
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
                            _finalize_element(root_state, record, state, budgets)
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
                            if isinstance(parent, dict) and parent_key is not None:
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
                                _finalize_element(root_state, record_val, state, budgets)
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
                            if isinstance(parent, dict) and parent_key is not None:
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

        root_summary: dict[str, Any] = {
            "elements_seen": root_state.elements_seen,
            "sampled_record_count": len(sample_indices),
            "sampled_prefix_len": state.elements_recognized,
            "prefix_coverage": prefix_coverage,
            "stop_reason": state.stop_reason if prefix_coverage else "none",
            "skipped_oversize": state.skipped_oversize_records,
            "skipped_oversize_records": state.skipped_oversize_records,
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
            stop_reason=state.stop_reason if prefix_coverage else None,
            sampled_prefix_len=state.elements_recognized,
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
    """Convert an ijson prefix string to a canonical JSONPath."""
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
    """Finalize a parsed element: check size, update reservoir, track fields."""
    state.elements_recognized += 1
    root_state.elements_seen += 1
    element_index = root_state.elements_seen - 1

    # Serialize and check size
    try:
        record_canonical = canonical_bytes(record)
    except (TypeError, ValueError):
        # Cannot canonicalize -- use JSON fallback for size check
        record_json = json.dumps(record, separators=(",", ":"), sort_keys=True)
        record_bytes_data = record_json.encode("utf-8")
        record_canonical = record_bytes_data

    record_byte_count = len(record_canonical)

    if record_byte_count > budgets.max_record_bytes:
        state.skipped_oversize_records += 1
        return

    record_hash = sha256_hex(record_canonical)

    # Update field type distribution
    _update_field_types(root_state.field_types, record)

    # Reservoir sampling: keep the N items with smallest random keys
    random_key = root_state.prng.next_float()
    entry = _ReservoirEntry(
        random_key=random_key,
        index=element_index,
        record=record,
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
