"""Partial mapping sample-set cursor binding helpers."""

from __future__ import annotations

from typing import Sequence

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.constants import MAPPER_VERSION
from mcp_artifact_gateway.util.hashing import sha256_trunc


class SampleSetHashBindingError(ValueError):
    """Raised when cursor sample-set binding does not match stored samples."""


def compute_sample_set_hash(
    *,
    root_path: str,
    sample_indices: Sequence[int],
    map_budget_fingerprint: str,
    mapper_version: str = MAPPER_VERSION,
) -> str:
    payload = {
        "root_path": root_path,
        "sample_indices": list(sample_indices),
        "map_budget_fingerprint": map_budget_fingerprint,
        "mapper_version": mapper_version,
    }
    return sha256_trunc(canonical_bytes(payload), 32)


def assert_sample_set_hash_binding(cursor_payload: dict[str, object], expected_hash: str) -> None:
    actual = cursor_payload.get("sample_set_hash")
    if actual != expected_hash:
        msg = "cursor sample_set_hash mismatch"
        raise SampleSetHashBindingError(msg)

