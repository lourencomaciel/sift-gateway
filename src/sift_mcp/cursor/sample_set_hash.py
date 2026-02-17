"""Compute and verify sample-set hashes for cursor binding.

Provide a deterministic hash over the root path, sampled
indices, budget fingerprint, and mapper version so that
cursors can detect when the underlying sample set has
changed.  Key exports are ``compute_sample_set_hash`` and
``assert_sample_set_hash_binding``.
"""

from __future__ import annotations

from collections.abc import Sequence

from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.constants import MAPPER_VERSION
from sift_mcp.util.hashing import sha256_trunc


class SampleSetHashBindingError(ValueError):
    """Raised when cursor sample-set hash does not match stored samples.

    Indicates the sampled records or mapping configuration have
    changed since the cursor was issued.
    """


def compute_sample_set_hash(
    *,
    root_path: str,
    sample_indices: Sequence[int],
    map_budget_fingerprint: str,
    mapper_version: str = MAPPER_VERSION,
) -> str:
    """Compute a deterministic hash over the sample set configuration.

    Combine root path, sampled indices, budget fingerprint, and
    mapper version into a truncated SHA-256 hex digest that
    cursors embed for staleness detection.

    Args:
        root_path: Canonical JSONPath of the sampled root.
        sample_indices: Ordered indices of sampled elements.
        map_budget_fingerprint: Budget configuration hash.
        mapper_version: Mapper version string for binding.

    Returns:
        A 32-char truncated SHA-256 hex digest.
    """
    payload = {
        "root_path": root_path,
        "sample_indices": list(sample_indices),
        "map_budget_fingerprint": map_budget_fingerprint,
        "mapper_version": mapper_version,
    }
    return sha256_trunc(canonical_bytes(payload), 32)


def assert_sample_set_hash_binding(
    cursor_payload: dict[str, object], expected_hash: str
) -> None:
    """Verify that a cursor's sample_set_hash matches expected.

    Args:
        cursor_payload: Decoded cursor payload dict.
        expected_hash: The recomputed sample set hash.

    Raises:
        SampleSetHashBindingError: If the hash does not match.
    """
    actual = cursor_payload.get("sample_set_hash")
    if actual != expected_hash:
        msg = "cursor sample_set_hash mismatch"
        raise SampleSetHashBindingError(msg)
