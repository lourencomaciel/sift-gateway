"""Sample set hash computation for partial mode cursor binding per Section 14.3.

The sample_set_hash binds a cursor to the exact set of sampled records so that
the server can detect when the sample set has changed (e.g. due to new data
arriving or budget configuration changes).
"""

from __future__ import annotations

import hashlib
import json


def compute_sample_set_hash(
    root_path: str,
    sample_indices: list[int],
    map_budget_fingerprint: str,
    mapper_version: str,
) -> str:
    """Compute the sample set hash for partial mode cursor binding.

    Builds a canonical JSON object from the inputs and returns the first 32
    hex characters of its SHA-256 digest.

    The canonical form uses ``json.dumps`` with ``sort_keys=True`` and compact
    separators.  This is safe because all values in the object are simple types
    (strings and a list of ints) -- no floats or Decimals that would require
    RFC 8785 special handling.

    Parameters:
        root_path: The JSONPath root used for the partial traversal.
        sample_indices: Sorted list of sampled record indices.
        map_budget_fingerprint: Fingerprint of the mapping budget config.
        mapper_version: The mapper version string.

    Returns:
        A 32-character lowercase hex string (first 128 bits of SHA-256).
    """
    if any((not isinstance(i, int)) or i < 0 for i in sample_indices):
        raise ValueError("sample_indices must be a list of non-negative integers")
    if sample_indices != sorted(sample_indices):
        raise ValueError("sample_indices must be sorted ascending for stable hashing")
    if len(sample_indices) != len(set(sample_indices)):
        raise ValueError("sample_indices must not contain duplicates")

    obj = {
        "map_budget_fingerprint": map_budget_fingerprint,
        "mapper_version": mapper_version,
        "root_path": root_path,
        "sample_indices": sample_indices,
    }
    # Keys are already in sorted order; sort_keys=True is a safety net.
    # No floats or Decimals, so standard json.dumps is equivalent to RFC 8785
    # for this specific structure.
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:32]
