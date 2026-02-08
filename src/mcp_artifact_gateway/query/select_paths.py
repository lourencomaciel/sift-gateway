"""select_paths canonicalization per spec section 12.3.1.

Provides deterministic normalization, deduplication, and hashing of
select-path lists so that cursor binding is stable across equivalent
but differently-formatted path sets.
"""

from __future__ import annotations

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.jsonpath import is_absolute, normalize_jsonpath
from mcp_artifact_gateway.util.hashing import sha256_hex


def canonicalize_select_paths(paths: list[str]) -> list[str]:
    """Normalize, validate, deduplicate, and sort a list of select paths.

    Each path is:
    1. Stripped of leading/trailing whitespace.
    2. Validated as a relative path (must NOT start with ``$``).
    3. Prepended with ``$`` so it can be parsed as a full JSONPath, then
       normalized via :func:`normalize_jsonpath`, then the leading ``$``
       is stripped back off to return the relative canonical form.
    4. Duplicates removed (preserving first occurrence is irrelevant since
       we sort).
    5. Sorted lexicographically by Unicode code point.

    Args:
        paths: List of relative JSONPath strings.

    Returns:
        Sorted, deduplicated list of canonical relative paths.

    Raises:
        ValueError: If any path is absolute (starts with ``$``) or invalid.
    """
    canonical: list[str] = []
    for raw_path in paths:
        stripped = raw_path.strip()
        if not stripped:
            raise ValueError("select_paths must not contain empty strings")
        if is_absolute(stripped):
            raise ValueError(
                f"select_paths must be relative (must not start with '$'): "
                f"{stripped!r}"
            )
        # Prepend $ to make it parseable as a full JSONPath, normalize,
        # then strip the leading $.
        full = "$" + stripped
        normalized_full = normalize_jsonpath(full)
        # Strip the leading '$' to get the canonical relative form.
        canonical_relative = normalized_full[1:]
        canonical.append(canonical_relative)

    # Deduplicate and sort
    seen: set[str] = set()
    deduped: list[str] = []
    for p in canonical:
        if p not in seen:
            seen.add(p)
            deduped.append(p)

    deduped.sort()
    return deduped


def select_paths_hash(canonical_paths: list[str]) -> str:
    """Compute the SHA-256 hash of a canonical select-paths list.

    The hash is ``sha256(canonical_json(canonical_paths_array))`` expressed
    as a lowercase hex digest. The input MUST already be canonicalized via
    :func:`canonicalize_select_paths`.

    Args:
        canonical_paths: Canonicalized list of relative paths.

    Returns:
        64-character lowercase hex SHA-256 digest.
    """
    return sha256_hex(canonical_bytes(canonical_paths))
