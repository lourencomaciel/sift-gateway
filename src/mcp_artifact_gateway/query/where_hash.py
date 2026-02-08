"""Where clause hashing per spec section 12.3.1.

Two modes are supported:

- **raw_string** (default): ``sha256(where_clause.encode('utf-8')).hexdigest()``
- **canonical_ast**: Parse to AST, canonicalize (commutative sort for AND/OR,
  numeric literal normalization, string escape normalization, redundant
  parenthesis removal), then ``sha256(canonical_json(ast))``. This mode is
  deferred and currently raises ``NotImplementedError``.
"""

from __future__ import annotations

from mcp_artifact_gateway.util.hashing import sha256_hex


def where_hash(where_clause: str, mode: str = "raw_string") -> str:
    """Compute a deterministic hash of a where clause.

    Args:
        where_clause: The raw where-clause string.
        mode: Hashing mode. One of ``"raw_string"`` or ``"canonical_ast"``.

    Returns:
        64-character lowercase hex SHA-256 digest.

    Raises:
        ValueError: If *mode* is not a recognized value.
        NotImplementedError: If *mode* is ``"canonical_ast"`` (not yet
            implemented -- requires full AST canonicalization logic which
            is deferred per the spec's phased rollout plan).
    """
    if mode == "raw_string":
        return sha256_hex(where_clause.encode("utf-8"))
    elif mode == "canonical_ast":
        raise NotImplementedError(
            "canonical_ast mode is not yet implemented. "
            "It requires AST canonicalization (commutative sort for AND/OR, "
            "numeric literal normalization, string escape normalization, "
            "redundant parenthesis removal) which is deferred to a future release. "
            "Use mode='raw_string' for now."
        )
    else:
        raise ValueError(
            f"Unknown where_hash mode: {mode!r}. "
            f"Supported modes: 'raw_string', 'canonical_ast'."
        )
