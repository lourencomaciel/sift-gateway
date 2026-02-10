"""Compute stable hashes of where-clause expressions.

Support two canonicalization modes: ``raw_string`` (hash the
literal expression bytes) and ``canonical_ast`` (parse and
canonicalize the AST before hashing) to enable deterministic
cursor binding over where filters.  Key export is
``where_hash``.
"""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.where_dsl import (
    canonicalize_where_ast,
    parse_where_expression,
)
from mcp_artifact_gateway.util.hashing import sha256_hex


def where_hash(where: Any, *, mode: str = "raw_string") -> str:
    """Compute a stable SHA-256 hash of a where expression.

    Two modes are supported: ``raw_string`` hashes the
    literal bytes of the expression (or canonical JSON for
    dicts), while ``canonical_ast`` parses and canonicalizes
    the AST before hashing for semantic equivalence.

    Args:
        where: Where expression as a string or dict.
        mode: Canonicalization mode (``"raw_string"`` or
            ``"canonical_ast"``).

    Returns:
        Hex-encoded SHA-256 digest.

    Raises:
        ValueError: If the mode is unsupported or the
            input type is incompatible with the mode.
    """
    if mode == "raw_string":
        if isinstance(where, str):
            return sha256_hex(where.encode("utf-8"))
        return sha256_hex(canonical_bytes(where))

    if mode == "canonical_ast":
        if isinstance(where, dict):
            canonical_ast = canonicalize_where_ast(where)
            return sha256_hex(canonical_bytes(canonical_ast))
        if isinstance(where, str):
            parsed = parse_where_expression(where)
            canonical_ast = canonicalize_where_ast(parsed)
            return sha256_hex(canonical_bytes(canonical_ast))
        msg = "canonical_ast mode requires dict or string where"
        raise ValueError(msg)

    msg = f"unsupported where canonicalization mode: {mode}"
    raise ValueError(msg)
