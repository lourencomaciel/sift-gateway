"""Stable where-clause hashing."""

from __future__ import annotations

from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.query.where_dsl import canonicalize_where_ast
from mcp_artifact_gateway.util.hashing import sha256_hex


def where_hash(where: Any, *, mode: str = "raw_string") -> str:
    if mode == "raw_string":
        if isinstance(where, str):
            return sha256_hex(where.encode("utf-8"))
        return sha256_hex(canonical_bytes(where))

    if mode == "canonical_ast":
        if isinstance(where, dict):
            canonical_ast = canonicalize_where_ast(where)
            return sha256_hex(canonical_bytes(canonical_ast))
        if isinstance(where, str):
            return sha256_hex(where.encode("utf-8"))
        msg = "canonical_ast mode requires dict or string where"
        raise ValueError(msg)

    msg = f"unsupported where canonicalization mode: {mode}"
    raise ValueError(msg)

