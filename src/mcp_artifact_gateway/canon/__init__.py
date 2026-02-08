"""Canonicalization, decimal-safe JSON, and compression utilities.

This package provides the deterministic serialization foundation for the
MCP Artifact Gateway. All content-addressable hashing flows through the
RFC 8785 canonical JSON implementation.
"""

from mcp_artifact_gateway.canon.compress import compress, decompress
from mcp_artifact_gateway.canon.decimal_json import load_decimal, loads_decimal
from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes, canonical_json_str

__all__ = [
    "canonical_bytes",
    "canonical_json_str",
    "compress",
    "decompress",
    "load_decimal",
    "loads_decimal",
]
