"""Compute request identity for artifact deduplication and caching.

Combines upstream instance identity, prefix, tool name, and
RFC 8785 canonical arguments into a deterministic request key.
Also provides a dedupe hash that optionally excludes specified
JSON paths for reuse lookups.  Exports ``RequestIdentity`` as
a frozen dataclass and the ``compute_request_identity`` and
``compute_dedupe_hash`` helper functions.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sift_mcp.canon.decimal_json import loads_decimal
from sift_mcp.canon.rfc8785 import canonical_bytes
from sift_mcp.util.hashing import request_key as compute_request_key
from sift_mcp.util.hashing import sha256_hex


def _coerce_floats_to_decimal(value: Any) -> Any:
    """Recursively convert float values to Decimal.

    Walk dicts and lists depth-first, replacing any ``float``
    value with its ``Decimal`` equivalent so that the RFC 8785
    canonical serializer accepts it.

    Args:
        value: JSON-compatible Python value.

    Returns:
        The same structure with floats replaced by Decimals.
    """
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _coerce_floats_to_decimal(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_coerce_floats_to_decimal(item) for item in value]
    return value


@dataclass(frozen=True)
class RequestIdentity:
    """Computed identity for a tool call request.

    Encapsulates the sha256-based request key together with
    the constituent parts used to derive it, enabling both
    cache lookups and diagnostic logging.

    Attributes:
        request_key: SHA-256 hex digest of the full identity.
        request_args_hash: SHA-256 hex digest of canonical args.
        request_args_prefix: First N chars of canonical args
            (capped at REQUEST_ARGS_PREFIX_CAP) for debugging.
        upstream_instance_id: Identity of the upstream server.
        prefix: Tool namespace prefix.
        tool_name: Name of the upstream tool invoked.
        canonical_args: Raw RFC 8785 canonical arg bytes.
        REQUEST_ARGS_PREFIX_CAP: Maximum length of the
            request_args_prefix field (class constant).
    """

    request_key: str  # sha256 hex of full identity
    request_args_hash: str  # sha256 hex of canonical args
    request_args_prefix: (
        str  # first N chars of canonical args for debugging (capped)
    )
    upstream_instance_id: str
    prefix: str
    tool_name: str
    canonical_args: bytes  # the canonical arg bytes

    REQUEST_ARGS_PREFIX_CAP = 200


def compute_request_identity(
    *,
    upstream_instance_id: str,
    prefix: str,
    tool_name: str,
    forwarded_args: dict[str, Any],
) -> RequestIdentity:
    """Compute request_key from upstream identity and tool args.

    Derives a deterministic SHA-256 request key from the
    upstream instance, prefix, tool name, and RFC 8785
    canonical arguments.

    Args:
        upstream_instance_id: Identity of the upstream server.
        prefix: Tool namespace prefix.
        tool_name: Name of the upstream tool.
        forwarded_args: Arguments dict to canonicalize.

    Returns:
        A RequestIdentity with the computed request key and
        constituent parts.
    """
    canonical_args = canonical_bytes(_coerce_floats_to_decimal(forwarded_args))
    args_hash = sha256_hex(canonical_args)

    # Compute request key per spec
    req_key = compute_request_key(
        upstream_instance_id,
        prefix,
        tool_name,
        canonical_args,
    )

    # Capped prefix for debugging
    args_text = canonical_args.decode("utf-8", errors="replace")
    args_prefix = args_text[: RequestIdentity.REQUEST_ARGS_PREFIX_CAP]

    return RequestIdentity(
        request_key=req_key,
        request_args_hash=args_hash,
        request_args_prefix=args_prefix,
        upstream_instance_id=upstream_instance_id,
        prefix=prefix,
        tool_name=tool_name,
        canonical_args=canonical_args,
    )


def compute_dedupe_hash(
    canonical_args: bytes,
    *,
    exclusion_paths: list[str] | None = None,
) -> str:
    """Compute dedupe hash with optional JSONPath exclusions.

    Used for reuse lookup only; does NOT define storage
    identity.

    Args:
        canonical_args: RFC 8785 canonical arg bytes.
        exclusion_paths: Optional list of top-level key paths
            (e.g. ``$.key``) to exclude before hashing.

    Returns:
        SHA-256 hex digest of the (possibly filtered)
        canonical args.
    """
    if not exclusion_paths:
        return sha256_hex(canonical_args)

    # Parse the canonical args, remove excluded paths, re-canonicalize
    # Use loads_decimal to preserve Decimal values (no Python float drift)
    args_obj = loads_decimal(canonical_args)
    for path in exclusion_paths:
        # Simple top-level key exclusion for now
        # Path format: $.key or just key
        key = path.lstrip("$").lstrip(".")
        args_obj.pop(key, None)

    return sha256_hex(canonical_bytes(args_obj))
