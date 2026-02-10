"""Request identity computation for artifact deduplication and caching."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mcp_artifact_gateway.canon.decimal_json import loads_decimal
from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.util.hashing import request_key as compute_request_key
from mcp_artifact_gateway.util.hashing import sha256_hex


@dataclass(frozen=True)
class RequestIdentity:
    """Computed identity for a tool call request."""

    request_key: str  # sha256 hex of full identity
    request_args_hash: str  # sha256 hex of canonical args
    request_args_prefix: str  # first N chars of canonical args for debugging (capped)
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
    """Compute request_key from upstream identity + tool + canonical args.

    request_key = sha256(upstream_instance_id|prefix|tool|canonical_args_bytes)
    """
    canonical_args = canonical_bytes(forwarded_args)
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
    """Compute dedupe hash, optionally with JSONPath exclusions applied.

    This hash is used for reuse lookup only. It does NOT define storage identity.
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
