"""Resolve artifact ID references in mirrored tool arguments.

Scan top-level string values in forwarded arguments for artifact
IDs matching the ``art_<32hex>`` pattern, optionally followed by
a ``:$.jsonpath`` query suffix.  For each match, fetch the stored
envelope from the database and substitute the JSON or text payload
(or a JSONPath-selected subset) so that the upstream tool receives
the actual data.

Exports ``resolve_artifact_refs``, ``is_artifact_ref``, and
``parse_artifact_ref``.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from sift_mcp.constants import ARTIFACT_ID_PREFIX, WORKSPACE_ID
from sift_mcp.mcp.handlers.common import (
    ENVELOPE_COLUMNS,
    extract_json_target,
    row_to_dict,
)
from sift_mcp.query.jsonpath import JsonPathError, evaluate_jsonpath
from sift_mcp.storage.payload_store import reconstruct_envelope
from sift_mcp.tools.artifact_get import FETCH_ARTIFACT_SQL

_PREFIX_LEN = len(ARTIFACT_ID_PREFIX) + 32

_ARTIFACT_PREFIX_RE = re.compile(
    r"^" + re.escape(ARTIFACT_ID_PREFIX) + r"[0-9a-f]{32}"
)

_ARTIFACT_BARE_RE = re.compile(
    r"^" + re.escape(ARTIFACT_ID_PREFIX) + r"[0-9a-f]{32}$"
)


@dataclass(frozen=True)
class ResolveError:
    """Describes why an artifact reference could not be resolved.

    Attributes:
        code: Machine-readable error code for gateway_error.
        message: Human-readable description of the failure.
    """

    code: str
    message: str


@dataclass(frozen=True)
class ParsedRef:
    """A parsed artifact reference.

    Attributes:
        artifact_id: The ``art_<32hex>`` identifier.
        jsonpath: Optional JSONPath query string (starts with
            ``$``), or ``None`` for a bare reference.
    """

    artifact_id: str
    jsonpath: str | None


def is_artifact_ref(value: Any) -> bool:
    """Check whether a value looks like an artifact reference.

    Matches both bare ``art_<32hex>`` references and query
    references of the form ``art_<32hex>:$.jsonpath``.

    Args:
        value: Any argument value.

    Returns:
        ``True`` when the value is a string matching an
        artifact reference pattern.
    """
    return parse_artifact_ref(value) is not None


def parse_artifact_ref(value: Any) -> ParsedRef | None:
    """Parse an artifact reference string into its components.

    Accepts both bare references (``art_<32hex>``) and query
    references (``art_<32hex>:$.jsonpath``).

    Args:
        value: String to parse.

    Returns:
        A ``ParsedRef`` on success, or ``None`` if the value
        does not match any artifact reference pattern.
    """
    if not isinstance(value, str):
        return None
    if not _ARTIFACT_PREFIX_RE.match(value):
        return None
    # Bare ref: exactly prefix_len chars.
    if _ARTIFACT_BARE_RE.match(value):
        return ParsedRef(artifact_id=value, jsonpath=None)
    # Query ref: art_<32hex>:$...
    if (
        len(value) > _PREFIX_LEN + 1
        and value[_PREFIX_LEN] == ":"
        and value[_PREFIX_LEN + 1] == "$"
    ):
        return ParsedRef(
            artifact_id=value[:_PREFIX_LEN],
            jsonpath=value[_PREFIX_LEN + 1 :],
        )
    # Trailing garbage — not a valid reference.
    return None


def _fetch_and_extract(
    connection: Any,
    artifact_id: str,
    jsonpath: str | None = None,
    *,
    blobs_payload_dir: Any | None = None,
) -> Any | ResolveError:
    """Fetch an artifact envelope and extract its payload value.

    Queries the artifact and payload tables, reconstructs the
    envelope, and returns the JSON value (via
    ``extract_json_target``) or text content.  When *jsonpath*
    is provided, further evaluates the query against the
    extracted value.  Returns a ``ResolveError`` when the
    artifact is missing, deleted, contains only binary
    references, or the JSONPath query yields no matches.

    Args:
        connection: Active database connection.
        artifact_id: The artifact ID to resolve.
        jsonpath: Optional JSONPath query to evaluate against
            the extracted value.
        blobs_payload_dir: Root directory for payload blob files.

    Returns:
        The extracted JSON/text value on success, or a
        ``ResolveError`` on failure.
    """
    row = row_to_dict(
        connection.execute(
            FETCH_ARTIFACT_SQL,
            (WORKSPACE_ID, artifact_id),
        ).fetchone(),
        ENVELOPE_COLUMNS,
    )
    if row is None:
        return ResolveError(
            code="NOT_FOUND",
            message=(
                f"artifact ref {artifact_id} could not be resolved:"
                " artifact not found"
            ),
        )

    if row.get("deleted_at") is not None:
        return ResolveError(
            code="GONE",
            message=(
                f"artifact ref {artifact_id} could not be resolved:"
                " artifact has been deleted"
            ),
        )

    # Reconstruct the envelope from stored JSONB or payload file.
    envelope_value = row.get("envelope")
    payload_fs_path = row.get("payload_fs_path")

    if isinstance(envelope_value, dict) and "content" in envelope_value:
        envelope = envelope_value
    elif not isinstance(payload_fs_path, str) or not payload_fs_path:
        return ResolveError(
            code="INTERNAL",
            message=(
                f"artifact ref {artifact_id} could not be resolved:"
                " missing payload file path"
            ),
        )
    else:
        if blobs_payload_dir is None:
            return ResolveError(
                code="INTERNAL",
                message=(
                    f"artifact ref {artifact_id} could not be resolved:"
                    " payload root unavailable"
                ),
            )
        try:
            envelope = reconstruct_envelope(
                payload_fs_path=payload_fs_path,
                blobs_payload_dir=blobs_payload_dir,
                encoding=str(row.get("envelope_canonical_encoding", "none")),
                expected_hash=str(row.get("payload_hash_full", "")),
            )
        except ValueError:
            return ResolveError(
                code="INTERNAL",
                message=(
                    f"artifact ref {artifact_id} could not be"
                    " resolved: envelope reconstruction failed"
                ),
            )

    # Refuse binary-only envelopes.
    if row.get("contains_binary_refs"):
        content = envelope.get("content", [])
        has_json_or_text = any(
            isinstance(p, dict) and p.get("type") in ("json", "text")
            for p in content
        )
        if not has_json_or_text:
            return ResolveError(
                code="INVALID_ARGUMENT",
                message=(
                    f"artifact ref {artifact_id} contains only"
                    " binary data and cannot be used as a tool"
                    " argument"
                ),
            )

    # Extract the JSON target value (same logic as artifact.get).
    mapped_part_index = row.get("mapped_part_index")
    value = extract_json_target(envelope, mapped_part_index)

    # If extract_json_target returned the full envelope (no JSON
    # part found), try falling back to the first text part.
    if value is envelope:
        content = envelope.get("content", [])
        for part in content:
            if isinstance(part, dict):
                if part.get("type") == "json" and "value" in part:
                    value = part["value"]
                    break
                if part.get("type") == "text" and "text" in part:
                    value = part["text"]
                    break

    # Apply JSONPath query if provided.
    if jsonpath is not None:
        try:
            matches = evaluate_jsonpath(value, jsonpath)
        except JsonPathError as exc:
            return ResolveError(
                code="INVALID_ARGUMENT",
                message=(
                    f"artifact ref {artifact_id}: invalid JSONPath query: {exc}"
                ),
            )
        if not matches:
            return ResolveError(
                code="NOT_FOUND",
                message=(
                    f"artifact ref {artifact_id}: JSONPath"
                    f" '{jsonpath}' matched no values"
                ),
            )
        # Single match: unwrap; multiple: return list.
        return matches[0] if len(matches) == 1 else matches

    return value


def resolve_artifact_refs(
    connection: Any,
    args: dict[str, Any],
    *,
    blobs_payload_dir: Any | None = None,
) -> dict[str, Any] | ResolveError:
    """Resolve top-level artifact ID references in tool arguments.

    Scan each top-level value in *args*.  When a value is a string
    matching the ``art_<32hex>`` pattern, fetch the corresponding
    stored envelope and substitute the JSON or text payload.
    Non-matching values are passed through unchanged.

    Only top-level string values are inspected; values nested
    inside dicts or lists are never resolved.

    Args:
        connection: Active database connection.
        args: Forwarded tool arguments (reserved keys already
            stripped).
        blobs_payload_dir: Root directory for payload blob files.

    Returns:
        A new dict with artifact references replaced by their
        stored payload values, or a ``ResolveError`` on the
        first resolution failure.
    """
    # Fast path: if no values match, return args unchanged.
    refs: dict[str, ParsedRef] = {}
    for key, value in args.items():
        if isinstance(value, str):
            parsed = parse_artifact_ref(value)
            if parsed is not None:
                refs[key] = parsed
    if not refs:
        return args

    resolved = dict(args)
    for key, parsed in refs.items():
        result = _fetch_and_extract(
            connection,
            parsed.artifact_id,
            parsed.jsonpath,
            blobs_payload_dir=blobs_payload_dir,
        )
        if isinstance(result, ResolveError):
            return result
        resolved[key] = result

    return resolved
