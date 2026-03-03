"""Helper utilities shared by the MCP gateway server module."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Awaitable, Callable, Mapping
import importlib.util
import json
from pathlib import Path
import shutil
from typing import Any

from fastmcp.server.dependencies import get_context
from fastmcp.tools.tool import Tool, ToolResult

from sift_gateway.cursor.payload import CursorStaleError
from sift_gateway.cursor.token import CursorTokenError
from sift_gateway.envelope.responses import gateway_error
from sift_gateway.fs.blob_store import BinaryRef, BlobStore
from sift_gateway.tools.usage_hint import PAGINATION_COMPLETENESS_RULE

_SUPPORTED_ENVELOPE_PARTS = {
    "json",
    "text",
    "resource_ref",
    "binary_ref",
    "image_ref",
}
_MEDIA_BLOCK_TYPES = {"image", "video"}
_MEDIA_DATA_KEYS = ("data", "base64", "image_data", "video_data")
_MEDIA_MIME_KEYS = ("mime", "mimeType", "mime_type", "media_type", "mediaType")


def artifact_tool_description(
    *,
    code_query_package_summary: str,
) -> str:
    """Build artifact-tool description with compact package summary."""
    return (
        "Interact with stored artifacts. "
        "Actions: query, next_page, blob_list, blob_materialize, blob_cleanup, blob_manifest. "
        'Use action="query" with query_kind="code" to run Python over '
        "stored artifacts. "
        f"Code-query packages: {code_query_package_summary}. "
        'Use action="next_page" to fetch additional upstream pages for a '
        "paginated artifact. "
        "Use action=\"blob_list\" to discover linked blobs without "
        "returning bytes. "
        "Use action=\"blob_materialize\" to stage one blob as a local "
        "file path for downstream tools. "
        "Use action=\"blob_cleanup\" to remove staged blob files from "
        "allowed local staging roots. "
        "Use action=\"blob_manifest\" to export blob metadata to local "
        "CSV/JSON files. "
        f"{PAGINATION_COMPLETENESS_RULE}"
    )


def not_implemented(tool_name: str) -> dict[str, Any]:
    """Return a NOT_IMPLEMENTED gateway error for a tool."""
    return gateway_error(
        "NOT_IMPLEMENTED",
        f"{tool_name} is not wired to persistence yet",
    )


def cursor_position(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract position_state dict from cursor payload."""
    position = payload.get("position_state")
    if not isinstance(position, dict):
        msg = "cursor missing position_state"
        raise CursorTokenError(msg)
    return position


def assert_cursor_field(
    payload: Mapping[str, Any],
    *,
    field: str,
    expected: object,
) -> None:
    """Raise CursorStaleError if a cursor field does not match."""
    actual = payload.get(field)
    if actual != expected:
        msg = f"cursor {field} mismatch"
        raise CursorStaleError(msg)


def check_sample_corruption(
    root_row: dict[str, Any],
    sample_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return INTERNAL error if expected sample indices are missing rows."""
    expected_raw = root_row.get("sample_indices")
    if not isinstance(expected_raw, list) or not expected_raw:
        return None
    expected = {int(i) for i in expected_raw if isinstance(i, int)}
    actual = {
        int(row["sample_index"])
        for row in sample_rows
        if isinstance(row.get("sample_index"), int)
    }
    missing = sorted(expected - actual)
    if missing:
        return gateway_error(
            "INTERNAL",
            "sample data corruption: expected sample rows missing",
            details={
                "root_key": root_row.get("root_key"),
                "missing_indices": missing,
                "expected_count": len(expected),
                "actual_count": len(actual),
            },
        )
    return None


def mcp_safe_name(qualified_name: str) -> str:
    """Convert a dotted qualified name to an MCP-safe name."""
    return qualified_name.replace(".", "_")


def assert_unique_safe_tool_name(
    seen: dict[str, str],
    *,
    safe_name: str,
    qualified_name: str,
) -> None:
    """Ensure MCP-safe tool names remain collision-free."""
    existing = seen.get(safe_name)
    if existing is not None and existing != qualified_name:
        msg = (
            "tool name collision after MCP-safe sanitization: "
            f"{existing!r} and {qualified_name!r} -> {safe_name!r}"
        )
        raise ValueError(msg)
    seen[safe_name] = qualified_name


def command_resolvable(command: str | None) -> bool:
    """Return whether a stdio command appears resolvable on this host."""
    if not command:
        return False
    if "/" in command:
        candidate = Path(command)
        return candidate.exists() and candidate.is_file()
    return shutil.which(command) is not None


def stdio_module_probe(args: list[str]) -> dict[str, Any] | None:
    """Return module import diagnostics for ``python -m <module>`` launches."""
    if len(args) < 2 or args[0] != "-m":
        return None
    module = args[1]
    probe: dict[str, Any] = {"module": module}
    try:
        spec = importlib.util.find_spec(module)
    except ModuleNotFoundError as exc:
        probe["importable"] = False
        probe["error"] = str(exc)
        return probe
    probe["importable"] = spec is not None
    if spec is None:
        probe["error"] = "module not found"
    return probe


def ensure_gateway_context(arguments: dict[str, Any]) -> dict[str, Any]:
    """Auto-inject ``_gateway_context.session_id`` from MCP transport."""
    ctx = arguments.get("_gateway_context")
    if isinstance(ctx, dict) and ctx.get("session_id"):
        return arguments
    try:
        mcp_ctx = get_context()
        session_id = mcp_ctx.session_id
    except RuntimeError:
        return arguments
    gw_ctx: dict[str, Any] = dict(ctx) if isinstance(ctx, dict) else {}
    gw_ctx.setdefault("session_id", session_id)
    return {**arguments, "_gateway_context": gw_ctx}


class RuntimeTool(Tool):
    """FastMCP tool subclass that accepts raw argument dicts."""

    handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    response_sanitizer: Callable[[dict[str, Any]], dict[str, Any]] | None = None

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        """Execute the handler with raw arguments."""
        arguments = ensure_gateway_context(arguments)
        result = await self.handler(arguments)
        if self.response_sanitizer is not None:
            result = self.response_sanitizer(result)
        return ToolResult(structured_content=result)


def upstream_error_message(result: dict[str, Any]) -> str:
    """Extract a human-readable error message from an upstream result."""
    content = result.get("content")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    return "upstream tool returned an error"


def _blob_uri(blob_id: str) -> str:
    """Return stable internal URI for a stored blob ID."""
    return f"sift://blob/{blob_id}"


def _mime_from_mapping(value: Mapping[str, Any] | None) -> str | None:
    """Extract a MIME string from a mapping, if available."""
    if not isinstance(value, Mapping):
        return None
    for key in _MEDIA_MIME_KEYS:
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _decode_base64_payload(raw: str) -> tuple[bytes, str | None] | None:
    """Decode base64 payloads, including optional ``data:*;base64,`` URLs."""
    payload = raw.strip()
    mime_from_data_url: str | None = None
    if payload.startswith("data:"):
        prefix, sep, encoded = payload.partition(",")
        if not sep:
            return None
        meta = prefix[5:]
        mime_candidate, _, extra = meta.partition(";")
        if "base64" not in extra.lower():
            return None
        if mime_candidate.strip():
            mime_from_data_url = mime_candidate.strip()
        payload = encoded.strip()
    try:
        return base64.b64decode(payload, validate=True), mime_from_data_url
    except (binascii.Error, ValueError):
        return None


def _extract_inline_media_payload(
    block: Mapping[str, Any],
) -> tuple[bytes, str | None, str] | None:
    """Extract inline image/video bytes from an MCP content block."""
    part_type_raw = block.get("type")
    if not isinstance(part_type_raw, str):
        return None
    part_type = part_type_raw.lower().strip()
    if part_type not in _MEDIA_BLOCK_TYPES:
        return None

    source = block.get("source")
    candidates: list[Mapping[str, Any]] = [block]
    if isinstance(source, Mapping):
        candidates.insert(0, source)

    for candidate in candidates:
        for key in _MEDIA_DATA_KEYS:
            raw_data = candidate.get(key)
            if not isinstance(raw_data, str) or not raw_data.strip():
                continue
            decoded = _decode_base64_payload(raw_data)
            if decoded is None:
                continue
            payload, mime_from_data_url = decoded
            mime = (
                mime_from_data_url
                or _mime_from_mapping(candidate)
                or _mime_from_mapping(block)
            )
            return payload, mime, part_type
    return None


def normalize_upstream_content(
    *,
    content: list[dict[str, Any]] | None,
    structured_content: Any,
    blob_store: BlobStore | None = None,
    binary_refs_out: list[BinaryRef] | None = None,
) -> list[Mapping[str, Any]]:
    """Normalize upstream content blocks into envelope parts.

    Inline image/video base64 payloads are converted to binary refs when a
    blob store is available, preventing large media blobs from re-entering
    model context.
    """
    normalized: list[Mapping[str, Any]] = []
    if isinstance(structured_content, (dict, list)):
        normalized.append({"type": "json", "value": structured_content})
    elif structured_content is not None:
        normalized.append(
            {
                "type": "text",
                "text": json.dumps(structured_content, ensure_ascii=False),
            }
        )

    for block in content or []:
        if blob_store is not None:
            media_payload = _extract_inline_media_payload(block)
            if media_payload is not None:
                payload, mime, media_kind = media_payload
                resolved_mime = mime or (
                    "image/png" if media_kind == "image" else "video/mp4"
                )
                blob_ref = blob_store.put_bytes(payload, mime=resolved_mime)
                if binary_refs_out is not None:
                    binary_refs_out.append(blob_ref)
                normalized.append(
                    {
                        "type": (
                            "image_ref"
                            if media_kind == "image"
                            else "binary_ref"
                        ),
                        "blob_id": blob_ref.blob_id,
                        "binary_hash": blob_ref.binary_hash,
                        "mime": blob_ref.mime,
                        "byte_count": blob_ref.byte_count,
                        "uri": _blob_uri(blob_ref.blob_id),
                    }
                )
                continue
        part_type = block.get("type")
        if part_type in _SUPPORTED_ENVELOPE_PARTS:
            normalized.append(block)
            continue
        if isinstance(block.get("text"), str):
            normalized.append({"type": "text", "text": block["text"]})
            continue
        normalized.append(
            {
                "type": "text",
                "text": json.dumps(block, sort_keys=True, ensure_ascii=False),
            }
        )
    return normalized
