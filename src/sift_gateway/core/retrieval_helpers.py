"""Shared helpers for artifact retrieval and envelope targeting."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from sift_gateway.envelope.content_extract import (
    parse_text_as_json,
    queryable_json_from_part,
)


class RetrievalTouchRuntime(Protocol):
    """Runtime protocol that supports retrieval touch updates."""

    def safe_touch_for_retrieval(
        self,
        connection: Any,
        *,
        session_id: str,
        artifact_id: str,
    ) -> bool:
        """Touch retrieval metadata for one artifact id."""


def _parse_text_as_json(text: str) -> Any | None:
    r"""Parse a text string as JSON, returning None on failure.

    Handles double-encoded JSON strings (e.g. ``"\\"{ ... }\\""``)
    consistently with the mapping runner's ``_score_text_part``.
    """
    return parse_text_as_json(text)


def extract_json_target(
    envelope: dict[str, Any],
    mapped_part_index: int | None,
) -> Any:
    """Extract JSON content target that mapping root_paths are relative to.

    Supports both explicit ``json`` parts (with a ``value`` key) and
    ``text`` parts whose text content is valid JSON.  The latter case
    occurs when upstream MCP servers return JSON data as ``TextContent``
    blocks (the default for FastMCP over stdio transport).
    """
    from sift_gateway.mapping.json_strings import resolve_json_strings

    if not isinstance(mapped_part_index, int):
        return envelope
    content = envelope.get("content", [])
    if 0 <= mapped_part_index < len(content):
        part = content[mapped_part_index]
        if not isinstance(part, dict):
            return envelope
        parsed, _part_type, _encoding = queryable_json_from_part(part)
        if parsed is not None:
            return resolve_json_strings(parsed)
    return envelope


def touch_retrieval_artifacts(
    runtime: RetrievalTouchRuntime,
    connection: Any,
    *,
    session_id: str,
    artifact_ids: Sequence[str],
) -> None:
    """Touch retrieval timestamp for artifact ids and commit when needed."""
    touched = False
    for artifact_id in artifact_ids:
        touched = (
            runtime.safe_touch_for_retrieval(
                connection,
                session_id=session_id,
                artifact_id=artifact_id,
            )
            or touched
        )
    if touched:
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()
