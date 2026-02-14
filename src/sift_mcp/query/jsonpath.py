"""Parse and evaluate a minimal JSONPath subset for retrieval.

Support dotted fields, bracket-quoted fields, integer array
indices, and the ``[*]`` wildcard operator.  Provide parsing
into ``Segment`` lists, canonical round-trip rendering, and
evaluation against a JSON document.  Key exports are
``parse_jsonpath``, ``evaluate_jsonpath``,
``canonicalize_jsonpath``, ``Segment``, and ``JsonPathError``.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_BRACKET_ESCAPES = {
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "\\": "\\",
    "'": "'",
}


class JsonPathError(ValueError):
    """Raised when a JSONPath expression is outside the supported subset.

    Covers syntax errors, unsupported operators, and limit
    violations (max length, max segments, wildcard expansion).
    """


@dataclass(frozen=True)
class Segment:
    """Single parsed segment of a JSONPath expression.

    Attributes:
        kind: "field" for named keys, "index" for integer
            array positions, "wildcard" for ``[*]``.
        value: Key string, integer index, or None for wildcard.
    """

    kind: Literal["field", "index", "wildcard"]
    value: str | int | None


def parse_jsonpath(
    path: str,
    *,
    max_length: int | None = None,
    max_segments: int | None = None,
) -> list[Segment]:
    """Parse a JSONPath string into a list of segments.

    Support dotted fields, bracket-quoted fields, integer
    array indices, and the ``[*]`` wildcard operator.

    Args:
        path: JSONPath expression starting with ``$``.
        max_length: Optional cap on path string length.
        max_segments: Optional cap on parsed segment count.

    Returns:
        Ordered list of parsed Segment objects.

    Raises:
        JsonPathError: If the path is syntactically invalid
            or exceeds the configured limits.
    """
    if max_length is not None and len(path) > max_length:
        msg = f"JSONPath exceeds max length ({max_length})"
        raise JsonPathError(msg)
    if not path or path[0] != "$":
        msg = "JSONPath must start with '$'"
        raise JsonPathError(msg)

    segments: list[Segment] = []
    i = 1
    while i < len(path):
        char = path[i]
        if char == ".":
            i += 1
            match = _IDENT_RE.match(path, i)
            if not match:
                msg = f"invalid dotted field at offset {i}"
                raise JsonPathError(msg)
            name = match.group(0)
            segments.append(Segment(kind="field", value=name))
            if max_segments is not None and len(segments) > max_segments:
                msg = f"JSONPath exceeds max segments ({max_segments})"
                raise JsonPathError(msg)
            i = match.end()
            continue

        if char != "[":
            if char == "?" or path[i:].startswith("?("):
                msg = (
                    "JSONPath filter predicates (?(@...)) "
                    "are not supported. Use the 'where' "
                    "parameter for filtering instead."
                )
                raise JsonPathError(msg)
            msg = f"invalid token at offset {i}"
            raise JsonPathError(msg)

        # Detect filter predicates inside brackets: [?(@...)]
        if path.startswith("[?(", i):
            msg = (
                "JSONPath filter predicates (?(@...)) "
                "are not supported. Use the 'where' "
                "parameter for filtering instead."
            )
            raise JsonPathError(msg)

        if path.startswith("[*]", i):
            segments.append(Segment(kind="wildcard", value=None))
            if max_segments is not None and len(segments) > max_segments:
                msg = f"JSONPath exceeds max segments ({max_segments})"
                raise JsonPathError(msg)
            i += 3
            continue

        if i + 2 < len(path) and path[i + 1] == "'":
            i += 2
            out = []
            while i < len(path):
                if path[i] == "\\":
                    i += 1
                    if i >= len(path):
                        msg = "unterminated escape in bracket field"
                        raise JsonPathError(msg)
                    escape_char = path[i]
                    unescaped = _BRACKET_ESCAPES.get(escape_char)
                    if unescaped is None:
                        msg = (
                            f"unsupported escape sequence"
                            f" '\\{escape_char}'"
                            " in bracket field"
                        )
                        raise JsonPathError(msg)
                    out.append(unescaped)
                    i += 1
                    continue
                if path[i] == "'" and i + 1 < len(path) and path[i + 1] == "]":
                    i += 2
                    break
                out.append(path[i])
                i += 1
            else:
                msg = "unterminated bracket field"
                raise JsonPathError(msg)
            segments.append(Segment(kind="field", value="".join(out)))
            if max_segments is not None and len(segments) > max_segments:
                msg = f"JSONPath exceeds max segments ({max_segments})"
                raise JsonPathError(msg)
            continue

        # Detect union syntax: [a,b] or ['a','b']
        bracket_end = path.find("]", i)
        if bracket_end != -1 and "," in path[i:bracket_end]:
            msg = (
                "JSONPath union syntax is not "
                "supported. Use 'select_paths' "
                "for multi-field projection."
            )
            raise JsonPathError(msg)

        i += 1
        start = i
        while i < len(path) and path[i].isdigit():
            i += 1
        if start == i or i >= len(path) or path[i] != "]":
            msg = f"invalid array index at offset {start}"
            raise JsonPathError(msg)
        index = int(path[start:i])
        i += 1
        segments.append(Segment(kind="index", value=index))
        if max_segments is not None and len(segments) > max_segments:
            msg = f"JSONPath exceeds max segments ({max_segments})"
            raise JsonPathError(msg)

    return segments


def canonicalize_jsonpath(
    path: str,
    *,
    max_length: int | None = None,
    max_segments: int | None = None,
) -> str:
    """Render a JSONPath in canonical bracket/dot notation.

    Parse the path and re-emit each segment in a
    deterministic form: dotted for simple identifiers,
    bracket-quoted for special-character keys.

    Args:
        path: JSONPath expression starting with ``$``.
        max_length: Optional cap on path string length.
        max_segments: Optional cap on parsed segment count.

    Returns:
        Canonical string representation of the path.

    Raises:
        JsonPathError: If the path is syntactically invalid
            or exceeds the configured limits.
    """
    parts = ["$"]
    for seg in parse_jsonpath(
        path, max_length=max_length, max_segments=max_segments
    ):
        if seg.kind == "field":
            name = str(seg.value)
            if _IDENT_RE.fullmatch(name):
                parts.append(f".{name}")
            else:
                escaped = (
                    name.replace("\\", "\\\\")
                    .replace("'", "\\'")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t")
                )
                parts.append(f"['{escaped}']")
        elif seg.kind == "index":
            parts.append(f"[{seg.value}]")
        else:
            parts.append("[*]")
    return "".join(parts)


def evaluate_jsonpath(
    document: Any,
    path: str,
    *,
    max_length: int | None = None,
    max_segments: int | None = None,
    max_wildcard_expansion_total: int | None = None,
) -> list[Any]:
    """Evaluate a JSONPath against a document and return matches.

    Walk the document tree segment by segment, expanding
    wildcards into sorted-key object entries or list items.

    Args:
        document: JSON-compatible Python value to query.
        path: JSONPath expression starting with ``$``.
        max_length: Optional cap on path string length.
        max_segments: Optional cap on parsed segment count.
        max_wildcard_expansion_total: Optional cumulative cap
            on the number of nodes produced by wildcard
            expansion across all segments.

    Returns:
        List of matched values (may be empty).

    Raises:
        JsonPathError: If the path is invalid or wildcard
            expansion exceeds the configured limit.
    """
    nodes = [document]
    wildcard_expansion_total = 0
    for seg in parse_jsonpath(
        path, max_length=max_length, max_segments=max_segments
    ):
        next_nodes: list[Any] = []
        if seg.kind == "field":
            field = str(seg.value)
            for node in nodes:
                if isinstance(node, dict) and field in node:
                    next_nodes.append(node[field])
        elif seg.kind == "index":
            if not isinstance(seg.value, int):
                msg = "invalid index segment in parsed JSONPath"
                raise JsonPathError(msg)
            idx = seg.value
            for node in nodes:
                if isinstance(node, list) and 0 <= idx < len(node):
                    next_nodes.append(node[idx])
        else:
            for node in nodes:
                if isinstance(node, list):
                    wildcard_expansion_total += len(node)
                    next_nodes.extend(node)
                elif isinstance(node, dict):
                    wildcard_expansion_total += len(node)
                    for key in sorted(node):
                        next_nodes.append(node[key])
            if (
                max_wildcard_expansion_total is not None
                and wildcard_expansion_total > max_wildcard_expansion_total
            ):
                msg = (
                    "JSONPath wildcard expansion exceeds max total "
                    f"({max_wildcard_expansion_total})"
                )
                raise JsonPathError(msg)
        nodes = next_nodes
    return nodes
