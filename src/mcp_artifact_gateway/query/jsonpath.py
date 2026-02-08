"""Minimal JSONPath subset parser/evaluator for retrieval tools."""

from __future__ import annotations

import re
from dataclasses import dataclass
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
    """Raised when JSONPath expression is outside supported subset."""


@dataclass(frozen=True)
class Segment:
    kind: Literal["field", "index", "wildcard"]
    value: str | int | None


def parse_jsonpath(path: str) -> list[Segment]:
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
            i = match.end()
            continue

        if char != "[":
            msg = f"invalid token at offset {i}"
            raise JsonPathError(msg)

        if path.startswith("[*]", i):
            segments.append(Segment(kind="wildcard", value=None))
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
                        msg = f"unsupported escape sequence '\\{escape_char}' in bracket field"
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
            continue

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

    return segments


def canonicalize_jsonpath(path: str) -> str:
    parts = ["$"]
    for seg in parse_jsonpath(path):
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


def evaluate_jsonpath(document: Any, path: str) -> list[Any]:
    nodes = [document]
    for seg in parse_jsonpath(path):
        next_nodes: list[Any] = []
        if seg.kind == "field":
            field = str(seg.value)
            for node in nodes:
                if isinstance(node, dict) and field in node:
                    next_nodes.append(node[field])
        elif seg.kind == "index":
            idx = int(seg.value)
            for node in nodes:
                if isinstance(node, list) and 0 <= idx < len(node):
                    next_nodes.append(node[idx])
        else:
            for node in nodes:
                if isinstance(node, list):
                    next_nodes.extend(node)
                elif isinstance(node, dict):
                    for key in sorted(node):
                        next_nodes.append(node[key])
        nodes = next_nodes
    return nodes
