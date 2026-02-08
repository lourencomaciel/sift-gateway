"""JSONPath subset parser and evaluator per spec section 12.3.

Supported grammar (strict subset -- no filters, recursive descent, or slices):
    $ . name | ['...'] | [n] | [*]

Segment types:
    RootSegment       -- represents ``$``
    PropertySegment   -- ``.name`` or ``['name']``
    IndexSegment      -- ``[n]`` (non-negative integer)
    WildcardSegment   -- ``[*]``

Traversal contract (section 12.4):
    - Arrays: ascending index order
    - Objects: lexicographic key order (Unicode code point)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Union


# ---------------------------------------------------------------------------
# Segment types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RootSegment:
    """Represents the ``$`` root accessor."""
    pass


@dataclass(frozen=True, slots=True)
class PropertySegment:
    """Represents ``.name`` or ``['name']`` property access."""
    name: str


@dataclass(frozen=True, slots=True)
class IndexSegment:
    """Represents ``[n]`` integer index access (non-negative)."""
    index: int


@dataclass(frozen=True, slots=True)
class WildcardSegment:
    """Represents ``[*]`` wildcard access."""
    pass


Segment = Union[RootSegment, PropertySegment, IndexSegment, WildcardSegment]

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class BudgetExceededError(Exception):
    """Raised when wildcard expansion exceeds the configured maximum."""
    pass


# ---------------------------------------------------------------------------
# Bracket string escape handling
# ---------------------------------------------------------------------------

_BRACKET_ESCAPES: dict[str, str] = {
    "\\\\": "\\",
    "\\'": "'",
    "\\n": "\n",
    "\\r": "\r",
    "\\t": "\t",
}


def _unescape_bracket_string(raw: str) -> str:
    """Unescape a bracket-notation string literal (without surrounding quotes).

    Supported escapes: ``\\\\``, ``\\'``, ``\\n``, ``\\r``, ``\\t``.
    Any other backslash sequence is an error.
    """
    result: list[str] = []
    i = 0
    while i < len(raw):
        if raw[i] == "\\":
            if i + 1 >= len(raw):
                raise ValueError("Trailing backslash in bracket string")
            two = raw[i : i + 2]
            if two in _BRACKET_ESCAPES:
                result.append(_BRACKET_ESCAPES[two])
                i += 2
            else:
                raise ValueError(
                    f"Invalid escape sequence in bracket string: {two!r}"
                )
        else:
            result.append(raw[i])
            i += 1
    return "".join(result)


def _escape_for_bracket(name: str) -> str:
    """Escape a property name for bracket notation ``['...']``.

    Produces canonical escaping: only ``\\``, ``'``, and control chars
    (newline, carriage return, tab) are escaped.
    """
    result: list[str] = []
    for ch in name:
        if ch == "\\":
            result.append("\\\\")
        elif ch == "'":
            result.append("\\'")
        elif ch == "\n":
            result.append("\\n")
        elif ch == "\r":
            result.append("\\r")
        elif ch == "\t":
            result.append("\\t")
        else:
            result.append(ch)
    return "".join(result)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def parse_jsonpath(
    path: str,
    max_length: int = 4096,
    max_segments: int = 64,
) -> list[Segment]:
    """Parse a JSONPath string into a list of segments.

    Args:
        path: JSONPath string to parse.
        max_length: Maximum allowed length of the path string.
        max_segments: Maximum number of segments (including root).

    Returns:
        List of Segment objects.

    Raises:
        ValueError: If the path is syntactically invalid or exceeds caps.
    """
    if not isinstance(path, str):
        raise ValueError(f"JSONPath must be a string, got {type(path).__name__}")
    if len(path) > max_length:
        raise ValueError(
            f"JSONPath exceeds maximum length: {len(path)} > {max_length}"
        )
    if not path:
        raise ValueError("JSONPath must not be empty")

    pos = 0
    segments: list[Segment] = []

    # Must start with '$'
    if path[0] != "$":
        raise ValueError(
            f"JSONPath must start with '$', got {path[0]!r} at position 0"
        )
    segments.append(RootSegment())
    pos = 1

    while pos < len(path):
        if len(segments) >= max_segments:
            raise ValueError(
                f"JSONPath exceeds maximum segment count: {max_segments}"
            )

        ch = path[pos]

        if ch == ".":
            # Dot notation property access: .name
            pos += 1
            if pos >= len(path):
                raise ValueError(
                    "JSONPath has trailing dot with no property name"
                )
            # Read identifier
            m = _IDENTIFIER_RE.match(path, pos)
            if m is None:
                raise ValueError(
                    f"Invalid property name at position {pos}: "
                    f"expected identifier matching [A-Za-z_][A-Za-z0-9_]*"
                )
            segments.append(PropertySegment(m.group()))
            pos = m.end()

        elif ch == "[":
            pos += 1
            if pos >= len(path):
                raise ValueError("JSONPath has unclosed bracket at end of path")

            if path[pos] == "*":
                # Wildcard: [*]
                pos += 1
                if pos >= len(path) or path[pos] != "]":
                    raise ValueError(
                        f"Expected ']' after '[*' at position {pos}"
                    )
                pos += 1
                segments.append(WildcardSegment())

            elif path[pos] == "'":
                # Bracket string: ['...']
                pos += 1  # skip opening quote
                str_start = pos
                # Scan for closing quote, handling escapes
                while pos < len(path):
                    if path[pos] == "\\":
                        pos += 2  # skip escape sequence
                    elif path[pos] == "'":
                        break
                    else:
                        pos += 1
                if pos >= len(path):
                    raise ValueError(
                        "JSONPath has unclosed string literal in bracket notation"
                    )
                raw = path[str_start:pos]
                pos += 1  # skip closing quote
                if pos >= len(path) or path[pos] != "]":
                    raise ValueError(
                        f"Expected ']' after bracket string at position {pos}"
                    )
                pos += 1
                name = _unescape_bracket_string(raw)
                segments.append(PropertySegment(name))

            elif path[pos].isdigit():
                # Integer index: [n]
                idx_start = pos
                while pos < len(path) and path[pos].isdigit():
                    pos += 1
                if pos >= len(path) or path[pos] != "]":
                    raise ValueError(
                        f"Expected ']' after integer index at position {pos}"
                    )
                idx_str = path[idx_start:pos]
                # Reject leading zeros (except bare "0")
                if len(idx_str) > 1 and idx_str[0] == "0":
                    raise ValueError(
                        f"Integer index must not have leading zeros: {idx_str!r}"
                    )
                idx = int(idx_str)
                pos += 1
                segments.append(IndexSegment(idx))

            else:
                raise ValueError(
                    f"Unexpected character after '[' at position {pos}: "
                    f"{path[pos]!r}. Expected '*', single-quoted string, or integer."
                )

        else:
            raise ValueError(
                f"Unexpected character at position {pos}: {ch!r}. "
                f"Expected '.' or '['."
            )

    if len(segments) > max_segments:
        raise ValueError(
            f"JSONPath exceeds maximum segment count: {len(segments)} > {max_segments}"
        )

    return segments


# ---------------------------------------------------------------------------
# Normalization / canonical form
# ---------------------------------------------------------------------------

def _segment_to_canonical(seg: Segment) -> str:
    """Serialize a single segment to its canonical string form."""
    if isinstance(seg, RootSegment):
        return "$"
    elif isinstance(seg, PropertySegment):
        if _IDENTIFIER_RE.fullmatch(seg.name):
            return f".{seg.name}"
        else:
            return f"['{_escape_for_bracket(seg.name)}']"
    elif isinstance(seg, IndexSegment):
        return f"[{seg.index}]"
    elif isinstance(seg, WildcardSegment):
        return "[*]"
    else:
        raise TypeError(f"Unknown segment type: {type(seg).__name__}")


def normalize_jsonpath(path: str) -> str:
    """Parse and re-serialize a JSONPath to canonical form.

    Canonical form rules:
        - Identifiers use dot notation: ``.name``
        - Non-identifier property names use bracket notation with canonical escaping
        - Integer indices are bare: ``[n]``
        - Wildcards are ``[*]``

    This form is used for cursor binding; format changes require a
    traversal_contract_version bump.
    """
    segments = parse_jsonpath(path)
    return "".join(_segment_to_canonical(seg) for seg in segments)


# ---------------------------------------------------------------------------
# Path classification helpers
# ---------------------------------------------------------------------------

def is_absolute(path: str) -> bool:
    """Return True if *path* starts with ``$``."""
    return path.startswith("$")


def is_relative(path: str) -> bool:
    """Return True if *path* does NOT start with ``$``."""
    return not path.startswith("$")


# ---------------------------------------------------------------------------
# Path evaluation
# ---------------------------------------------------------------------------

def evaluate_path(
    obj: Any,
    segments: list[Segment],
    max_wildcard_expansion: int = 10000,
    max_wildcard_expansion_total: int | None = None,
) -> list[tuple[list[Segment], Any]]:
    """Evaluate a parsed JSONPath against a Python object.

    Args:
        obj: The root Python object (typically a dict or list).
        segments: Parsed JSONPath segments (first must be RootSegment).
        max_wildcard_expansion: Maximum number of members a single wildcard
            may expand to before raising BudgetExceededError.
        max_wildcard_expansion_total: Optional cap on total wildcard expansions
            across the entire evaluation.

    Returns:
        List of ``(resolved_path_segments, value)`` pairs. Ordering follows
        the traversal contract (section 12.4): arrays by ascending index,
        objects by lexicographic key order.

    Raises:
        BudgetExceededError: If a wildcard expands more members than allowed.
        ValueError: If the first segment is not RootSegment.
    """
    if not segments:
        raise ValueError("Segment list must not be empty")
    if not isinstance(segments[0], RootSegment):
        raise ValueError("First segment must be RootSegment ($)")

    # Start with the root object and an initial resolved path of [$]
    results: list[tuple[list[Segment], Any]] = [([RootSegment()], obj)]
    total_expanded = 0

    for seg in segments[1:]:
        next_results: list[tuple[list[Segment], Any]] = []

        for resolved_path, current in results:
            if isinstance(seg, PropertySegment):
                if isinstance(current, dict) and seg.name in current:
                    next_results.append(
                        (resolved_path + [seg], current[seg.name])
                    )
                # If not a dict or key missing, this path branch is dropped

            elif isinstance(seg, IndexSegment):
                if isinstance(current, list) and 0 <= seg.index < len(current):
                    next_results.append(
                        (resolved_path + [seg], current[seg.index])
                    )
                # If not a list or index out of range, this path branch is dropped

            elif isinstance(seg, WildcardSegment):
                if isinstance(current, dict):
                    keys = sorted(current.keys())
                    if len(keys) > max_wildcard_expansion:
                        raise BudgetExceededError(
                            f"Wildcard expansion on object with {len(keys)} keys "
                            f"exceeds maximum of {max_wildcard_expansion}"
                        )
                    total_expanded += len(keys)
                    if (
                        max_wildcard_expansion_total is not None
                        and total_expanded > max_wildcard_expansion_total
                    ):
                        raise BudgetExceededError(
                            f"Total wildcard expansion {total_expanded} exceeds "
                            f"maximum of {max_wildcard_expansion_total}"
                        )
                    for key in keys:
                        next_results.append(
                            (
                                resolved_path + [PropertySegment(key)],
                                current[key],
                            )
                        )
                elif isinstance(current, list):
                    if len(current) > max_wildcard_expansion:
                        raise BudgetExceededError(
                            f"Wildcard expansion on array with {len(current)} "
                            f"elements exceeds maximum of {max_wildcard_expansion}"
                        )
                    total_expanded += len(current)
                    if (
                        max_wildcard_expansion_total is not None
                        and total_expanded > max_wildcard_expansion_total
                    ):
                        raise BudgetExceededError(
                            f"Total wildcard expansion {total_expanded} exceeds "
                            f"maximum of {max_wildcard_expansion_total}"
                        )
                    for idx in range(len(current)):
                        next_results.append(
                            (
                                resolved_path + [IndexSegment(idx)],
                                current[idx],
                            )
                        )
                # Wildcard on scalar: no results (branch dropped)

            else:
                raise TypeError(
                    f"Unexpected segment type during evaluation: "
                    f"{type(seg).__name__}"
                )

        results = next_results

    return results
