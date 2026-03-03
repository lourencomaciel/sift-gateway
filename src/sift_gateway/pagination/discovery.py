"""Heuristic upstream pagination discovery.

Discover likely pagination signals directly from response JSON when a
per-upstream pagination config is missing or incomplete.  Detection is
designed to be fast and conservative:

- prioritize explicit/common pagination paths
- parse ``next`` URLs for query-based cursors
- infer offset/page progression from request args
- flag suspicious limit-hit pages when pagination is unclear

This module is intentionally transport-agnostic: it does not read
gateway ``UpstreamConfig`` and only uses response payload, request args,
and optional metadata.  That keeps it reusable for non-MCP entrypoints
(for example a future CLI mode).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import re
from typing import Any
from urllib.parse import parse_qsl, urlparse

from sift_gateway.pagination.path_eval import evaluate_path as _evaluate_path


@dataclass(frozen=True)
class PaginationDiscovery:
    """Best-effort pagination discovery result.

    Attributes:
        has_more: Explicit has-more signal when discovered, otherwise None.
        next_params: Suggested next-call argument overrides when discovered.
        strategy: Detector strategy name that produced ``next_params``.
        confidence: Heuristic confidence in ``[0, 1]``.
        limit_hit: True when observed record count is at least request limit.
        rejected_reason: Optional detector rejection reason when
            has-more signals exist but no advancing continuation could
            be derived (for example ``"non_advancing_cursor"``).
        rejected_param: Likely request parameter causing a non-advancing
            cursor rejection when available (for example ``"after"``).
    """

    has_more: bool | None
    next_params: dict[str, Any] | None
    strategy: str | None
    confidence: float
    limit_hit: bool
    rejected_reason: str | None = None
    rejected_param: str | None = None


_PAGINATION_PARAM_NAMES = frozenset(
    {
        "after",
        "before",
        "cursor",
        "nextcursor",
        "pagetoken",
        "nextpagetoken",
        "continuationtoken",
        "token",
        "offset",
        "skip",
        "start",
        "page",
        "pagenumber",
        "pageindex",
    }
)

_NON_PAGINATION_QUERY_KEYS = frozenset(
    {
        # Keys are normalized via _normalize_name before comparison.
        "accesstoken",
        "apikey",
        "fields",
        "signature",
        "sig",
    }
)

_NON_ADVANCING_QUERY_PARAM_NAMES = frozenset(
    {
        "limit",
        "perpage",
        "pagesize",
        "maxresults",
        "count",
    }
)

_HEADER_NAMES = (
    "headers",
    "response_headers",
    "http_headers",
)

_HEADER_NEXT_URL_KEYS = (
    "x-next-page",
    "x-next-url",
    "next-page",
    "next-url",
)

_HEADER_CURSOR_KEYS = (
    "x-next-cursor",
    "x-cursor",
    "x-page-token",
    "x-next-page-token",
    "x-continuation-token",
)

_HEADER_HAS_MORE_KEYS = (
    "x-has-more",
    "x-has-next-page",
)

_TRUE_STRINGS = frozenset({"1", "true", "yes", "y", "on"})

_FALSE_STRINGS = frozenset({"0", "false", "no", "n", "off"})

_REJECTED_REASON_NON_ADVANCING_CURSOR = "non_advancing_cursor"

_LINK_NEXT_PATTERN = re.compile(
    r"\s*<([^>]+)>(.*)",
    re.IGNORECASE,
)

_LINK_PARAM_PATTERN = re.compile(
    r";\s*([!#$%&'*+\-.^_`|~0-9A-Za-z]+)\s*=\s*(\"[^\"]*\"|[^;,\s]+)",
    re.IGNORECASE,
)

_LIMIT_ARG_CANDIDATES = (
    "limit",
    "per_page",
    "page_size",
    "pageSize",
    "max_results",
    "maxResults",
    "count",
)

_OFFSET_ARG_CANDIDATES = (
    "offset",
    "skip",
    "start",
    "from",
)

_PAGE_ARG_CANDIDATES = (
    "page",
    "page_number",
    "pageNumber",
    "page_index",
    "pageIndex",
)

_HAS_NEXT_BOOL_PATHS = (
    "$.has_next",
    "$.hasNext",
    "$.has_next_page",
    "$.hasNextPage",
    "$.paging.has_next",
    "$.paging.hasNext",
    "$.paging.has_next_page",
    "$.paging.hasNextPage",
    "$.page_info.has_next_page",
    "$.pageInfo.hasNextPage",
    "$.result.page_info.has_next_page",
    "$.result.pageInfo.hasNextPage",
)

_HAS_MORE_BOOL_PATHS = (
    "$.has_more",
    "$.hasMore",
    "$.paging.has_more",
    "$.paging.hasMore",
    "$.pagination.has_more",
    "$.pagination.hasMore",
    "$.meta.has_more",
    "$.meta.hasMore",
    "$.result.has_more",
    "$.result.hasMore",
    "$.result.paging.has_more",
    "$.result.paging.hasMore",
    "$.result.pagination.has_more",
    "$.result.pagination.hasMore",
)

_IS_LAST_BOOL_PATHS = (
    "$.is_last",
    "$.isLast",
    "$.last_page",
    "$.lastPage",
    "$.paging.is_last",
    "$.paging.isLast",
    "$.paging.last_page",
    "$.paging.lastPage",
    "$.page_info.is_last_page",
    "$.pageInfo.isLastPage",
    "$.result.page_info.is_last_page",
    "$.result.pageInfo.isLastPage",
)

_NEXT_VALUE_PATHS = (
    "$.next",
    "$.next_page",
    "$.nextPage",
    "$.next_url",
    "$.nextUrl",
    "$.paging.next",
    "$.paging.next_url",
    "$.paging.nextUrl",
    "$.pagination.next",
    "$.pagination.next_url",
    "$.pagination.nextUrl",
    "$.links.next",
    "$.links.next.href",
    "$.result.next",
    "$.result.next_page",
    "$.result.nextPage",
    "$.result.next_url",
    "$.result.nextUrl",
    "$.result.paging.next",
    "$.result.paging.next_url",
    "$.result.paging.nextUrl",
    "$.result.pagination.next",
    "$.result.pagination.next_url",
    "$.result.pagination.nextUrl",
    "$.result.links.next",
    "$.result.links.next.href",
)

_NEXT_URL_PATHS = (
    "$.paging.next",
    "$.paging.next_url",
    "$.paging.nextUrl",
    "$.pagination.next",
    "$.pagination.next_url",
    "$.pagination.nextUrl",
    "$.links.next",
    "$.links.next.href",
    "$.next",
    "$.next_url",
    "$.nextUrl",
    "$.result.paging.next",
    "$.result.paging.next_url",
    "$.result.paging.nextUrl",
    "$.result.pagination.next",
    "$.result.pagination.next_url",
    "$.result.pagination.nextUrl",
    "$.result.links.next",
    "$.result.links.next.href",
    "$.result.next",
    "$.result.next_url",
    "$.result.nextUrl",
)

_CURSOR_PATH_SPECS: tuple[tuple[str, tuple[str, ...], float], ...] = (
    ("$.result.paging.cursors.after", ("after", "cursor"), 0.99),
    ("$.result.paging.after", ("after", "cursor"), 0.95),
    (
        "$.result.paging.next_cursor",
        ("next_cursor", "cursor", "after"),
        0.94,
    ),
    (
        "$.result.paging.nextCursor",
        ("nextCursor", "cursor", "after"),
        0.94,
    ),
    (
        "$.result.pagination.next_cursor",
        ("next_cursor", "cursor", "after"),
        0.93,
    ),
    (
        "$.result.pagination.nextCursor",
        ("nextCursor", "cursor", "after"),
        0.93,
    ),
    ("$.paging.cursors.after", ("after", "cursor"), 0.99),
    ("$.paging.after", ("after", "cursor"), 0.95),
    ("$.paging.next_cursor", ("next_cursor", "cursor", "after"), 0.94),
    ("$.paging.nextCursor", ("nextCursor", "cursor", "after"), 0.94),
    (
        "$.pagination.next_cursor",
        ("next_cursor", "cursor", "after"),
        0.93,
    ),
    (
        "$.pagination.nextCursor",
        ("nextCursor", "cursor", "after"),
        0.93,
    ),
    ("$.next_cursor", ("next_cursor", "cursor", "after"), 0.9),
    ("$.nextCursor", ("nextCursor", "cursor", "after"), 0.9),
    ("$.result.next_cursor", ("next_cursor", "cursor", "after"), 0.9),
    ("$.result.nextCursor", ("nextCursor", "cursor", "after"), 0.9),
    ("$.cursor", ("cursor", "after"), 0.82),
    ("$.page_info.end_cursor", ("after", "cursor", "end_cursor"), 0.92),
    ("$.pageInfo.endCursor", ("after", "cursor", "endCursor"), 0.92),
    (
        "$.next_page_token",
        ("next_page_token", "page_token", "token"),
        0.92,
    ),
    (
        "$.nextPageToken",
        ("nextPageToken", "pageToken", "page_token", "token"),
        0.92,
    ),
    (
        "$.continuation_token",
        ("continuation_token", "continuationToken", "page_token", "token"),
        0.9,
    ),
    (
        "$.continuationToken",
        ("continuationToken", "continuation_token", "pageToken", "token"),
        0.9,
    ),
    (
        "$.response_metadata.next_page_token",
        ("next_page_token", "page_token", "token"),
        0.88,
    ),
    (
        "$.result.next_page_token",
        ("next_page_token", "page_token", "token"),
        0.88,
    ),
    (
        "$.result.response_metadata.next_page_token",
        ("next_page_token", "page_token", "token"),
        0.88,
    ),
)


def _normalize_name(name: str) -> str:
    """Normalize key/param names for fuzzy matching."""
    return (
        name.strip()
        .lower()
        .replace("_", "")
        .replace("-", "")
        .replace(".", "")
        .replace(" ", "")
    )


def _arg_name_lookup(original_args: dict[str, Any]) -> dict[str, str]:
    """Map normalized arg names back to their original key casing."""
    lookup: dict[str, str] = {}
    for key in original_args:
        if isinstance(key, str) and key:
            lookup.setdefault(_normalize_name(key), key)
    return lookup


def _select_param_name(
    *,
    candidates: Iterable[str],
    original_args: dict[str, Any],
) -> str:
    """Choose the most likely request param name for a discovered token."""
    arg_lookup = _arg_name_lookup(original_args)
    for candidate in candidates:
        normalized = _normalize_name(candidate)
        if normalized in arg_lookup:
            return arg_lookup[normalized]
    for candidate in candidates:
        if candidate:
            return candidate
    return "cursor"


def _is_non_empty_scalar(value: Any) -> bool:
    """Return True for non-empty scalar values suitable as params."""
    if isinstance(value, bool):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return isinstance(value, (int, float))


def _looks_like_url(value: str) -> bool:
    """Return True when a string looks like a URL."""
    stripped = value.strip()
    if not stripped:
        return False
    if "://" in stripped or stripped.startswith(("/", "?")):
        return True
    return bool(urlparse(stripped).query)


def _is_pagination_param_name(name: str) -> bool:
    """Return True when query arg name looks pagination-related."""
    return _normalize_name(name) in _PAGINATION_PARAM_NAMES


def _is_non_advancing_param_name(name: str) -> bool:
    """Return True for params that tune page size but not position."""
    return _normalize_name(name) in _NON_ADVANCING_QUERY_PARAM_NAMES


def _coerce_query_value_like_example(
    *,
    raw_value: str,
    example_value: Any,
) -> Any:
    """Coerce query value to match the type used in original args."""
    if isinstance(example_value, bool):
        normalized = raw_value.strip().lower()
        if normalized in _TRUE_STRINGS:
            return True
        if normalized in _FALSE_STRINGS:
            return False
        return raw_value
    if isinstance(example_value, int) and not isinstance(example_value, bool):
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return raw_value
    if isinstance(example_value, float):
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return raw_value
    return raw_value


def _collect_next_query_params(
    *,
    query_pairs: list[tuple[str, str]],
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Select and type-coerce query params that likely advance paging."""
    selected: dict[str, Any] = {}
    has_advancing_signal = False
    original_arg_lookup = _arg_name_lookup(original_args)
    for raw_name, raw_value in query_pairs:
        normalized = _normalize_name(raw_name)
        if not normalized:
            continue
        if normalized in _NON_PAGINATION_QUERY_KEYS:
            continue
        original_arg_key = original_arg_lookup.get(normalized)
        in_original_args = original_arg_key is not None
        is_pagination_param = _is_pagination_param_name(raw_name)
        if not (in_original_args or is_pagination_param):
            continue

        if in_original_args:
            assert original_arg_key is not None
            example_value = original_args[original_arg_key]
            coerced_value = _coerce_query_value_like_example(
                raw_value=raw_value,
                example_value=example_value,
            )
            # Keep the canonical arg key casing from the original
            # request so merge/update operations overwrite correctly.
            selected[original_arg_key] = coerced_value
            if (
                not _is_non_advancing_param_name(original_arg_key)
                and coerced_value != example_value
            ):
                has_advancing_signal = True
            continue

        selected[raw_name] = raw_value
        if not _is_non_advancing_param_name(raw_name):
            has_advancing_signal = True

    if not selected or not has_advancing_signal:
        return None, 0.0

    intersects_request_args = any(key in original_args for key in selected)
    return selected, 0.97 if intersects_request_args else 0.9


def _discover_next_params_from_url(
    *,
    json_value: Any,
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Extract next params from common next-URL fields."""
    for path in _NEXT_URL_PATHS:
        raw_value = _evaluate_path(json_value, path)
        if not isinstance(raw_value, str):
            continue
        url_value = raw_value.strip()
        if not url_value or not _looks_like_url(url_value):
            continue
        parsed = urlparse(url_value)
        query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
        if not query_pairs:
            continue
        selected, confidence = _collect_next_query_params(
            query_pairs=query_pairs,
            original_args=original_args,
        )
        if selected is not None:
            return selected, confidence
    return None, 0.0


def _extract_raw_headers(
    upstream_meta: Any,
) -> dict[str, Any] | None:
    """Return first header-like mapping from upstream metadata."""
    if not isinstance(upstream_meta, dict):
        return None
    for key in _HEADER_NAMES:
        candidate = upstream_meta.get(key)
        if isinstance(candidate, dict):
            return candidate
    return None


def _coerce_header_value(
    *,
    header_name: str,
    value: Any,
) -> str | None:
    """Normalize a header value to one string or None."""
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return None
    scalar_values = [
        str(item) for item in value if isinstance(item, (str, int, float))
    ]
    if not scalar_values:
        return None
    if header_name == "link":
        # Preserve all link entries so rel="next" can be found
        # even when it is not the first list element.
        return ", ".join(scalar_values)
    return scalar_values[0]


def _normalize_headers(
    upstream_meta: Any,
) -> dict[str, str]:
    """Extract lowercase header map from upstream meta payload."""
    raw_headers = _extract_raw_headers(upstream_meta)
    if raw_headers is None:
        return {}

    normalized: dict[str, str] = {}
    for key, value in raw_headers.items():
        if not isinstance(key, str):
            continue
        normalized_key = key.strip().lower()
        normalized_value = _coerce_header_value(
            header_name=normalized_key,
            value=value,
        )
        if normalized_value is not None:
            normalized[normalized_key] = normalized_value
    return normalized


def _next_params_from_url_query(
    *,
    url_value: str,
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Parse URL query parameters and return pagination-relevant ones."""
    parsed = urlparse(url_value)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    if not query_pairs:
        return None, 0.0
    return _collect_next_query_params(
        query_pairs=query_pairs,
        original_args=original_args,
    )


def _split_link_header_entries(link_value: str) -> list[str]:
    """Split a Link header value into top-level entries."""
    link_entries: list[str] = []
    current: list[str] = []
    in_quotes = False
    in_angle = False
    for ch in link_value:
        if ch == '"' and not in_angle:
            in_quotes = not in_quotes
        elif ch == "<" and not in_quotes:
            in_angle = True
        elif ch == ">" and in_angle:
            in_angle = False
        if ch == "," and not in_quotes and not in_angle:
            entry = "".join(current).strip()
            if entry:
                link_entries.append(entry)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        link_entries.append(tail)
    return link_entries


def _has_rel_next(param_blob: str) -> bool:
    """Return True when Link params include rel=next."""
    for param_match in _LINK_PARAM_PATTERN.finditer(param_blob):
        name = param_match.group(1).strip().lower()
        if name != "rel":
            continue
        raw_value = param_match.group(2).strip()
        if raw_value.startswith('"') and raw_value.endswith('"'):
            raw_value = raw_value[1:-1]
        rel_tokens = {
            token.strip().lower()
            for token in raw_value.split()
            if token.strip()
        }
        if "next" in rel_tokens:
            return True
    return False


def _discover_link_next_url(headers: dict[str, str]) -> str | None:
    """Extract rel=next URL from RFC5988 Link header."""
    link_value = headers.get("link")
    if not isinstance(link_value, str) or not link_value.strip():
        return None

    for entry in _split_link_header_entries(link_value):
        match = _LINK_NEXT_PATTERN.match(entry)
        if match is None:
            continue
        url_value = match.group(1).strip()
        if not url_value:
            continue
        if _has_rel_next(match.group(2)):
            return url_value
    return None


def _discover_has_more_from_headers(
    headers: dict[str, str],
) -> bool | None:
    """Detect has-more booleans from common pagination headers."""
    for key in _HEADER_HAS_MORE_KEYS:
        raw_value = headers.get(key)
        if not isinstance(raw_value, str):
            continue
        normalized = raw_value.strip().lower()
        if normalized in _TRUE_STRINGS:
            return True
        if normalized in _FALSE_STRINGS:
            return False
    if _discover_link_next_url(headers) is not None:
        return True
    if any(
        isinstance(headers.get(key), str) and headers.get(key, "").strip()
        for key in _HEADER_NEXT_URL_KEYS
    ):
        return True
    if any(
        isinstance(headers.get(key), str) and headers.get(key, "").strip()
        for key in _HEADER_CURSOR_KEYS
    ):
        return True
    return None


def _cursor_header_candidates(header_key: str) -> tuple[str, ...]:
    """Return candidate request args for a cursor-related header key."""
    if "page-token" in header_key:
        return (
            "nextPageToken",
            "next_page_token",
            "pageToken",
            "page_token",
            "token",
        )
    if "continuation" in header_key:
        return (
            "continuationToken",
            "continuation_token",
            "pageToken",
            "page_token",
            "token",
        )
    return (
        "after",
        "cursor",
        "nextCursor",
        "next_cursor",
        "pageToken",
        "token",
    )


def _advances_existing_arg(
    *,
    candidates: Iterable[str],
    new_value: Any,
    original_args: dict[str, Any],
    arg_lookup: dict[str, str],
) -> bool:
    """Return True when candidates advance at least one matching arg."""
    matched_arg = False
    for candidate in candidates:
        existing_key = arg_lookup.get(_normalize_name(candidate))
        if existing_key is None:
            continue
        matched_arg = True
        if original_args.get(existing_key) != new_value:
            return True
    return not matched_arg


def _discover_link_header_params(
    *,
    headers: dict[str, str],
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Try Link header first because it is explicit and standardized."""
    link_next = _discover_link_next_url(headers)
    if not isinstance(link_next, str):
        return None, 0.0
    return _next_params_from_url_query(
        url_value=link_next,
        original_args=original_args,
    )


def _discover_next_url_header_params(
    *,
    headers: dict[str, str],
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Parse x-next-url style headers."""
    for key in _HEADER_NEXT_URL_KEYS:
        raw_value = headers.get(key)
        if not isinstance(raw_value, str):
            continue
        params, confidence = _next_params_from_url_query(
            url_value=raw_value.strip(),
            original_args=original_args,
        )
        if params is not None:
            return params, confidence
    return None, 0.0


def _discover_cursor_header_params(
    *,
    headers: dict[str, str],
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Read cursor/token values from headers and map to request args."""
    arg_lookup = _arg_name_lookup(original_args)
    for key in _HEADER_CURSOR_KEYS:
        raw_value = headers.get(key)
        if not isinstance(raw_value, str):
            continue
        cursor_value = raw_value.strip()
        if not cursor_value:
            continue
        candidates = _cursor_header_candidates(key)
        if not _advances_existing_arg(
            candidates=candidates,
            new_value=cursor_value,
            original_args=original_args,
            arg_lookup=arg_lookup,
        ):
            continue
        param_name = _select_param_name(
            candidates=candidates,
            original_args=original_args,
        )
        return {param_name: cursor_value}, 0.88
    return None, 0.0


def _discover_next_params_from_headers(
    *,
    original_args: dict[str, Any],
    upstream_meta: Any,
) -> tuple[dict[str, Any] | None, float, str | None]:
    """Discover next params from response headers."""
    headers = _normalize_headers(upstream_meta)
    if not headers:
        return None, 0.0, None

    params, confidence = _discover_link_header_params(
        headers=headers,
        original_args=original_args,
    )
    if params is not None:
        return params, confidence, "header_link"

    params, confidence = _discover_next_url_header_params(
        headers=headers,
        original_args=original_args,
    )
    if params is not None:
        return params, confidence, "header_next_url"

    params, confidence = _discover_cursor_header_params(
        headers=headers,
        original_args=original_args,
    )
    if params is not None:
        return params, confidence, "header_cursor"

    return None, 0.0, None


def _discover_next_params_from_paths(
    *,
    json_value: Any,
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Extract next params from common cursor/token response paths."""
    arg_lookup = _arg_name_lookup(original_args)
    for path, param_candidates, confidence in _CURSOR_PATH_SPECS:
        raw_value = _evaluate_path(json_value, path)
        if not _is_non_empty_scalar(raw_value):
            continue
        # Guard against same-page loops when upstream echoes the current
        # cursor/token instead of returning an advancing value.
        if not _advances_existing_arg(
            candidates=param_candidates,
            new_value=raw_value,
            original_args=original_args,
            arg_lookup=arg_lookup,
        ):
            continue
        param_name = _select_param_name(
            candidates=param_candidates,
            original_args=original_args,
        )
        return {param_name: raw_value}, confidence
    return None, 0.0


def _discover_next_params_from_next_object(
    *,
    json_value: Any,
    original_args: dict[str, Any],
) -> tuple[dict[str, Any] | None, float]:
    """Extract next params from ``next`` objects."""
    arg_lookup = _arg_name_lookup(original_args)
    next_candidates = (
        _evaluate_path(json_value, "$.next"),
        _evaluate_path(json_value, "$.paging.next"),
        _evaluate_path(json_value, "$.pagination.next"),
        _evaluate_path(json_value, "$.result.next"),
        _evaluate_path(json_value, "$.result.paging.next"),
        _evaluate_path(json_value, "$.result.pagination.next"),
    )
    for candidate in next_candidates:
        if not isinstance(candidate, dict):
            continue
        selected: dict[str, Any] = {}
        for key, value in candidate.items():
            if not isinstance(key, str):
                continue
            if not _is_non_empty_scalar(value):
                continue
            original_arg_key = arg_lookup.get(_normalize_name(key))
            if original_arg_key is not None:
                if original_args.get(original_arg_key) == value:
                    continue
                # Reuse the original request key casing/shape so merge
                # operations overwrite existing args instead of creating
                # duplicate aliases.
                selected[original_arg_key] = value
                continue
            if _is_pagination_param_name(key):
                selected[key] = value
        if selected:
            confidence = 0.85 if len(selected) == 1 else 0.8
            return selected, confidence
    return None, 0.0


def _read_bool_path(data: Any, path: str) -> bool | None:
    """Read a boolean path; return None when not present/non-bool."""
    value = _evaluate_path(data, path)
    if isinstance(value, bool):
        return value
    return None


def _nested_presence_truthy(value: Any) -> bool:
    """Return truthiness for nested values under ``next``-like objects."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value)
    if isinstance(value, dict):
        return bool(value)
    return isinstance(value, (int, float))


def _read_presence_signal(data: Any, path: str) -> bool | None:
    """Read presence signal from path value."""
    value = _evaluate_path(data, path)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value)
    if isinstance(value, dict):
        return any(_nested_presence_truthy(nested) for nested in value.values())
    return True


def _discover_has_more_signal(json_value: Any) -> bool | None:
    """Discover explicit has-more signal from common response paths."""
    for path in _HAS_NEXT_BOOL_PATHS:
        value = _read_bool_path(json_value, path)
        if value is not None:
            return value
    for path in _HAS_MORE_BOOL_PATHS:
        value = _read_bool_path(json_value, path)
        if value is not None:
            return value
    for path in _IS_LAST_BOOL_PATHS:
        value = _read_bool_path(json_value, path)
        if value is not None:
            return not value
    for path in _NEXT_VALUE_PATHS:
        value = _read_presence_signal(json_value, path)
        if value is not None:
            return value
    return None


def _first_int_at_least(
    args: dict[str, Any],
    candidates: tuple[str, ...],
    *,
    minimum: int,
) -> tuple[str, int] | None:
    """Return first integer arg meeting a minimum value."""
    normalized = _arg_name_lookup(args)
    for candidate in candidates:
        arg_key = normalized.get(_normalize_name(candidate))
        if arg_key is None:
            continue
        value = args.get(arg_key)
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value >= minimum
        ):
            return arg_key, value
    return None


def _infer_record_count(json_value: Any) -> int | None:
    """Infer record count from common record-array paths."""
    if isinstance(json_value, list):
        return len(json_value)
    record_paths = (
        "$.data",
        "$.results",
        "$.items",
        "$.records",
        "$.entries",
        "$.nodes",
        "$.result.data",
        "$.result.items",
    )
    for path in record_paths:
        candidate = _evaluate_path(json_value, path)
        if isinstance(candidate, list):
            return len(candidate)
    return None


def _discover_limit_hit(
    *,
    json_value: Any,
    original_args: dict[str, Any],
) -> bool:
    """Detect suspicious page-size saturation (records >= requested limit)."""
    limit_pair = _first_int_at_least(
        original_args,
        _LIMIT_ARG_CANDIDATES,
        minimum=1,
    )
    if limit_pair is None:
        return False
    _limit_key, limit_value = limit_pair
    observed = _infer_record_count(json_value)
    if observed is None:
        return False
    return observed >= limit_value


def _infer_numeric_progression(
    *,
    original_args: dict[str, Any],
    has_more: bool | None,
    limit_hit: bool,
) -> tuple[dict[str, Any] | None, float]:
    """Infer offset/page progression from existing request args."""
    if not (has_more is True or limit_hit):
        return None, 0.0
    limit_pair = _first_int_at_least(
        original_args,
        _LIMIT_ARG_CANDIDATES,
        minimum=1,
    )
    offset_pair = _first_int_at_least(
        original_args,
        _OFFSET_ARG_CANDIDATES,
        minimum=0,
    )
    if limit_pair is not None and offset_pair is not None:
        offset_key, offset_value = offset_pair
        _limit_key, limit_value = limit_pair
        return {offset_key: offset_value + limit_value}, 0.72
    page_pair = _first_int_at_least(
        original_args,
        _PAGE_ARG_CANDIDATES,
        minimum=0,
    )
    if page_pair is not None:
        page_key, page_value = page_pair
        return {page_key: page_value + 1}, 0.68
    return None, 0.0


def _discover_next_params(
    *,
    json_value: Any,
    original_args: dict[str, Any],
    upstream_meta: Any | None,
    has_more_signal: bool | None,
    limit_hit: bool,
) -> tuple[dict[str, Any] | None, float, str | None]:
    """Try pagination next-param detectors in descending confidence."""
    next_params, confidence = _discover_next_params_from_url(
        json_value=json_value,
        original_args=original_args,
    )
    if next_params is not None:
        return next_params, confidence, "next_url_query"

    next_params, confidence = _discover_next_params_from_paths(
        json_value=json_value,
        original_args=original_args,
    )
    if next_params is not None:
        return next_params, confidence, "cursor_path"

    next_params, confidence = _discover_next_params_from_next_object(
        json_value=json_value,
        original_args=original_args,
    )
    if next_params is not None:
        return next_params, confidence, "next_object"

    next_params, confidence, strategy = _discover_next_params_from_headers(
        original_args=original_args,
        upstream_meta=upstream_meta,
    )
    if next_params is not None:
        return next_params, confidence, strategy

    next_params, confidence = _infer_numeric_progression(
        original_args=original_args,
        has_more=has_more_signal,
        limit_hit=limit_hit,
    )
    if next_params is not None:
        return next_params, confidence, "numeric_args"

    return None, 0.0, None


def _query_params_are_non_advancing(
    *,
    query_pairs: list[tuple[str, str]],
    original_args: dict[str, Any],
) -> bool:
    """Return True when pagination query params only echo request position."""
    arg_lookup = _arg_name_lookup(original_args)
    saw_pagination_param = False
    for raw_name, raw_value in query_pairs:
        normalized = _normalize_name(raw_name)
        if not normalized:
            continue
        existing_key = arg_lookup.get(normalized)
        is_known_pagination = _is_pagination_param_name(raw_name)
        if existing_key is None and not is_known_pagination:
            continue
        if existing_key is None:
            continue
        if _is_non_advancing_param_name(existing_key):
            continue
        saw_pagination_param = True
        example_value = original_args.get(existing_key)
        coerced = _coerce_query_value_like_example(
            raw_value=raw_value,
            example_value=example_value,
        )
        if coerced != example_value:
            return False
    return saw_pagination_param


def _looks_non_advancing_next_url(
    *,
    url_value: str,
    original_args: dict[str, Any],
) -> bool:
    """Return True when next URL includes cursor params but no progression."""
    parsed = urlparse(url_value)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
    if not query_pairs:
        return False
    return _query_params_are_non_advancing(
        query_pairs=query_pairs,
        original_args=original_args,
    )


def _detect_non_advancing_cursor_rejection(
    *,
    json_value: Any,
    original_args: dict[str, Any],
    upstream_meta: Any | None,
) -> str | None:
    """Return rejection reason when continuation tokens do not advance."""
    arg_lookup = _arg_name_lookup(original_args)

    for path, param_candidates, _confidence in _CURSOR_PATH_SPECS:
        raw_value = _evaluate_path(json_value, path)
        if not _is_non_empty_scalar(raw_value):
            continue
        for candidate in param_candidates:
            existing_key = arg_lookup.get(_normalize_name(candidate))
            if existing_key is None:
                continue
            if _is_non_advancing_param_name(existing_key):
                continue
            if original_args.get(existing_key) == raw_value:
                return _REJECTED_REASON_NON_ADVANCING_CURSOR

    next_candidates = (
        _evaluate_path(json_value, "$.next"),
        _evaluate_path(json_value, "$.paging.next"),
        _evaluate_path(json_value, "$.pagination.next"),
        _evaluate_path(json_value, "$.result.next"),
        _evaluate_path(json_value, "$.result.paging.next"),
        _evaluate_path(json_value, "$.result.pagination.next"),
    )
    for next_candidate in next_candidates:
        if isinstance(next_candidate, str) and _looks_like_url(next_candidate) and (
            _looks_non_advancing_next_url(
                url_value=next_candidate,
                original_args=original_args,
            )
        ):
            return _REJECTED_REASON_NON_ADVANCING_CURSOR
        if not isinstance(next_candidate, dict):
            continue
        for key, value in next_candidate.items():
            if not isinstance(key, str):
                continue
            if not _is_non_empty_scalar(value):
                continue
            existing_key = arg_lookup.get(_normalize_name(key))
            if existing_key is None:
                continue
            if _is_non_advancing_param_name(existing_key):
                continue
            if original_args.get(existing_key) == value:
                return _REJECTED_REASON_NON_ADVANCING_CURSOR

    for path in _NEXT_URL_PATHS:
        raw_value = _evaluate_path(json_value, path)
        if not isinstance(raw_value, str):
            continue
        url_value = raw_value.strip()
        if not url_value or not _looks_like_url(url_value):
            continue
        if _looks_non_advancing_next_url(
            url_value=url_value,
            original_args=original_args,
        ):
            return _REJECTED_REASON_NON_ADVANCING_CURSOR

    headers = _normalize_headers(upstream_meta)
    if headers:
        link_next = _discover_link_next_url(headers)
        if isinstance(link_next, str) and _looks_non_advancing_next_url(
            url_value=link_next,
            original_args=original_args,
        ):
            return _REJECTED_REASON_NON_ADVANCING_CURSOR
        for key in _HEADER_NEXT_URL_KEYS:
            raw_value = headers.get(key)
            if not isinstance(raw_value, str):
                continue
            if _looks_non_advancing_next_url(
                url_value=raw_value.strip(),
                original_args=original_args,
            ):
                return _REJECTED_REASON_NON_ADVANCING_CURSOR
        for key in _HEADER_CURSOR_KEYS:
            raw_value = headers.get(key)
            if not isinstance(raw_value, str):
                continue
            cursor_value = raw_value.strip()
            if not cursor_value:
                continue
            for candidate in _cursor_header_candidates(key):
                existing_key = arg_lookup.get(_normalize_name(candidate))
                if existing_key is None:
                    continue
                if _is_non_advancing_param_name(existing_key):
                    continue
                if original_args.get(existing_key) == cursor_value:
                    return _REJECTED_REASON_NON_ADVANCING_CURSOR

    return None


def _likely_rejected_cursor_param(
    *,
    original_args: dict[str, Any],
) -> str | None:
    """Return a best-effort likely cursor parameter from request args."""
    for key in original_args:
        if not isinstance(key, str) or not key:
            continue
        if not _is_non_empty_scalar(original_args.get(key)):
            continue
        normalized = _normalize_name(key)
        if not normalized:
            continue
        if normalized not in _PAGINATION_PARAM_NAMES:
            continue
        if _is_non_advancing_param_name(key):
            continue
        return key
    return None


def discover_pagination(
    *,
    json_value: Any,
    original_args: dict[str, Any],
    upstream_meta: Any | None = None,
) -> PaginationDiscovery:
    """Discover pagination signals from upstream response JSON.

    Args:
        json_value: Parsed JSON payload from the upstream response.
        original_args: Forwarded request arguments for this page.
        upstream_meta: Optional upstream metadata payload (headers, etc.).

    Returns:
        ``PaginationDiscovery`` with discovered has-more signal and
        next-argument overrides when available.
    """
    has_more_signal = _discover_has_more_signal(json_value)
    header_has_more = _discover_has_more_from_headers(
        _normalize_headers(upstream_meta)
    )
    if has_more_signal is None:
        has_more_signal = header_has_more
    limit_hit = _discover_limit_hit(
        json_value=json_value,
        original_args=original_args,
    )

    next_params, confidence, strategy = _discover_next_params(
        json_value=json_value,
        original_args=original_args,
        upstream_meta=upstream_meta,
        has_more_signal=has_more_signal,
        limit_hit=limit_hit,
    )

    rejected_reason: str | None = None
    rejected_param: str | None = None
    if has_more_signal is True and next_params is None:
        rejected_reason = _detect_non_advancing_cursor_rejection(
            json_value=json_value,
            original_args=original_args,
            upstream_meta=upstream_meta,
        )
        if rejected_reason == _REJECTED_REASON_NON_ADVANCING_CURSOR:
            rejected_param = _likely_rejected_cursor_param(
                original_args=original_args
            )

    if has_more_signal is None and next_params is not None:
        has_more_signal = True

    return PaginationDiscovery(
        has_more=has_more_signal,
        next_params=next_params,
        strategy=strategy,
        confidence=confidence,
        limit_hit=limit_hit,
        rejected_reason=rejected_reason,
        rejected_param=rejected_param,
    )
