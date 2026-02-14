"""Auto-pagination: merge multiple upstream pages into one artifact.

Pure functions for the auto-pagination loop that fetches successive
upstream pages and merges their JSON content into a single envelope.
Exports ``AutoPaginationResult``, ``resolve_auto_paginate_limits``,
``merge_envelopes``, and record-counting helpers.

Typical usage example::

    limits = resolve_auto_paginate_limits(gateway_cfg, upstream_cfg)
    if limits.max_pages > 1:
        result = auto_paginate(...)
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

from sift_mcp.config.settings import GatewayConfig, UpstreamConfig
from sift_mcp.envelope.model import (
    ContentPart,
    Envelope,
    JsonContentPart,
)
from sift_mcp.pagination.extract import PaginationAssessment


@dataclass(frozen=True)
class AutoPaginationLimits:
    """Resolved auto-pagination limits for a request.

    Attributes:
        max_pages: Maximum pages to fetch (0 or 1 disables).
        max_records: Approximate record budget used as a stop
            threshold for additional page fetches.
        timeout: Timeout in seconds for the loop.
    """

    max_pages: int
    max_records: int
    timeout: float


@dataclass(frozen=True)
class AutoPaginationResult:
    """Result of the auto-pagination loop.

    Attributes:
        envelope: Merged envelope with all fetched JSON data.
        assessment: Final pagination assessment from the last
            successfully fetched page.
        pages_fetched: Total number of pages fetched (including
            the initial page).
        total_records: Total record count across all pages.
        stopped_reason: Why the loop stopped (``"complete"``,
            ``"max_pages"``, ``"max_records"``, ``"timeout"``,
            ``"error"``, ``"binary_content"``).
        binary_refs: Binary refs produced by oversize JSON
            replacement during auto-fetched pages.
    """

    envelope: Envelope
    assessment: PaginationAssessment
    pages_fetched: int
    total_records: int
    stopped_reason: str
    binary_refs: list[Any] = field(default_factory=list)


def resolve_auto_paginate_limits(
    gateway_config: GatewayConfig,
    upstream_config: UpstreamConfig,
) -> AutoPaginationLimits:
    """Resolve auto-pagination limits with per-upstream overrides.

    Per-upstream fields override gateway defaults when not None.

    Args:
        gateway_config: Gateway-level configuration.
        upstream_config: Per-upstream configuration.

    Returns:
        Resolved ``AutoPaginationLimits``.
    """
    max_pages = (
        upstream_config.auto_paginate_max_pages
        if upstream_config.auto_paginate_max_pages is not None
        else gateway_config.auto_paginate_max_pages
    )
    max_records = (
        upstream_config.auto_paginate_max_records
        if upstream_config.auto_paginate_max_records is not None
        else gateway_config.auto_paginate_max_records
    )
    timeout = (
        upstream_config.auto_paginate_timeout_seconds
        if upstream_config.auto_paginate_timeout_seconds is not None
        else gateway_config.auto_paginate_timeout_seconds
    )
    return AutoPaginationLimits(
        max_pages=max_pages,
        max_records=max_records,
        timeout=timeout,
    )


_COMMON_WRAPPER_KEYS = ("data", "results", "items", "records", "entries")


def _extract_json_content(envelope: Envelope) -> Any | None:
    """Extract the JSON value from the first JsonContentPart.

    Args:
        envelope: Envelope to inspect.

    Returns:
        The JSON value, or ``None`` if no JSON part exists.
    """
    for part in envelope.content:
        if isinstance(part, JsonContentPart):
            return part.value
    return None


def _count_json_records(envelope: Envelope) -> int:
    """Count records in the first JSON content part.

    Checks common wrapper keys (``data``, ``results``, ``items``,
    ``records``, ``entries``) for a list value, or treats a bare
    list as the record set.

    Args:
        envelope: Envelope whose first JSON part to count.

    Returns:
        Number of records found, or 0 if no countable content.
    """
    value = _extract_json_content(envelope)
    if value is None:
        return 0
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in _COMMON_WRAPPER_KEYS:
            child = value.get(key)
            if isinstance(child, list):
                return len(child)
    return 0


def _count_json_value_records(value: Any) -> int:
    """Count records in a raw JSON value.

    Mirrors ``_count_json_records`` but operates on an already-
    extracted JSON value rather than an ``Envelope``.

    Args:
        value: A JSON-compatible Python value.

    Returns:
        Number of records found, or 0 if not countable.
    """
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in _COMMON_WRAPPER_KEYS:
            child = value.get(key)
            if isinstance(child, list):
                return len(child)
    return 0


def _merge_json_values(base: Any, additional: Any) -> Any:
    """Merge two JSON values by concatenating their record arrays.

    Handles three cases:
    1. Both are dicts with a common wrapper key containing a list
       -- concatenate the lists under that key.  Top-level wrapper
       metadata (e.g. ``next``, ``has_more``) is taken from
       *additional* so the merged result reflects the latest page.
    2. Both are bare lists -- concatenate them.
    3. Fallback -- wrap both in a list.

    Args:
        base: Base JSON value (from first page).
        additional: Additional JSON value to merge in.

    Returns:
        Merged JSON value.
    """
    if isinstance(base, dict) and isinstance(additional, dict):
        for key in _COMMON_WRAPPER_KEYS:
            base_list = base.get(key)
            add_list = additional.get(key)
            if isinstance(base_list, list) and isinstance(add_list, list):
                merged = dict(additional)
                merged[key] = base_list + add_list
                return merged
    if isinstance(base, list) and isinstance(additional, list):
        return base + additional
    return [base, additional]


def merge_envelopes(
    base: Envelope,
    additional_json_values: list[Any],
    final_assessment: PaginationAssessment,
) -> Envelope:
    """Merge additional JSON values into a base envelope.

    Replaces the first ``JsonContentPart`` in the base envelope
    with a merged version containing all additional page data.
    Updates pagination state from the final assessment.

    Args:
        base: Envelope from the first page.
        additional_json_values: JSON values from subsequent pages.
        final_assessment: Assessment from the last fetched page.

    Returns:
        New envelope with merged JSON content and updated
        pagination meta.
    """
    base_json = _extract_json_content(base)
    if base_json is None:
        return base

    merged_value = base_json
    for val in additional_json_values:
        merged_value = _merge_json_values(merged_value, val)

    new_parts: list[ContentPart] = []
    replaced = False
    for part in base.content:
        if isinstance(part, JsonContentPart) and not replaced:
            new_parts.append(JsonContentPart(value=merged_value))
            replaced = True
        else:
            new_parts.append(part)

    new_meta = dict(base.meta)
    if final_assessment.state is not None:
        new_meta["_gateway_pagination"] = final_assessment.state.to_dict()
    elif "_gateway_pagination" in new_meta:
        del new_meta["_gateway_pagination"]

    return dataclasses.replace(
        base,
        content=new_parts,
        meta=new_meta,
    )
