from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from sift_mcp.config.settings import PaginationConfig, UpstreamConfig
from sift_mcp.envelope.model import Envelope, JsonContentPart, TextContentPart
from sift_mcp.mcp.handlers.mirrored_tool import (
    _auto_paginate_loop,
    _inject_pagination_state,
)
from sift_mcp.pagination.auto import _extract_json_content
from sift_mcp.pagination.extract import PaginationAssessment


def _page_value(
    start_id: int,
    count: int,
    *,
    next_cursor: str | None,
) -> dict[str, Any]:
    """Build a page-shaped JSON payload for cursor pagination tests."""
    paging: dict[str, Any] = {"cursors": {"after": next_cursor}}
    if next_cursor is not None:
        paging["next"] = f"https://example.test/page?after={next_cursor}"
    else:
        paging["next"] = None
    return {
        "data": [{"id": i} for i in range(start_id, start_id + count)],
        "paging": paging,
    }


def _mirrored_with_cursor_pagination() -> Any:
    cfg = UpstreamConfig(
        prefix="demo",
        transport="stdio",
        command="/usr/bin/printf",
        pagination=PaginationConfig(
            strategy="cursor",
            cursor_response_path="$.paging.cursors.after",
            cursor_param_name="after",
            has_more_response_path="$.paging.next",
        ),
    )
    return SimpleNamespace(
        prefix="demo",
        original_name="echo",
        upstream=SimpleNamespace(config=cfg),
    )


def _mirrored_without_pagination_config() -> Any:
    cfg = UpstreamConfig(
        prefix="demo",
        transport="stdio",
        command="/usr/bin/printf",
    )
    return SimpleNamespace(
        prefix="demo",
        original_name="echo",
        upstream=SimpleNamespace(config=cfg),
    )


def _initial_page(
    mirrored: Any,
    *,
    forwarded_args: dict[str, Any],
    page_value: dict[str, Any],
) -> tuple[Envelope, PaginationAssessment]:
    first_envelope = Envelope(
        upstream_instance_id="inst_demo",
        upstream_prefix="demo",
        tool="echo",
        status="ok",
        content=[JsonContentPart(value=page_value)],
    )
    envelope, assessment = _inject_pagination_state(
        first_envelope,
        mirrored.upstream.config,
        forwarded_args,
        mirrored.prefix,
        page_number=0,
    )
    assert assessment is not None
    return envelope, assessment


class _DummyCtx:
    """Minimal context needed by _auto_paginate_loop."""

    db_pool = None

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = list(pages)
        self.calls: list[dict[str, Any]] = []

    async def _call_upstream_with_metrics(
        self,
        *,
        mirrored: Any,
        forwarded_args: dict[str, Any],
    ) -> dict[str, Any]:
        self.calls.append(dict(forwarded_args))
        page = self._pages.pop(0)
        return {
            "content": [],
            "structuredContent": page,
            "isError": False,
            "meta": {},
        }

    def _envelope_from_upstream_result(
        self,
        *,
        mirrored: Any,
        upstream_result: dict[str, Any],
    ) -> tuple[Envelope, list[Any]]:
        return (
            Envelope(
                upstream_instance_id="inst_demo",
                upstream_prefix=mirrored.prefix,
                tool=mirrored.original_name,
                status="ok",
                content=[
                    JsonContentPart(value=upstream_result["structuredContent"])
                ],
            ),
            [],
        )


class _DummyNonJsonCtx(_DummyCtx):
    """Dummy context that returns text-only non-JSON follow-up pages."""

    def _envelope_from_upstream_result(
        self,
        *,
        mirrored: Any,
        upstream_result: dict[str, Any],
    ) -> tuple[Envelope, list[Any]]:
        return (
            Envelope(
                upstream_instance_id="inst_demo",
                upstream_prefix=mirrored.prefix,
                tool=mirrored.original_name,
                status="ok",
                content=[TextContentPart(text="no-json-followup")],
            ),
            [],
        )


def test_auto_paginate_max_records_stops_on_page_boundary() -> None:
    mirrored = _mirrored_with_cursor_pagination()
    forwarded_args = {"message": "hello", "limit": 300}
    first_envelope, first_assessment = _initial_page(
        mirrored,
        forwarded_args=forwarded_args,
        page_value=_page_value(1, 300, next_cursor="CUR2"),
    )
    ctx = _DummyCtx(
        [
            _page_value(301, 300, next_cursor="CUR3"),
            _page_value(601, 300, next_cursor="CUR4"),
            _page_value(901, 300, next_cursor="CUR5"),
            _page_value(1201, 300, next_cursor=None),
        ]
    )

    result = asyncio.run(
        _auto_paginate_loop(
            ctx,
            mirrored,
            first_envelope=first_envelope,
            first_assessment=first_assessment,
            forwarded_args=forwarded_args,
            max_pages=10,
            max_records=1000,
            timeout=30.0,
        )
    )

    merged = _extract_json_content(result.envelope)
    assert isinstance(merged, dict)
    assert isinstance(merged.get("data"), list)
    assert len(merged["data"]) == 1200
    assert result.pages_fetched == 4
    assert result.total_records == 1200
    assert result.stopped_reason == "max_records"
    assert len(ctx.calls) == 3
    assert [call.get("after") for call in ctx.calls] == [
        "CUR2",
        "CUR3",
        "CUR4",
    ]
    assert result.assessment.has_more is True
    assert result.assessment.state is not None
    assert result.assessment.state.next_params.get("after") == "CUR5"


def test_auto_paginate_does_not_trim_initial_page_when_over_cap() -> None:
    mirrored = _mirrored_with_cursor_pagination()
    forwarded_args = {"message": "hello", "limit": 5000}
    first_envelope, first_assessment = _initial_page(
        mirrored,
        forwarded_args=forwarded_args,
        page_value=_page_value(1, 5000, next_cursor="CUR2"),
    )
    ctx = _DummyCtx([_page_value(5001, 100, next_cursor=None)])

    result = asyncio.run(
        _auto_paginate_loop(
            ctx,
            mirrored,
            first_envelope=first_envelope,
            first_assessment=first_assessment,
            forwarded_args=forwarded_args,
            max_pages=10,
            max_records=1000,
            timeout=30.0,
        )
    )

    merged = _extract_json_content(result.envelope)
    assert isinstance(merged, dict)
    assert isinstance(merged.get("data"), list)
    assert len(merged["data"]) == 5000
    assert result.pages_fetched == 1
    assert result.total_records == 5000
    assert result.stopped_reason == "max_records"
    assert ctx.calls == []


def test_auto_paginate_discovery_terminal_signal_clears_state() -> None:
    mirrored = _mirrored_without_pagination_config()
    forwarded_args = {"page": 1, "limit": 2}
    first_envelope, first_assessment = _initial_page(
        mirrored,
        forwarded_args=forwarded_args,
        page_value={
            "has_more": True,
            "items": [{"id": 1}, {"id": 2}],
        },
    )
    assert first_assessment.state is not None
    assert first_assessment.state.next_params == {"page": 2}

    ctx = _DummyCtx(
        [
            {"has_more": False, "items": [{"id": 3}]},
        ]
    )

    result = asyncio.run(
        _auto_paginate_loop(
            ctx,
            mirrored,
            first_envelope=first_envelope,
            first_assessment=first_assessment,
            forwarded_args=forwarded_args,
            max_pages=10,
            max_records=1000,
            timeout=30.0,
        )
    )

    merged = _extract_json_content(result.envelope)
    assert isinstance(merged, dict)
    assert isinstance(merged.get("items"), list)
    assert len(merged["items"]) == 3
    assert ctx.calls == [{"page": 2, "limit": 2}]
    assert result.assessment.has_more is False
    assert result.assessment.state is None
    assert result.stopped_reason == "complete"
    assert "_gateway_pagination" not in result.envelope.meta


def test_auto_paginate_discovery_no_signal_followup_clears_state() -> None:
    mirrored = _mirrored_without_pagination_config()
    forwarded_args = {"page": 1, "limit": 2}
    first_envelope, first_assessment = _initial_page(
        mirrored,
        forwarded_args=forwarded_args,
        page_value={
            "has_more": True,
            "items": [{"id": 1}, {"id": 2}],
        },
    )
    assert first_assessment.state is not None
    assert first_assessment.state.next_params == {"page": 2}

    ctx = _DummyCtx(
        [
            {"items": [{"id": 3}]},
        ]
    )

    result = asyncio.run(
        _auto_paginate_loop(
            ctx,
            mirrored,
            first_envelope=first_envelope,
            first_assessment=first_assessment,
            forwarded_args=forwarded_args,
            max_pages=10,
            max_records=1000,
            timeout=30.0,
        )
    )

    merged = _extract_json_content(result.envelope)
    assert isinstance(merged, dict)
    assert isinstance(merged.get("items"), list)
    assert len(merged["items"]) == 3
    assert ctx.calls == [{"page": 2, "limit": 2}]
    assert result.assessment.has_more is False
    assert result.assessment.state is None
    assert result.stopped_reason == "complete"
    assert "_gateway_pagination" not in result.envelope.meta


def test_auto_paginate_discovery_non_json_followup_clears_state() -> None:
    mirrored = _mirrored_without_pagination_config()
    forwarded_args = {"page": 1, "limit": 2}
    first_envelope, first_assessment = _initial_page(
        mirrored,
        forwarded_args=forwarded_args,
        page_value={
            "has_more": True,
            "items": [{"id": 1}, {"id": 2}],
        },
    )
    assert first_assessment.state is not None
    assert first_assessment.state.next_params == {"page": 2}

    ctx = _DummyNonJsonCtx([{"ignored": True}])
    result = asyncio.run(
        _auto_paginate_loop(
            ctx,
            mirrored,
            first_envelope=first_envelope,
            first_assessment=first_assessment,
            forwarded_args=forwarded_args,
            max_pages=10,
            max_records=1000,
            timeout=30.0,
        )
    )

    merged = _extract_json_content(result.envelope)
    assert isinstance(merged, dict)
    assert isinstance(merged.get("items"), list)
    assert len(merged["items"]) == 2
    assert ctx.calls == [{"page": 2, "limit": 2}]
    assert result.assessment.has_more is False
    assert result.assessment.state is None
    assert result.assessment.retrieval_status == "PARTIAL"
    assert result.assessment.partial_reason == "SIGNAL_INCONCLUSIVE"
    assert result.stopped_reason == "binary_content"
    assert "_gateway_pagination" not in result.envelope.meta
