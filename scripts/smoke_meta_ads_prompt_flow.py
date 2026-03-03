#!/usr/bin/env python3
"""Smoke-test the Meta Ads prompt flow end-to-end through Sift.

This script validates the workflow:
1) List Meta ad accounts.
2) Fetch ads + insights for the last 7 days (with pagination continuation).
3) Rank "doing well" ads launched in the last 7 days.
4) Fetch creatives and attach image URLs when available.
5) Assert image URLs are not redacted by gateway secret redaction.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any

import asyncio
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

_REDACTED_MARKERS = ("[REDACTED_SECRET]", "[MASKED]", "[REDACTED]")

_ACCOUNT_ROWS_CODE = """
def run(data, schema, params):
    rows = []
    for page in data:
        if not isinstance(page, dict):
            continue
        result = page.get('result')
        if not isinstance(result, dict):
            continue
        for row in result.get('data', []):
            if not isinstance(row, dict):
                continue
            raw_id = row.get('id')
            acct_id = row.get('account_id')
            if isinstance(raw_id, str) and raw_id.startswith('act_'):
                edge_id = raw_id
            elif acct_id:
                edge_id = 'act_' + str(acct_id)
            else:
                continue
            rows.append({
                'edge_account_id': edge_id,
                'account_id': str(acct_id or raw_id),
                'name': row.get('name'),
                'currency': row.get('currency'),
                'status': row.get('account_status'),
            })
    return rows
"""

_RANK_ROWS_CODE = """
from datetime import datetime, timezone

def _flatten_pages(pages):
    rows = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        result = page.get('result')
        if not isinstance(result, dict):
            continue
        data = result.get('data')
        if isinstance(data, list):
            for row in data:
                if isinstance(row, dict):
                    rows.append(row)
    return rows

def _parse_float(value):
    try:
        if value in (None, ''):
            return 0.0
        return float(value)
    except Exception:
        return 0.0

def _parse_dt(raw):
    if not isinstance(raw, str) or not raw:
        return None
    txt = raw.strip()
    if txt.endswith('Z'):
        txt = txt[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(txt)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _actions_to_purchases(actions):
    if not isinstance(actions, list):
        return 0.0
    total = 0.0
    for row in actions:
        if not isinstance(row, dict):
            continue
        action_type = str(row.get('action_type') or '').lower()
        if 'purchase' not in action_type:
            continue
        total += _parse_float(row.get('value'))
    return total

def run(artifacts, schemas, params):
    ads_id = params['ads_id']
    ins_id = params['ins_id']
    since = params['since']
    top_n = int(params.get('top_n', 10))
    since_dt = _parse_dt(since + 'T00:00:00+00:00')

    ads_rows = _flatten_pages(artifacts.get(ads_id, []))
    ins_rows = _flatten_pages(artifacts.get(ins_id, []))

    ads_by_id = {}
    for row in ads_rows:
        ad_id = row.get('id') or row.get('ad_id')
        if not ad_id:
            continue
        ad_id = str(ad_id)
        created_raw = (
            row.get('created_time')
            or row.get('created_at')
            or row.get('start_time')
            or row.get('updated_time')
        )
        created_dt = _parse_dt(created_raw)
        ads_by_id[ad_id] = {
            'ad_id': ad_id,
            'ad_name': row.get('name') or row.get('ad_name') or ad_id,
            'campaign_name': row.get('campaign_name'),
            'adset_name': row.get('adset_name'),
            'launched_at': created_raw,
            'created_dt': created_dt,
        }

    joined = []
    for row in ins_rows:
        ad_id = row.get('ad_id') or row.get('id')
        if not ad_id:
            continue
        ad_id = str(ad_id)
        ad_meta = ads_by_id.get(ad_id)
        if ad_meta is None:
            continue
        created_dt = ad_meta.get('created_dt')
        if since_dt is not None:
            if created_dt is None or created_dt < since_dt:
                continue

        spend = _parse_float(row.get('spend'))
        impressions = _parse_float(row.get('impressions'))
        clicks = _parse_float(row.get('clicks'))
        ctr = _parse_float(row.get('ctr'))
        cpc = _parse_float(row.get('cpc'))
        purchases = _actions_to_purchases(row.get('actions'))
        roas = purchases / spend if spend > 0 else 0.0
        score = (roas * 0.5) + (ctr * 0.3) + (purchases * 0.2)

        joined.append({
            'ad_id': ad_id,
            'ad_name': ad_meta.get('ad_name'),
            'campaign_name': ad_meta.get('campaign_name'),
            'adset_name': ad_meta.get('adset_name'),
            'launched_at': ad_meta.get('launched_at'),
            'spend': round(spend, 2),
            'impressions': int(impressions),
            'clicks': int(clicks),
            'ctr': round(ctr, 4),
            'cpc': round(cpc, 4),
            'purchases': round(purchases, 2),
            'roas': round(roas, 4),
            'score': round(score, 4),
        })

    joined.sort(
        key=lambda row: (row['score'], row['purchases'], -row['cpc']),
        reverse=True,
    )
    return {
        'rows': joined[:top_n],
        'ads_total': len(ads_rows),
        'insights_total': len(ins_rows),
        'launched_last_7d_total': len(joined),
    }
"""

_EXTRACT_CREATIVE_CODE = """
import json

def _flatten_rows(data):
    rows = []
    for page in data:
        if not isinstance(page, dict):
            continue
        result = page.get('result')
        if isinstance(result, str):
            try:
                parsed = json.loads(result)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                result = parsed
        if not isinstance(result, dict):
            continue
        page_rows = result.get('data')
        if isinstance(page_rows, list):
            for row in page_rows:
                if isinstance(row, dict):
                    rows.append(row)
    return rows

def run(data, schema, params):
    ad_id = params.get('ad_id')
    best = {
        'ad_id': ad_id,
        'image_url': None,
        'thumbnail_url': None,
        'is_video': False,
    }
    for row in _flatten_rows(data):
        image_url = row.get('image_url') or row.get('url')
        thumbnail_url = row.get('thumbnail_url')
        object_story_spec = row.get('object_story_spec')
        if isinstance(object_story_spec, dict):
            link_data = object_story_spec.get('link_data')
            if isinstance(link_data, dict):
                image_url = (
                    image_url
                    or link_data.get('picture')
                    or link_data.get('image_url')
                )
            video_data = object_story_spec.get('video_data')
            if isinstance(video_data, dict):
                best['is_video'] = True
                thumbnail_url = (
                    thumbnail_url
                    or video_data.get('image_url')
                    or video_data.get('picture')
                )
        asset_feed_spec = row.get('asset_feed_spec')
        if isinstance(asset_feed_spec, dict):
            images = asset_feed_spec.get('images')
            if isinstance(images, list) and images:
                first = images[0]
                if isinstance(first, dict):
                    image_url = image_url or first.get('url')
        if image_url:
            best['image_url'] = image_url
        if thumbnail_url:
            best['thumbnail_url'] = thumbnail_url
        if best['image_url']:
            return best
    return best
"""


class FlowError(RuntimeError):
    """Raised when the integration flow fails validation."""


@dataclass(frozen=True)
class FlowSummary:
    """High-level summary of one script run."""

    account_id: str
    ads_pages: int
    insights_pages: int
    ads_total: int
    insights_total: int
    launched_last_7d_total: int
    rows_returned: int
    image_ads: int
    redacted_url_count: int


def _write_line(text: str, *, stream: Any | None = None) -> None:
    target = stream if stream is not None else sys.stdout
    target.write(text)
    target.write("\n")


def _contains_redaction(value: Any) -> bool:
    return isinstance(value, str) and any(
        marker in value for marker in _REDACTED_MARKERS
    )


def _parse_tool_result(result: Any) -> dict[str, Any]:
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, dict):
        return structured
    content = getattr(result, "content", None)
    if isinstance(content, list):
        for block in content:
            payload = (
                block.model_dump(by_alias=True, exclude_none=True)
                if hasattr(block, "model_dump")
                else block
            )
            if not isinstance(payload, dict):
                continue
            if payload.get("type") != "text":
                continue
            text = payload.get("text")
            if not isinstance(text, str):
                continue
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
    msg = "unable to parse MCP tool result payload"
    raise FlowError(msg)


async def _call_tool(
    client: Client,
    name: str,
    args: dict[str, Any],
) -> dict[str, Any]:
    payload = _parse_tool_result(await client.call_tool(name, args))
    if payload.get("type") == "gateway_error":
        msg = f"{name} failed: {json.dumps(payload, ensure_ascii=False)}"
        raise FlowError(msg)
    return payload


async def _code_query_single(
    client: Client,
    *,
    artifact_id: str,
    scope: str,
    code: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return await _call_tool(
        client,
        "artifact",
        {
            "action": "query",
            "query_kind": "code",
            "artifact_id": artifact_id,
            "root_path": "$",
            "scope": scope,
            "code": code,
            "params": params or {},
        },
    )


async def _code_query_multi(
    client: Client,
    *,
    artifact_ids: list[str],
    scope: str,
    code: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return await _call_tool(
        client,
        "artifact",
        {
            "action": "query",
            "query_kind": "code",
            "artifact_ids": artifact_ids,
            "root_paths": {aid: "$" for aid in artifact_ids},
            "scope": scope,
            "code": code,
            "params": params or {},
        },
    )


async def _continue_all_pages(
    client: Client,
    *,
    first_payload: dict[str, Any],
    max_pages: int,
) -> tuple[str, int]:
    current_id = str(first_payload.get("artifact_id", ""))
    pages = 1
    pagination = first_payload.get("pagination")
    has_more = bool(isinstance(pagination, dict) and pagination.get("has_more"))
    if not has_more:
        return current_id, pages
    for _ in range(max_pages):
        next_payload = await _call_tool(
            client,
            "artifact",
            {"action": "next_page", "artifact_id": current_id},
        )
        pages += 1
        current_id = str(next_payload.get("artifact_id", current_id))
        pagination = next_payload.get("pagination")
        has_more = bool(
            isinstance(pagination, dict) and pagination.get("has_more")
        )
        if not has_more:
            break
    return current_id, pages


def _candidate_account_indices(
    *,
    total_accounts: int,
    preferred_index: int,
    max_to_try: int,
) -> list[int]:
    if max_to_try <= 0:
        return []
    ordered: list[int] = []
    seen: set[int] = set()
    for idx in [preferred_index, *range(total_accounts)]:
        if idx in seen:
            continue
        seen.add(idx)
        ordered.append(idx)
        if len(ordered) >= max_to_try:
            break
    return ordered


async def _run_flow(args: argparse.Namespace) -> dict[str, Any]:
    transport = StdioTransport(
        command="uv",
        args=[
            "run",
            "sift-gateway",
            "--data-dir",
            str(args.data_dir),
        ],
    )
    async with Client(transport, timeout=float(args.timeout_seconds)) as client:
        accounts_payload = await _call_tool(
            client,
            "meta-ads_get_ad_accounts",
            {"limit": int(args.accounts_limit)},
        )
        accounts_artifact = str(accounts_payload["artifact_id"])
        accounts_rows_payload = await _code_query_single(
            client,
            artifact_id=accounts_artifact,
            scope="all_related",
            code=_ACCOUNT_ROWS_CODE,
        )
        accounts_rows = accounts_rows_payload.get("payload")
        if not isinstance(accounts_rows, list) or not accounts_rows:
            raise FlowError("no ad accounts returned from meta-ads_get_ad_accounts")
        account_index = int(args.account_index)
        if account_index < 0 or account_index >= len(accounts_rows):
            msg = (
                "account_index out of range; got "
                f"{account_index}, available={len(accounts_rows)}"
            )
            raise FlowError(msg)

        days = int(args.days)
        now = datetime.now(timezone.utc).date()
        since = (now - timedelta(days=days)).isoformat()
        until = now.isoformat()
        max_accounts_to_try = int(args.max_accounts_to_try)
        account_try_indices = _candidate_account_indices(
            total_accounts=len(accounts_rows),
            preferred_index=account_index,
            max_to_try=max_accounts_to_try,
        )
        if not account_try_indices:
            raise FlowError("max_accounts_to_try must be >= 1")

        account_edge_id = ""
        ads_artifact_first = ""
        ads_artifact_last = ""
        ads_pages = 0
        insights_artifact_first = ""
        insights_artifact_last = ""
        insights_pages = 0
        ranked_artifact = ""
        ranked_body: dict[str, Any] = {}
        ranked_rows: list[dict[str, Any]] = []
        attempts: list[dict[str, Any]] = []

        for idx in account_try_indices:
            candidate_account_id = str(accounts_rows[idx]["edge_account_id"])
            ads_payload = await _call_tool(
                client,
                "meta-ads_get_ads",
                {
                    "account_id": candidate_account_id,
                    "limit": int(args.ads_limit),
                },
            )
            candidate_ads_first = str(ads_payload["artifact_id"])
            candidate_ads_last, candidate_ads_pages = await _continue_all_pages(
                client,
                first_payload=ads_payload,
                max_pages=int(args.max_pages),
            )

            insights_payload = await _call_tool(
                client,
                "meta-ads_get_insights",
                {
                    "object_id": candidate_account_id,
                    "level": "ad",
                    "limit": int(args.insights_limit),
                    "time_range": {"since": since, "until": until},
                },
            )
            candidate_insights_first = str(insights_payload["artifact_id"])
            candidate_insights_last, candidate_insights_pages = (
                await _continue_all_pages(
                    client,
                    first_payload=insights_payload,
                    max_pages=int(args.max_pages),
                )
            )

            ranked_payload = await _code_query_multi(
                client,
                artifact_ids=[candidate_ads_last, candidate_insights_last],
                scope="all_related",
                code=_RANK_ROWS_CODE,
                params={
                    "ads_id": candidate_ads_last,
                    "ins_id": candidate_insights_last,
                    "since": since,
                    "top_n": int(args.top_n),
                },
            )
            candidate_ranked_artifact = str(ranked_payload["artifact_id"])
            candidate_ranked_body = ranked_payload.get("payload")
            if not isinstance(candidate_ranked_body, dict):
                raise FlowError("ranked payload missing dict body")
            candidate_ranked_rows = candidate_ranked_body.get("rows")
            if not isinstance(candidate_ranked_rows, list):
                raise FlowError("ranked payload missing rows list")

            attempts.append(
                {
                    "account_index": idx,
                    "account_id": candidate_account_id,
                    "rows_returned": len(candidate_ranked_rows),
                    "launched_last_7d_total": int(
                        candidate_ranked_body.get("launched_last_7d_total", 0)
                    ),
                }
            )
            if not candidate_ranked_rows:
                continue

            account_edge_id = candidate_account_id
            ads_artifact_first = candidate_ads_first
            ads_artifact_last = candidate_ads_last
            ads_pages = candidate_ads_pages
            insights_artifact_first = candidate_insights_first
            insights_artifact_last = candidate_insights_last
            insights_pages = candidate_insights_pages
            ranked_artifact = candidate_ranked_artifact
            ranked_body = candidate_ranked_body
            ranked_rows = candidate_ranked_rows
            break

        if not ranked_rows:
            msg = (
                "no launched-last-7-days rows found across attempted accounts: "
                f"{json.dumps(attempts, ensure_ascii=False)}"
            )
            raise FlowError(msg)

        creative_map: dict[str, dict[str, Any]] = {}
        creative_artifacts: list[str] = []
        for row in ranked_rows:
            ad_id = str(row.get("ad_id", "")).strip()
            if not ad_id:
                continue
            creative_payload = await _call_tool(
                client,
                "meta-ads_get_ad_creatives",
                {"ad_id": ad_id},
            )
            creative_artifact = str(creative_payload["artifact_id"])
            creative_artifacts.append(creative_artifact)
            creative_rows = await _code_query_single(
                client,
                artifact_id=creative_artifact,
                scope="all_related",
                code=_EXTRACT_CREATIVE_CODE,
                params={"ad_id": ad_id},
            )
            payload = creative_rows.get("payload")
            if isinstance(payload, dict):
                creative_map[ad_id] = payload

        final_rows: list[dict[str, Any]] = []
        image_ads = 0
        redacted_url_count = 0
        for row in ranked_rows:
            ad_id = str(row.get("ad_id", "")).strip()
            creative = creative_map.get(ad_id, {})
            image_url = (
                creative.get("image_url") if isinstance(creative, dict) else None
            )
            thumbnail_url = (
                creative.get("thumbnail_url")
                if isinstance(creative, dict)
                else None
            )
            is_video = bool(
                isinstance(creative, dict) and creative.get("is_video")
            )
            if (
                (not isinstance(image_url, str) or not image_url)
                and isinstance(thumbnail_url, str)
                and thumbnail_url
                and not is_video
            ):
                image_url = thumbnail_url
            if isinstance(image_url, str) and image_url:
                image_ads += 1
            if _contains_redaction(image_url) or _contains_redaction(
                thumbnail_url
            ):
                redacted_url_count += 1
            final_rows.append(
                {
                    **row,
                    "image_url": image_url,
                    "thumbnail_url": thumbnail_url,
                    "is_video": is_video,
                }
            )

        summary = FlowSummary(
            account_id=account_edge_id,
            ads_pages=ads_pages,
            insights_pages=insights_pages,
            ads_total=int(ranked_body.get("ads_total", 0)),
            insights_total=int(ranked_body.get("insights_total", 0)),
            launched_last_7d_total=int(
                ranked_body.get("launched_last_7d_total", 0)
            ),
            rows_returned=len(final_rows),
            image_ads=image_ads,
            redacted_url_count=redacted_url_count,
        )

        artifacts = {
            "accounts": accounts_artifact,
            "ads_first": ads_artifact_first,
            "ads_last": ads_artifact_last,
            "insights_first": insights_artifact_first,
            "insights_last": insights_artifact_last,
            "ranked": ranked_artifact,
            "creatives": creative_artifacts,
        }
        return {
            "prompt": args.prompt,
            "artifacts": artifacts,
            "summary": summary.__dict__,
            "rows": final_rows,
        }


def _render_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Rank | Ad ID | Launched At | Spend | Purchases | ROAS | Image URL |",
        "|---:|---|---|---:|---:|---:|---|",
    ]
    for idx, row in enumerate(rows, start=1):
        image_url = row.get("image_url")
        if not isinstance(image_url, str) or not image_url:
            image_url = "-"
        lines.append(
            f"| {idx} | {row.get('ad_id')} | {row.get('launched_at')} | "
            f"{row.get('spend')} | {row.get('purchases')} | "
            f"{row.get('roas')} | {image_url} |"
        )
    return lines


def _assert_expectations(args: argparse.Namespace, report: dict[str, Any]) -> None:
    summary = report["summary"]
    if summary["rows_returned"] < int(args.require_rows):
        msg = (
            "rows_returned below required threshold: "
            f"{summary['rows_returned']} < {int(args.require_rows)}"
        )
        raise FlowError(msg)
    if summary["image_ads"] < int(args.require_image_ads):
        msg = (
            "image_ads below required threshold: "
            f"{summary['image_ads']} < {int(args.require_image_ads)}"
        )
        raise FlowError(msg)
    if bool(args.fail_on_redaction) and summary["redacted_url_count"] > 0:
        msg = (
            "image URL redaction regression detected: "
            f"redacted_url_count={summary['redacted_url_count']}"
        )
        raise FlowError(msg)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke-test the Meta Ads prompt flow through Sift.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path.home() / ".sift-gateway",
        help="Gateway data dir with configured/authenticated meta-ads upstream.",
    )
    parser.add_argument("--account-index", type=int, default=0)
    parser.add_argument(
        "--max-accounts-to-try",
        type=int,
        default=5,
        help=(
            "Number of accounts to probe (starting with --account-index) "
            "before failing if no launched-last-7-days rows are found."
        ),
    )
    parser.add_argument("--accounts-limit", type=int, default=200)
    parser.add_argument("--ads-limit", type=int, default=500)
    parser.add_argument("--insights-limit", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--require-rows", type=int, default=1)
    parser.add_argument("--require-image-ads", type=int, default=1)
    parser.add_argument(
        "--fail-on-redaction",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--prompt",
        default=(
            "what are my meta ads that are doing well launched last 7 days. "
            "output a table with results and their image if image ads"
        ),
        help="Prompt text kept for traceability in script output.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        report = asyncio.run(_run_flow(args))
        _assert_expectations(args, report)
    except FlowError as exc:
        _write_line(f"flow failed: {exc}", stream=sys.stderr)
        return 1

    if args.json:
        _write_line(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
        )
        return 0

    _write_line("Prompt:")
    _write_line(f"  {report['prompt']}")
    _write_line("")
    _write_line("Artifacts:")
    for key, value in report["artifacts"].items():
        if key == "creatives":
            _write_line(f"  {key}: {json.dumps(value)}")
        else:
            _write_line(f"  {key}: {value}")
    _write_line("")
    _write_line("Summary:")
    for key, value in report["summary"].items():
        _write_line(f"  {key}: {value}")
    _write_line("")
    _write_line("Results:")
    for line in _render_table(report["rows"]):
        _write_line(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
