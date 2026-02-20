#!/usr/bin/env python3
"""Compare terminal context load across OpenClaw no-sift and Sift flows.

Flows:
1) OpenClaw no-sift:
   - `curl -s URL`
   - `curl -s URL | jq -r '.[] | select(.email | test(needle; "i")) | .body'`
2) Sift codegen:
   - `sift-gateway run --json -- curl -s URL`
   - `sift-gateway code --json ...`

Optional token counting uses OpenAI /v1/responses/input_tokens.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import subprocess
import sys
from typing import Any
import urllib.error
import urllib.request

DEFAULT_URL = "https://jsonplaceholder.typicode.com/comments?_page=1&_limit=200"
TOKEN_COUNT_ENDPOINT = "https://api.openai.com/v1/responses/input_tokens"


@dataclass(slots=True)
class OpenClawNoSiftFlowResult:
    """Artifacts from OpenClaw no-sift flow."""

    fetch_stdout: bytes
    filter_stdout: bytes
    schema_keys: list[str]
    bodies: list[str]


@dataclass(slots=True)
class SiftFlowResult:
    """Artifacts from sift codegen flow."""

    artifact_id: str
    derived_artifact_id: str | None
    code_response_mode: str | None
    run_stdout: bytes
    code_stdout: bytes
    capture_payload_total_bytes: int
    match_count: int
    bodies: list[str]


def _run_command(argv: list[str], *, stdin_bytes: bytes | None = None) -> bytes:
    """Run command and return stdout bytes."""
    completed = subprocess.run(
        argv,
        check=False,
        capture_output=True,
        input=stdin_bytes,
    )
    if completed.returncode != 0:
        stderr_text = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(argv)}\n{stderr_text}"
        )
    return completed.stdout


def _parse_last_json_object(stdout_bytes: bytes) -> dict[str, Any]:
    """Parse last JSON object line, tolerating mixed stdout logs."""
    lines = [line for line in stdout_bytes.splitlines() if line.strip()]
    if not lines:
        raise ValueError("expected JSON output but got no stdout")

    parsed_objects: list[dict[str, Any]] = []
    for line in lines:
        try:
            candidate = json.loads(line.decode("utf-8", errors="strict"))
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            parsed_objects.append(candidate)

    if not parsed_objects:
        raise ValueError("no JSON object line found in stdout")
    return parsed_objects[-1]


def _extract_bodies_from_rows(rows: Any, needle_lower: str) -> list[str]:
    """Extract body strings for rows where email contains needle."""
    if not isinstance(rows, list):
        raise ValueError("expected list rows")

    bodies: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        email = row.get("email")
        body = row.get("body")
        if (
            isinstance(email, str)
            and isinstance(body, str)
            and needle_lower in email.lower()
        ):
            bodies.append(body)
    return bodies


def _extract_body_from_codegen_item(item: Any) -> str | None:
    """Extract body value from codegen output row shape."""
    if not isinstance(item, dict):
        return None
    value = item.get("body")
    if isinstance(value, str):
        return value

    projection = item.get("projection")
    if isinstance(projection, dict):
        projected = projection.get("$.body", projection.get("body"))
        if isinstance(projected, str):
            return projected
    return None


def _extract_codegen_items(payload: dict[str, Any]) -> list[Any] | None:
    """Return code result items from full-mode response variants."""
    items = payload.get("items")
    if isinstance(items, list):
        return items
    raw_payload = payload.get("payload")
    if isinstance(raw_payload, list):
        return raw_payload
    if isinstance(raw_payload, dict):
        nested_items = raw_payload.get("items")
        if isinstance(nested_items, list):
            return nested_items
    return None


def _extract_codegen_bodies(payload: dict[str, Any]) -> list[str]:
    """Extract body strings from code payload rows."""
    items = _extract_codegen_items(payload)
    bodies: list[str] = []
    if not isinstance(items, list):
        return bodies
    for item in items:
        body = _extract_body_from_codegen_item(item)
        if isinstance(body, str):
            bodies.append(body)
    return bodies


def _extract_codegen_match_count(
    payload: dict[str, Any],
    *,
    fallback_bodies: list[str],
) -> int:
    """Extract output row count from code payload metadata."""
    total_matched = payload.get("total_matched")
    if isinstance(total_matched, int):
        return total_matched
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        stats = metadata.get("stats")
        if isinstance(stats, dict):
            output_records = stats.get("output_records")
            if isinstance(output_records, int):
                return output_records
    return len(fallback_bodies)


def _rehydrate_schema_ref_bodies(
    *,
    sift_bin: str,
    artifact_id: str,
) -> tuple[list[str], bytes, int]:
    """Re-run a narrow code projection when initial code response is schema_ref."""
    hydrate_stdout = _run_command(
        [
            sift_bin,
            "code",
            artifact_id,
            "$",
            "--scope",
            "single",
            "--code",
            (
                "def run(data, schema, params): "
                "return [{'body': row.get('body')} for row in data if isinstance(row, dict)]"
            ),
            "--json",
        ]
    )
    hydrate_payload = _parse_last_json_object(hydrate_stdout)
    bodies = _extract_codegen_bodies(hydrate_payload)
    match_count = _extract_codegen_match_count(
        hydrate_payload,
        fallback_bodies=bodies,
    )
    return bodies, hydrate_stdout, match_count


def _parse_comments_payload(
    raw_payload: bytes,
    *,
    needle_lower: str,
) -> tuple[list[str], list[str]]:
    """Parse endpoint JSON and return (schema_keys, bodies)."""
    payload = json.loads(raw_payload.decode("utf-8", errors="strict"))
    if not isinstance(payload, list):
        raise ValueError("expected top-level JSON list from endpoint")

    schema_keys: list[str] = []
    if payload and isinstance(payload[0], dict):
        schema_keys = sorted([key for key in payload[0] if isinstance(key, str)])

    bodies = _extract_bodies_from_rows(payload, needle_lower)
    return schema_keys, bodies


def _openclaw_no_sift_flow(
    *,
    url: str,
    needle: str,
    needle_lower: str,
) -> OpenClawNoSiftFlowResult:
    """OpenClaw no-sift flow matching CLI behavior exactly."""
    fetch_stdout = _run_command(["curl", "-s", url])
    schema_keys, bodies = _parse_comments_payload(
        fetch_stdout,
        needle_lower=needle_lower,
    )

    filter_expr = (
        ".[] | select(.email | test(" + json.dumps(needle) + '; "i")) | .body'
    )
    filter_input = _run_command(["curl", "-s", url])
    filter_stdout = _run_command(
        ["jq", "-r", filter_expr],
        stdin_bytes=filter_input,
    )

    return OpenClawNoSiftFlowResult(
        fetch_stdout=fetch_stdout,
        filter_stdout=filter_stdout,
        schema_keys=schema_keys,
        bodies=bodies,
    )


def _sift_codegen_flow(*, sift_bin: str, url: str, needle: str) -> SiftFlowResult:
    """Capture + codegen flow via sift-gateway."""
    run_stdout = _run_command(
        [
            sift_bin,
            "run",
            "--json",
            "--",
            "curl",
            "-s",
            url,
        ]
    )
    run_payload = _parse_last_json_object(run_stdout)
    artifact_id = run_payload.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError("sift run response missing artifact_id")

    capture_payload_total_bytes = int(run_payload.get("payload_total_bytes", 0))

    code_source = (
        "def run(data, schema, params):\n"
        f"    needle = {json.dumps(needle.lower())}\n"
        "    return [\n"
        "        {'body': row.get('body')}\n"
        "        for row in data\n"
        "        if isinstance(row, dict)\n"
        "        and needle in str(row.get('email', '')).lower()\n"
        "    ]"
    )
    code_stdout = _run_command(
        [
            sift_bin,
            "code",
            artifact_id,
            "$",
            "--scope",
            "single",
            "--code",
            code_source,
            "--json",
        ]
    )
    code_payload = _parse_last_json_object(code_stdout)
    code_response_mode_raw = code_payload.get("response_mode")
    code_response_mode = (
        code_response_mode_raw
        if isinstance(code_response_mode_raw, str)
        else None
    )
    derived_artifact_id_raw = code_payload.get("artifact_id")
    if not isinstance(derived_artifact_id_raw, str):
        # Backward compatibility with pre-contract-v1 shape.
        derived_artifact_id_raw = code_payload.get("derived_artifact_id")
    derived_artifact_id = (
        derived_artifact_id_raw if isinstance(derived_artifact_id_raw, str) else None
    )
    bodies = _extract_codegen_bodies(code_payload)
    match_count = _extract_codegen_match_count(
        code_payload,
        fallback_bodies=bodies,
    )
    combined_code_stdout = code_stdout
    if (
        code_response_mode == "schema_ref"
        and not bodies
        and isinstance(derived_artifact_id, str)
        and derived_artifact_id
    ):
        rehydrated_bodies, hydrate_stdout, rehydrated_match_count = (
            _rehydrate_schema_ref_bodies(
                sift_bin=sift_bin,
                artifact_id=derived_artifact_id,
            )
        )
        if rehydrated_bodies:
            bodies = rehydrated_bodies
        match_count = rehydrated_match_count
        combined_code_stdout += b"\n" + hydrate_stdout

    return SiftFlowResult(
        artifact_id=artifact_id,
        derived_artifact_id=derived_artifact_id,
        code_response_mode=code_response_mode,
        run_stdout=run_stdout,
        code_stdout=combined_code_stdout,
        capture_payload_total_bytes=capture_payload_total_bytes,
        match_count=match_count,
        bodies=bodies,
    )


def _render_cli_context(command: str, stdout_bytes: bytes) -> str:
    """Render command + stdout as terminal text."""
    stdout_text = stdout_bytes.decode("utf-8", errors="replace")
    return f"$ {command}\n{stdout_text}"


def _count_input_tokens(*, api_key: str, model: str, input_text: str) -> int:
    """Call /v1/responses/input_tokens and return input token count."""
    payload = {"model": model, "input": input_text}
    request = urllib.request.Request(
        TOKEN_COUNT_ENDPOINT,
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            response_bytes = response.read()
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"token count HTTP error ({exc.code}): {error_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"token count request failed: {exc}") from exc

    data = json.loads(response_bytes.decode("utf-8", errors="strict"))
    input_tokens = data.get("input_tokens")
    if not isinstance(input_tokens, int):
        raise ValueError("token count response missing integer input_tokens")
    return input_tokens


def _reduction_metrics(*, baseline: int, candidate: int) -> dict[str, Any]:
    """Compute absolute and percent reduction from baseline to candidate."""
    reduction = baseline - candidate
    percent = (reduction / baseline * 100.0) if baseline else 0.0
    return {
        "reduction": reduction,
        "reduction_percent": round(percent, 2),
    }


def _compute_token_metrics(
    *,
    token_count_mode: str,
    token_model: str,
    url: str,
    needle: str,
    sift_bin: str,
    openclaw_no_sift: OpenClawNoSiftFlowResult,
    sift: SiftFlowResult,
) -> dict[str, Any] | None:
    """Compute token metrics, or return None if disabled/unavailable."""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if token_count_mode == "off":
        return None
    if not api_key:
        if token_count_mode == "on":
            raise RuntimeError("OPENAI_API_KEY is required when --token-count=on")
        return None

    openclaw_fetch_command = f"curl -s '{url}'"
    openclaw_filter_command = (
        f"curl -s '{url}' | jq -r "
        f"'.[] | select(.email | test({json.dumps(needle)}; \"i\")) | .body'"
    )
    sift_run_command = f"{sift_bin} run --json -- curl -s '{url}'"
    sift_code_command = (
        f"{sift_bin} code {sift.artifact_id} '$' --scope single "
        "--code \"def run(data, schema, params): return len(data)\" --json"
    )

    openclaw_fetch_tokens = _count_input_tokens(
        api_key=api_key,
        model=token_model,
        input_text=_render_cli_context(
            openclaw_fetch_command,
            openclaw_no_sift.fetch_stdout,
        ),
    )
    openclaw_filter_tokens = _count_input_tokens(
        api_key=api_key,
        model=token_model,
        input_text=_render_cli_context(
            openclaw_filter_command,
            openclaw_no_sift.filter_stdout,
        ),
    )
    openclaw_total_tokens = openclaw_fetch_tokens + openclaw_filter_tokens

    sift_run_tokens = _count_input_tokens(
        api_key=api_key,
        model=token_model,
        input_text=_render_cli_context(sift_run_command, sift.run_stdout),
    )
    sift_code_tokens = _count_input_tokens(
        api_key=api_key,
        model=token_model,
        input_text=_render_cli_context(sift_code_command, sift.code_stdout),
    )
    sift_total_tokens = sift_run_tokens + sift_code_tokens

    return {
        "model": token_model,
        "openclaw_no_sift_input_tokens": {
            "fetch": openclaw_fetch_tokens,
            "filter": openclaw_filter_tokens,
            "total": openclaw_total_tokens,
        },
        "with_sift_input_tokens": {
            "run": sift_run_tokens,
            "code": sift_code_tokens,
            "total": sift_total_tokens,
        },
        "reduction_vs_sift": _reduction_metrics(
            baseline=openclaw_total_tokens,
            candidate=sift_total_tokens,
        ),
        "note": "Computed via POST /v1/responses/input_tokens",
    }


def _build_report(
    *,
    url: str,
    needle: str,
    openclaw_no_sift: OpenClawNoSiftFlowResult,
    sift: SiftFlowResult,
    token_metrics: dict[str, Any] | None,
    outputs_equal: bool,
) -> dict[str, Any]:
    """Build machine-readable report."""
    openclaw_fetch_bytes = len(openclaw_no_sift.fetch_stdout)
    openclaw_filter_bytes = len(openclaw_no_sift.filter_stdout)
    openclaw_total_bytes = openclaw_fetch_bytes + openclaw_filter_bytes
    sift_run_bytes = len(sift.run_stdout)
    sift_code_bytes = len(sift.code_stdout)
    sift_total_bytes = sift_run_bytes + sift_code_bytes

    report: dict[str, Any] = {
        "url": url,
        "needle": needle,
        "without_sift_openclaw": {
            "cli_stdout_bytes": {
                "fetch": openclaw_fetch_bytes,
                "filter": openclaw_filter_bytes,
                "total": openclaw_total_bytes,
            },
            "schema_keys_discovered": openclaw_no_sift.schema_keys,
            "match_count": len(openclaw_no_sift.bodies),
            "bodies": openclaw_no_sift.bodies,
        },
        "with_sift_codegen": {
            "artifact_id": sift.artifact_id,
            "derived_artifact_id": sift.derived_artifact_id,
            "response_mode": sift.code_response_mode,
            "cli_stdout_bytes": {
                "run": sift_run_bytes,
                "code": sift_code_bytes,
                "total": sift_total_bytes,
            },
            "capture_payload_total_bytes": sift.capture_payload_total_bytes,
            "match_count": sift.match_count,
            "bodies": sift.bodies,
        },
        "comparison_bytes": {
            "openclaw_no_sift_vs_sift": _reduction_metrics(
                baseline=openclaw_total_bytes,
                candidate=sift_total_bytes,
            ),
        },
        "openai_input_tokens": token_metrics,
    }
    byte_reduction = report["comparison_bytes"]["openclaw_no_sift_vs_sift"]
    byte_direction = "reduced" if byte_reduction["reduction"] >= 0 else "increased"
    byte_percent = abs(byte_reduction["reduction_percent"])
    equivalence_text = (
        "with identical output." if outputs_equal else "but outputs did not match."
    )
    if token_metrics is None:
        release_note_summary = (
            f"For the {needle} query flow, Sift {byte_direction} CLI context bytes from "
            f"{openclaw_total_bytes} to {sift_total_bytes} "
            f"({byte_percent:.2f}%), {equivalence_text}"
        )
    else:
        token_openclaw = token_metrics["openclaw_no_sift_input_tokens"]["total"]
        token_sift = token_metrics["with_sift_input_tokens"]["total"]
        token_reduction_raw = token_metrics["reduction_vs_sift"]["reduction_percent"]
        token_direction = "reduced" if token_reduction_raw >= 0 else "increased"
        token_percent = abs(token_reduction_raw)
        release_note_summary = (
            f"For the {needle} query flow, Sift {token_direction} model input from "
            f"{token_openclaw} to {token_sift} tokens ({token_percent:.2f}%) and "
            f"{byte_direction} CLI context bytes from {openclaw_total_bytes} to "
            f"{sift_total_bytes} ({byte_percent:.2f}%), {equivalence_text}"
        )
    report["release_note_summary"] = release_note_summary
    return report


def _print_human_report(report: dict[str, Any]) -> None:
    """Render compact human summary."""
    openclaw_no_sift = report["without_sift_openclaw"]
    sift = report["with_sift_codegen"]
    comp = report["comparison_bytes"]

    print(f"URL: {report['url']}")
    print(f"needle: {report['needle']!r} (case-insensitive)")
    print("release_note_summary:")
    print(f"  {report['release_note_summary']}")
    print("")
    print("without_sift_openclaw:")
    print(
        "  cli_stdout_bytes: "
        f"{openclaw_no_sift['cli_stdout_bytes']['total']} "
        f"(fetch={openclaw_no_sift['cli_stdout_bytes']['fetch']}, "
        f"filter={openclaw_no_sift['cli_stdout_bytes']['filter']})"
    )
    print(f"  match_count: {openclaw_no_sift['match_count']}")
    print("")
    print("with_sift_codegen:")
    print(f"  artifact_id: {sift['artifact_id']}")
    print(f"  derived_artifact_id: {sift['derived_artifact_id']}")
    print(f"  response_mode: {sift.get('response_mode')}")
    print(
        "  cli_stdout_bytes: "
        f"{sift['cli_stdout_bytes']['total']} "
        f"(run={sift['cli_stdout_bytes']['run']}, "
        f"code={sift['cli_stdout_bytes']['code']})"
    )
    print(f"  capture_payload_total_bytes: {sift['capture_payload_total_bytes']}")
    print(f"  match_count: {sift['match_count']}")
    print("")
    print("comparison_bytes:")
    print(
        "  openclaw_no_sift_vs_sift: "
        f"{comp['openclaw_no_sift_vs_sift']['reduction']} "
        f"({comp['openclaw_no_sift_vs_sift']['reduction_percent']:.2f}%)"
    )
    print("")

    token_metrics = report.get("openai_input_tokens")
    if token_metrics is None:
        print("openai_input_tokens: skipped (set OPENAI_API_KEY or --token-count=on)")
        print("")
    else:
        openclaw_tokens = token_metrics["openclaw_no_sift_input_tokens"]
        sift_tokens = token_metrics["with_sift_input_tokens"]
        reduction_vs_sift = token_metrics["reduction_vs_sift"]
        print(f"openai_input_tokens ({token_metrics['model']}):")
        print(
            "  openclaw_no_sift_input_tokens: "
            f"{openclaw_tokens['total']} "
            f"(fetch={openclaw_tokens['fetch']}, "
            f"filter={openclaw_tokens['filter']})"
        )
        print(
            "  with_sift_input_tokens: "
            f"{sift_tokens['total']} "
            f"(run={sift_tokens['run']}, code={sift_tokens['code']})"
        )
        print(
            "  openclaw_no_sift_vs_sift: "
            f"{reduction_vs_sift['reduction']} "
            f"({reduction_vs_sift['reduction_percent']:.2f}%)"
        )
        print("")

    print("bodies:")
    for idx, body in enumerate(sift["bodies"], start=1):
        print(f"  {idx}. {body}")


def main() -> int:
    """Run CLI flow comparison and emit report output."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare terminal context load for OpenClaw no-sift and sift codegen "
            "flows."
        )
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--needle", default="joana")
    parser.add_argument("--sift-bin", default="sift-gateway")
    parser.add_argument(
        "--token-count",
        choices=("auto", "on", "off"),
        default="auto",
        help="Count OpenAI input tokens via /v1/responses/input_tokens",
    )
    parser.add_argument(
        "--token-model",
        default="gpt-5",
        help="Model passed to /v1/responses/input_tokens",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON report",
    )
    args = parser.parse_args()

    needle_lower = args.needle.lower()

    openclaw_no_sift = _openclaw_no_sift_flow(
        url=args.url,
        needle=args.needle,
        needle_lower=needle_lower,
    )
    sift = _sift_codegen_flow(
        sift_bin=args.sift_bin,
        url=args.url,
        needle=args.needle,
    )

    token_metrics = _compute_token_metrics(
        token_count_mode=args.token_count,
        token_model=args.token_model,
        url=args.url,
        needle=args.needle,
        sift_bin=args.sift_bin,
        openclaw_no_sift=openclaw_no_sift,
        sift=sift,
    )
    if sift.bodies:
        outputs_equal = openclaw_no_sift.bodies == sift.bodies
    else:
        outputs_equal = len(openclaw_no_sift.bodies) == sift.match_count
    report = _build_report(
        url=args.url,
        needle=args.needle,
        openclaw_no_sift=openclaw_no_sift,
        sift=sift,
        token_metrics=token_metrics,
        outputs_equal=outputs_equal,
    )

    if not outputs_equal:
        report["warning"] = "flow body results differ"
        if args.json:
            print(json.dumps(report, sort_keys=True))
        else:
            _print_human_report(report)
            print("warning: flow body results differ", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        _print_human_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
