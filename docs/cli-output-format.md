# CLI Output Format

Stable output contract for `sift-gateway` CLI.

## Goals

- Keep default output compact for agent context budgets.
- Keep `--json` output deterministic and machine-readable.
- Keep human-mode summaries stable unless explicitly versioned.

## Supported CLI Commands

- `sift-gateway run`
- `sift-gateway code`

Other historical artifact CLI commands are not part of this contract.

## Default Human Output

### `sift-gateway run`

```text
artifact: <artifact_id>
mode:     <full|schema_ref>
records:  <n|unknown>
bytes:    <payload_total_bytes>
capture:  <capture_kind>
expires:  <iso_timestamp>
tags:     <tag1>, <tag2>, ...
exit:     <command_exit_code>
next:     sift-gateway run --continue-from <artifact_id> -- <next-command>
schema_roots: <n>
hint:     use `sift-gateway code <artifact_id> '$' --expr "len(df)"`
```

Only present fields are emitted.

### `sift-gateway code`

Summary header plus formatted JSON payload:

```text
artifact: <artifact_id>
mode:     <full|schema_ref>
records:  <stats.output_records>
bytes:    <stats.bytes_out>
{ ...pretty JSON... }
```

Only present fields are emitted.

## JSON Mode (`--json`)

- Emits one JSON object to stdout.
- Output JSON is minified and deterministic.
- `sift-gateway run` exits with the wrapped command exit code.

Shared response keys:

- `response_mode`
- `artifact_id`
- optional `lineage`
- optional `pagination`
- optional `metadata`

### `run` pagination shape

When upstream pagination is discovered, `run` may include:

```json
{
  "pagination": {
    "layer": "upstream",
    "retrieval_status": "PARTIAL",
    "partial_reason": "MORE_PAGES_AVAILABLE",
    "has_more": true,
    "has_next_page": true,
    "next_params": {"after": "CUR_2"},
    "next_action": {
      "command": "run",
      "continue_from_artifact_id": "art_123",
      "command_line": "sift-gateway run --continue-from art_123 -- <next-command>"
    }
  }
}
```

CLI continuation is manual: apply `next_params` to the next command and run it
with `--continue-from <artifact_id>`.

## Compatibility Notes

- Additive JSON fields are allowed.
- If human-mode line structure changes, update:
  - `tests/unit/test_cli_main.py`
  - this document
