# CLI Output Format

Stable output contract for the `sift-gateway` CLI.

## Goals

- Keep default output compact for agent context budgets.
- Keep `--json` output machine-readable and deterministic.
- Avoid breaking human-mode line formats without explicit version notes.

## Default Human Output

### `sift-gateway list`

One line per artifact:

```text
<artifact_id> seq=<n> kind=<kind> status=<ok|error> source=<source_tool> capture=<capture_kind> bytes=<payload_total_bytes>
```

Optional pagination line:

```text
next_cursor: <cursor>
```

### `sift-gateway schema <artifact_id>`

```text
artifact: <artifact_id>
scope: <all_related|single>
artifacts: <count>
roots: <count>
- <root_path> count=<count_estimate>
next: <continuation_command>
hint: <continuation_hint>
```

`next` and `hint` are emitted only when upstream pagination is available.

### `sift-gateway get`, `sift-gateway query`, and `sift-gateway code`

Summary header plus formatted JSON payload:

```text
items: <n>
count: <n>
next_cursor: <cursor>
{ ...pretty JSON... }
```

Only present fields are emitted.

### `sift-gateway run`

```text
artifact: <artifact_id>
records:  <estimated_record_count|unknown>
bytes:    <payload_total_bytes>
capture:  <capture_kind>
expires:  <iso_timestamp>
tags:     <tag1>, <tag2>, ...
exit:     <command_exit_code>
next:     sift-gateway run --continue-from <artifact_id> -- <next-command>
hint:     use `sift-gateway query <artifact_id> '$'` to explore
```

Only present fields are emitted.

### `sift-gateway diff`

```text
left:    <artifact_id>
right:   <artifact_id>
equal:   <true|false>
hashes:  <left_hash> / <right_hash>
bytes:   <left_bytes> / <right_bytes>
```

If not equal, bounded unified diff lines are appended.

## JSON Mode (`--json`)

- Always emits one JSON object to stdout.
- Keys are sorted (`sort_keys=True`) for deterministic diffs.
- `sift-gateway run` returns command exit code as process exit code even in JSON mode.

When upstream pagination is discovered, `run` and `schema` can include:

```json
{
  "pagination": {
    "has_next_page": true,
    "page_number": 0,
    "next_params": {"after": "CUR_2"},
    "next_action": {
      "command": "run",
      "continue_from_artifact_id": "art_123",
      "command_line": "sift-gateway run --continue-from art_123 -- <next-command>"
    },
    "hint": "More results are available. Continue with \"sift-gateway run --continue-from art_123 -- <next-command>\" and use \"pagination.next_params\" as continuation values."
  }
}
```

CLI continuation is manual: apply `next_params` to the next command and run it
with `--continue-from <artifact_id>`.

## Compatibility Notes

- Additive fields in JSON mode are allowed.
- Human-mode line structure should remain stable; if changed, update:
  - `tests/unit/test_cli_main.py` snapshots
  - this document
