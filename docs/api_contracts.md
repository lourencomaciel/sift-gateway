# API Contracts

Canonical contract for Sift's public runtime surface.

## Scope

Public workflows are intentionally narrow:

1. Capture data:
   - MCP: mirrored upstream tool calls
   - CLI: `sift-gateway run -- <command>`
2. Continue upstream pagination:
   - MCP: `artifact(action="next_page", artifact_id=...)`
   - CLI: `sift-gateway run --continue-from <artifact_id> -- <next-command>`
3. Analyze artifacts:
   - MCP: `artifact(action="query", query_kind="code", ...)`
   - CLI: `sift-gateway code ...`
4. Materialize binary refs as local files:
   - MCP: `artifact(action="blob_list", ...)`
   - MCP: `artifact(action="blob_materialize", ...)`
   - MCP: `artifact(action="blob_cleanup", ...)`
   - MCP: `artifact(action="blob_manifest", ...)`

Legacy retrieval query kinds are not part of this contract.

## Required Gateway Context

Mirrored tool calls and `artifact(...)` calls must include
`_gateway_context.session_id`.

If missing, the gateway returns `INVALID_ARGUMENT`.

## Processing Pipeline

Mirrored tool calls, pagination continuations, and code outputs follow:

1. Execute tool/command/code.
2. Parse payload.
3. Detect pagination from raw parsed payload.
4. Redact sensitive output values.
5. Persist artifact.
6. Build mapping + schema-ref fallback data.
7. Choose response mode.
8. Return artifact-centric response.

Pagination detection happens before redaction. Persisted payloads are redacted.

## `artifact(action="query", query_kind="code")`

### Required arguments

- `_gateway_context.session_id`
- `action="query"`
- `query_kind="code"`
- `code`
- one target shape:
  - single target: `artifact_id` + `root_path`
  - multi target: `artifact_ids` + (`root_path` shared or `root_paths` exact map)

### Optional arguments

- `scope`: `single` (default, anchor artifact(s) only) or `all_related`
  (pagination-chain related artifacts)
- `params`: JSON object passed to `run(..., ..., params)`

### Runtime entrypoints

- single artifact: `run(data, schema, params)`
- multi artifact: `run(artifacts, schemas, params)`

Runtime shape notes:

- single: `data` is `list[dict]`
- multi: `artifacts` is `dict[artifact_id -> list[dict]]`
- prefer `scope=single` unless cross-artifact logic is required
- prefer compact outputs (aggregates or top-N) to reduce `schema_ref` responses

## Response shape (`query_kind="code"`)

Code-query responses are artifact-centric and include:

- `response_mode`
- `artifact_id` (derived artifact)
- `lineage`
- `metadata` (`stats` + determinism metadata)

Compatibility fields may be present:

- `items`
- `total_matched`
- `truncated`
- `scope`
- `stats`
- `determinism`
- `warnings`
- `sampled_only`

Code query responses do not expose a query-cursor loop.

## `artifact(action="next_page")`

### Required arguments

- `_gateway_context.session_id`
- `action="next_page"`
- `artifact_id`

### Behavior

- loads upstream pagination state from the referenced artifact
- replays the mirrored upstream tool with continuation params
- persists a new artifact linked with `parent_artifact_id` and `chain_seq`
- returns the same mirrored response contract (`full` or `schema_ref`)

If upstream pagination state is missing, the gateway returns
`INVALID_ARGUMENT` with diagnostics in `details` (for example:
`queryable_json_found`, `has_more_detected`, `next_params_detected`,
`continuable`, and `query_json_source` when available).

## `artifact(action="blob_list")`

### Required arguments

- `_gateway_context.session_id`
- `action="blob_list"`
- one target shape:
  - single anchor: `artifact_id` (optionally `scope="all_related"`)
  - explicit list: `artifact_ids` (single-scope only)

### Optional arguments

- `scope`: `single` (default) or `all_related` (anchor only)
- `limit`: max blobs to return (default `100`, max `1000`)

### Behavior

- joins `artifacts -> payload_binary_refs -> binary_blobs`
- returns metadata only (never inline blob bytes)
- deduplicates by `binary_hash`

Typical fields per blob row:

- `blob_id`
- `binary_hash`
- `mime`
- `byte_count`
- `artifact_ids`
- `source_artifact_id`
- `source_tool`
- `uri` (`sift://blob/<blob_id>`)

## `artifact(action="blob_materialize")`

### Required arguments

- `_gateway_context.session_id`
- `action="blob_materialize"`
- one identifier: `blob_id` or `binary_hash`

### Optional arguments

- `destination_dir` (must be under allowed staging roots)
- `filename`
- `extension`
- `if_exists`: `reuse` (default), `overwrite`, `fail`
- `materialize_mode`: `copy` (default), `hardlink`, `auto`
- `max_bytes`: optional byte-size guardrail

### Behavior

- resolves blob metadata from `binary_blobs`
- stages the blob to local filesystem and returns `path`
- returns metadata/path only (never inline blob bytes)
- includes `sha256` (same value as `binary_hash`) for upload handoff
- returns `materialize_mode_used` so callers can audit copy vs hardlink

Extension resolution order:

1. explicit `extension` (or filename suffix)
2. `python-magic` MIME (when available)
3. stored MIME map
4. `.bin` fallback

## `artifact(action="blob_cleanup")`

### Required arguments

- `_gateway_context.session_id`
- `action="blob_cleanup"`

### Optional arguments

- explicit cleanup mode:
  - `path` (single staged file path)
  - `paths` (list of staged file paths)
- sweep mode:
  - `destination_dir` (defaults to gateway staging dir)
  - `older_than_seconds` (default `0`, meaning all files)
- controls:
  - `dry_run` (report only)
  - `limit` (candidate cap)

### Behavior

- cleans staged local files only (does not remove DB blob records)
- enforces allowed staging roots
- supports explicit-path cleanup and directory sweep cleanup

## `artifact(action="blob_manifest")`

### Required arguments

- `_gateway_context.session_id`
- `action="blob_manifest"`
- one target shape:
  - single anchor: `artifact_id` (optionally `scope="all_related"`)
  - explicit list: `artifact_ids` (single-scope only)

### Optional arguments

- `format`: `csv` (default) or `json`
- `destination_dir`
- `filename`
- `limit`
- `if_exists`: `reuse`, `overwrite` (default), `fail`

### Behavior

- exports blob metadata from `blob_list`-style discovery into a local file
- returns manifest `path` and summary counts
- exports metadata only; no inline blob bytes

## Mirrored Response Contract

Mirrored upstream calls and `next_page` return:

- `response_mode`: `full` or `schema_ref`
- `artifact_id`
- `lineage`
- optional `pagination`
- optional `metadata`

### `full`

Includes inline `payload`.

### `schema_ref`

Includes:

- `artifact_id`
- either representative `sample_item` fields (`sample_item`, `sample_item_count`,
  `sample_item_source_index`, optional `sample_item_text_truncated`)
- or `schemas` (verbose schema fallback when sample preview is not representative)

`metadata` commonly includes:

- `usage` (code-query follow-up helper)
- `queryable_roots` (root paths valid for `root_path`/`root_paths`)
- optional `cardinality` (compact payload shape/count hints)
- optional `query_json_source` (`part_index`, `part_type`, `encoding`)

## Response Mode Selection

Mode selection is shared across mirrored calls and code output:

1. If pagination exists: `schema_ref`.
2. Else if serialized `full` bytes exceed configured cap: `schema_ref`.
3. Else: `full`.

## Pagination Metadata

When upstream pagination exists, `pagination` includes:

- `layer="upstream"`
- `retrieval_status` (`PARTIAL` or `COMPLETE`)
- `partial_reason`
- `has_more`
- `next` (object or `null`)
- `next.kind` (`tool_call` | `command` | `params_only`)
- optional `next.params`
- `capability` (`has_more_signal_detected`, `continuable`, `next_params_detected`)
- continuation hint

Do not claim completion until `pagination.retrieval_status == "COMPLETE"`.

## CLI Output Contract

`sift-gateway run` and `sift-gateway code` expose two output modes:

- default human summary output (compact, only present fields)
- `--json` machine output (single minified deterministic object)

### `sift-gateway run` human output fields

Possible lines:

- `artifact`
- `mode`
- `records`
- `bytes`
- `capture`
- `expires`
- `tags`
- `exit`
- `next`
- `schema_roots`
- `hint`

### `sift-gateway code` human output

Summary header plus formatted JSON payload.

### `--json` shared keys

- `response_mode`
- `artifact_id`
- optional `lineage`
- optional `pagination`
- optional `metadata`

For pagination, `run` includes a CLI-native continuation helper under
`pagination.next.command_line` when `pagination.next.kind == "command"`.

## Compatibility Rules

- additive JSON fields are allowed
- changes to human summary layout must be reflected in CLI tests and docs
