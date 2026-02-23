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

- `scope`: `all_related` (default, pagination-chain related artifacts) or `single`
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

## Pipeline mode (`steps`)

When `steps` is provided, the gateway executes a multi-step code pipeline
in a single tool call. Each step's output becomes the next step's input.

### Arguments

- `steps`: array of `{code, params?, name?}` objects (1 to `code_query_max_steps`,
  default limit 5)
- `name`: optional human-readable step label (non-empty string); included in
  pipeline metadata and error details for easier debugging
- When `steps` is present, top-level `code` is ignored
- Step 0 executes against the original artifact/root_path/scope
- Step 1+ executes against the previous step's derived artifact
  (`root_path="$"`, `scope="single"`)

### Error handling

If any step fails, the response includes `step_index` and `total_steps`
in the error details, plus `last_successful_artifact_id` when a prior
step succeeded. When the failing step has a `name`, the error details
include `step_name`.

### Response shape

Same as single-step code query, plus `metadata.pipeline`:

- `pipeline.version`: `"pipeline_v1"`
- `pipeline.step_count`: number of steps executed
- `pipeline.steps`: array of per-step objects:
  - `code_hash`: deterministic hash of step code
  - `params_hash`: deterministic hash of step params
  - `name`: step name (or `null` if not provided)
  - `item_count`: number of output items from this step
  - `used_bytes`: serialized output size in bytes
- `pipeline.intermediate_artifact_ids`: artifact IDs from all steps
  except the final one

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

## Response Mode Selection

Mode selection is shared across mirrored calls and code output:

1. If pagination exists: `schema_ref`.
2. Else if serialized `full` bytes exceed configured cap: `schema_ref`.
3. Else if `schema_ref` is at least 50% smaller: `schema_ref`.
4. Else: `full`.

## Pagination Metadata

When upstream pagination exists, `pagination` includes:

- `layer="upstream"`
- `retrieval_status` (`PARTIAL` or `COMPLETE`)
- `partial_reason`
- `has_more`
- `next` (object or `null`)
- `next.kind` (`tool_call` | `command` | `params_only`)
- optional `next.params`
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
