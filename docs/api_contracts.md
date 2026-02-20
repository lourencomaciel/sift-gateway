# API Contracts

Canonical behavior for Sift's public runtime surface.

## Public Tool Surface

### Mirrored upstream tools

Every mirrored upstream call is persisted as an artifact and returns one of:

- `response_mode="full"`
- `response_mode="schema_ref"`

### `artifact` tool

The consolidated `artifact` tool supports only two actions:

- `action="query"` with `query_kind="code"`
- `action="next_page"`

Legacy `query_kind` values (`describe`, `get`, `select`, `search`) are not part
of this contract.

## Required Gateway Context

Mirrored tool calls and `artifact(...)` calls must include
`_gateway_context.session_id`.

If missing, the gateway returns `INVALID_ARGUMENT`.

## `artifact(action="query", query_kind="code")`

### Required arguments

- `_gateway_context.session_id`
- `action="query"`
- `query_kind="code"`
- `code`
- target selection:
  - single target: `artifact_id` (and optional `root_path`, default `$`)
  - multi target: `artifact_ids` (and optional `root_paths` map)

### Optional arguments

- `scope`: `all_related` (default) or `single`
- `params`: JSON object passed to `run(..., ..., params)`

### Runtime entrypoints

- Single target: `run(data, schema, params)`
- Multi target: `run(artifacts, schemas, params)`

### Response shape (`query_kind="code"`)

Always returns artifact-centric payload with shared envelope keys:

- `response_mode`
- `artifact_id` (derived artifact)
- `lineage`
- `metadata` (includes `stats` and determinism metadata)

Compatibility fields may also be present:

- `items`
- `total_matched`
- `truncated`
- `scope`
- `stats`
- `determinism`
- `warnings`
- `sampled_only`

Code queries are unpaginated by retrieval cursor (no query cursor loop).

## `artifact(action="next_page")`

### Required arguments

- `_gateway_context.session_id`
- `action="next_page"`
- `artifact_id`

### Behavior

- Loads upstream pagination state from the referenced artifact.
- Replays the mirrored upstream tool with stored continuation params.
- Persists a new artifact linked by lineage (`parent_artifact_id`, `chain_seq`).
- Returns the same mirrored-response contract (`full` or `schema_ref`).

## Mirrored Response Contract

Mirrored upstream calls and next-page calls return a gateway payload with:

- `response_mode`: `full` or `schema_ref`
- `artifact_id`
- `lineage`
- optional `pagination`
- optional `metadata`

### `full` mode

Includes `payload` inline.

### `schema_ref` mode

Includes:

- `artifact_id`
- `schemas_compact`
- `schema_legend`

No verbose `schemas` field is part of this public contract.

## Response Mode Selection

Sift evaluates mode using the same policy across mirrored calls and code output:

1. If pagination exists: return `schema_ref`.
2. Else if serialized `full` bytes > `max_bytes`: return `schema_ref`.
3. Else if `schema_ref_bytes * 2 <= full_bytes`: return `schema_ref`.
4. Else return `full`.

`max_bytes` is driven by configured output budget (`max_bytes_out`).

## Pagination Metadata

When upstream pagination exists, responses include a `pagination` object with
upstream-layer metadata, including:

- `layer="upstream"`
- `retrieval_status` (`PARTIAL` or `COMPLETE`)
- `partial_reason`
- `has_more`
- `has_next_page`
- `next_action`
- optional `next_params`
- continuation `hint`

Do not claim completion until `pagination.retrieval_status == "COMPLETE"`.

## CLI Equivalence

CLI uses the same contract and storage model:

- capture: `sift-gateway run -- <command>`
- continuation: `sift-gateway run --continue-from <artifact_id> -- <next-command>`
- code query: `sift-gateway code ...`

CLI additionally rewrites `pagination.next_action` to a CLI-native command
shape for convenience.
