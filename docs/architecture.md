# Sift Architecture (Contract V1)

Authoritative architecture reference for the current public surface.

## 1. Overview

Sift is a local, single-tenant MCP proxy and artifact runtime that:

1. Mirrors upstream MCP tools.
2. Persists all mirrored responses as artifacts.
3. Exposes a narrow retrieval surface:
   - `artifact(action="query", query_kind="code")`
   - `artifact(action="next_page")`
4. Provides equivalent CLI workflows:
   - `sift-gateway run`
   - `sift-gateway run --continue-from`
   - `sift-gateway code`

## 2. Design invariants

- Single workspace: `WORKSPACE_ID = "local"`.
- Deterministic mappings and traversal.
- Always persist mirrored responses (including upstream errors).
- Explicit pagination continuation (never implicit completeness claims).
- Outbound redaction enabled by default.

## 3. Processing pipeline

For mirrored tool calls, CLI run captures, and code outputs:

1. Execute tool/command/code.
2. Parse JSON payload.
3. Detect pagination from raw parsed payload.
4. Redact sensitive output values.
5. Persist artifact.
6. Run mapping.
7. Build compact schema payload (`schemas_compact` + `schema_legend`).
8. Choose response mode (`full` or `schema_ref`).
9. Return response with lineage and pagination metadata.

## 4. Response modes

Every run/code-style response is artifact-centric with:

- `response_mode`
- `artifact_id`
- optional `lineage`
- optional `pagination`
- optional `metadata`

### `full`

Returns inline `payload`.

### `schema_ref`

Returns compact schema reference:

- `artifact_id`
- `schemas_compact`
- `schema_legend`

### Mode selection

1. If pagination exists: `schema_ref`.
2. Else if full bytes > configured cap: `schema_ref`.
3. Else if `schema_ref` is at least 50% smaller: `schema_ref`.
4. Else: `full`.

## 5. Pagination model

Sift uses explicit upstream pagination metadata (`pagination.layer="upstream"`).

Key fields:

- `retrieval_status` (`PARTIAL` | `COMPLETE`)
- `partial_reason`
- `has_more`
- `next` (object or `null`)
- `next.kind` (`tool_call` | `command` | `params_only`)
- optional `next.params`

Continuation APIs:

- MCP: `artifact(action="next_page", artifact_id=...)`
- CLI: `sift-gateway run --continue-from <artifact_id> -- <next-command>`

Each continued page is linked through lineage metadata:

- `parent_artifact_id`
- `chain_seq`

## 6. Storage model

### Filesystem

```
DATA_DIR/
  blobs/bin/<ab>/<cd>/<sha256_hex>
  resources/
  state/config.json
  state/gateway.db
  tmp/
  logs/
```

### SQLite core tables

- `sessions`
- `binary_blobs`
- `payload_blobs`
- `payload_binary_refs`
- `artifacts`
- `artifact_roots`

## 7. Mapping model

Sift builds deterministic mapping metadata for persisted payloads:

- full mapping for smaller payloads
- partial mapping with bounded sampling for larger payloads

Determinism linkage includes traversal/mapping versions and budget fingerprints.

## 8. Code runtime model

`query_kind="code"` executes sandboxed Python with bounded resources.

Supported entrypoints:

- single artifact: `run(data, schema, params)`
- multi artifact: `run(artifacts, schemas, params)`

Highlights:

- inputs are loaded from persisted, redacted artifacts
- outputs must be JSON-serializable
- output size bounded by `max_bytes_out`
- runtime enforces AST/import/time/memory guardrails

## 9. Request identity and lineage

- mirrored requests use deterministic identity hashing (`request_key`)
- each persisted artifact stores provenance (`capture_kind`, `capture_origin`)
- lineage links allow chain-aware continuation and scope-aware code execution

## 10. Error model

Stable top-level gateway error envelope:

```json
{
  "type": "gateway_error",
  "code": "INVALID_ARGUMENT",
  "message": "...",
  "details": {}
}
```

See `docs/errors.md` for full taxonomy.

## 11. Observability

- structured JSON logs (opt-in with `--logs`)
- metrics for capture, pagination, mapping, code runtime, and redaction events
- deterministic debug fields for lineage/mapping diagnostics

See `docs/observability.md` for event catalog.

## 12. Security posture

- outbound secret redaction on tool responses by default
- code runtime is policy-constrained but not a full OS sandbox
- non-local HTTP binds require auth token

See `../SECURITY.md`.
