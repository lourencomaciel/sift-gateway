# Observability Contract

Canonical reference for logging and runtime observability behavior.

## Structured logging

Sift uses `structlog` and emits structured events (JSON by default).

Configuration entrypoints:

- `configure_logging(json_output=True, level="INFO")`
- `get_logger(**initial_context)`

Common log fields:

- `event`: event name (prefer constants from `LogEvents`)
- `level`: log level
- `timestamp`: ISO timestamp
- bound context fields (for example `component`, `request_key`, `artifact_id`)

## Event naming

Use `LogEvents` constants from `src/sift_mcp/obs/logging.py`.

### Startup

- `gateway.startup.begin`
- `gateway.startup.upstream_discovered`
- `gateway.startup.complete`
- `gateway.startup.failed`

### Request

- `gateway.request.received`
- `gateway.request.key_computed`

### Artifact/mapping

- `gateway.artifact.created`
- `gateway.artifact.envelope_sizes`
- `gateway.artifact.oversize_json`
- `gateway.artifact.binary_blob_write`
- `gateway.artifact.binary_blob_dedupe`
- `gateway.mapping.started`
- `gateway.mapping.completed`
- `gateway.mapping.failed`
- `gateway.codegen.started`
- `gateway.codegen.completed`
- `gateway.codegen.failed`
- `gateway.codegen.timeout`
- `gateway.codegen.rejected`

### Cursor

- `gateway.cursor.issued`
- `gateway.cursor.verified`
- `gateway.cursor.invalid`
- `gateway.cursor.expired`
- `gateway.cursor.stale`

### Pagination/auto-pagination

- `gateway.auto_pagination.timeout`
- `gateway.auto_pagination.ref_resolution_error`
- `gateway.auto_pagination.upstream_timeout`
- `gateway.auto_pagination.upstream_failure`
- `gateway.auto_pagination.upstream_error_result`
- `gateway.auto_pagination.envelope_normalization_failed`
- `gateway.auto_pagination.binary_content_stop`

### Quota/pruning

- `gateway.quota.check`
- `gateway.quota.breach`
- `gateway.quota.prune_triggered`
- `gateway.quota.prune_complete`
- `gateway.quota.exceeded`
- `gateway.prune.soft_delete`
- `gateway.prune.hard_delete`
- `gateway.prune.bytes_reclaimed`
- `gateway.prune.fs_reconcile`

## Logging best-practice rules

- Prefer `LogEvents` constants over free-form event strings.
- Include stable machine fields (for example `artifact_id`, `request_key`, `pages_fetched`) instead of embedding key values in message text.
- Preserve `exc_info=True` on warning/error paths where stack traces matter.
- Use component binding (`get_logger(component="...")`) for subsystem-level filtering.

## Runtime status observability

`gateway_status` provides runtime diagnostics for upstreams:

- startup failures (`startup_error.code = UPSTREAM_STARTUP_FAILURE`)
- runtime failure metadata (`runtime.last_error_code`, `runtime.last_error_message`, timestamps)
- optional active probes (`probe_upstreams=true`)

Use `gateway_status` for health snapshots and logs for event timelines.

## Metrics additions for outbound secret redaction

`GatewayMetrics` exports redaction counters under the `security` section of
`snapshot()` / `reset()`:

- `secret_redaction_matches`
- `secret_redaction_failures`

When a fail-closed redaction attempt errors, Sift logs a warning with message
`tool response redaction failed` and `error_type` context.

## Metrics additions for code queries

`GatewayMetrics` exports code-query counters/histogram under the `codegen`
section of `snapshot()` / `reset()`:

- `executions`
- `success`
- `failure`
- `timeout`
- `input_records`
- `output_records`
- `latency` (min/max/sum/count/avg)
