# Error Contract

Canonical reference for Sift gateway/tool error responses.

## Error envelope

All gateway errors use this shape:

```json
{
  "type": "gateway_error",
  "code": "INVALID_ARGUMENT",
  "message": "human-readable description",
  "details": {}
}
```

Notes:

- `type` is always `gateway_error`.
- `code` is machine-readable and stable.
- `message` is intended for humans/logs.
- `details` is optional structured context (always present as object in response helper).

## Top-level `code` values

### Request/validation

| Code | Meaning |
|------|---------|
| `INVALID_ARGUMENT` | Invalid tool arguments, invalid cursor token format, invalid JSONPath/where/select params, or unsupported parameter combination |

### Artifact lifecycle

| Code | Meaning |
|------|---------|
| `NOT_FOUND` | Artifact (or related upstream tool) does not exist or is not visible in session scope |
| `GONE` | Artifact exists but has been soft-deleted |
| `RESOURCE_EXHAUSTED` | Query budget/limit exceeded (for example lineage size limits) |
| `NOT_IMPLEMENTED` | Requested operation requires a DB backend that is not configured |

### Cursor-specific

| Code | Meaning |
|------|---------|
| `CURSOR_EXPIRED` | Cursor TTL elapsed |
| `CURSOR_STALE` | Cursor binding mismatch due to changed artifact/query context |

### Internal gateway failures

| Code | Meaning |
|------|---------|
| `INTERNAL` | Unexpected gateway-side failure (DB/runtime corruption/reconstruction failure/etc.) |

## Upstream/runtime failure codes

These codes are emitted for upstream call failures and surfaced in gateway errors,
runtime status metadata, or persisted upstream error envelopes depending on context.

| Code | Meaning |
|------|---------|
| `UPSTREAM_DNS_FAILURE` | DNS resolution error |
| `UPSTREAM_TIMEOUT` | Upstream call timeout |
| `UPSTREAM_LAUNCH_FAILURE` | stdio process launch failure (missing executable/permission) |
| `UPSTREAM_NETWORK_FAILURE` | Network unreachable |
| `UPSTREAM_TRANSPORT_FAILURE` | Transport-layer OS/runtime failure |
| `UPSTREAM_RUNTIME_FAILURE` | Other upstream execution error |
| `UPSTREAM_RESPONSE_INVALID` | Upstream returned malformed MCP result payload |

Related status-only/runtime codes:

| Code | Where used |
|------|------------|
| `UPSTREAM_STARTUP_FAILURE` | `gateway_status` payload for startup probe failures |
| `UPSTREAM_TOOL_ERROR` | Runtime status metadata when upstream returns `isError=true` |
| `UPSTREAM_ERROR` | Persisted envelope `error.code` when upstream returns `isError=true` |

## `details.code` subcodes

Some handlers include a secondary machine code in `details.code`.

| `details.code` | Meaning |
|----------------|---------|
| `INCOMPATIBLE_LINEAGE_SCHEMA` | Related artifacts have incompatible schemas for requested root path |
| `MISSING_ROOT_PATH` | Related artifact does not contain requested root path |
| `SKIPPED_ARTIFACT` | Artifact was skipped while processing lineage query (for example missing/deleted/non-queryable) |

## Compatibility notes

- Prefer handling `INTERNAL` for gateway internal failures.
- `artifact(action="query", query_kind=...)` is the canonical retrieval surface; legacy handler entrypoints exist only for compatibility wrappers.
