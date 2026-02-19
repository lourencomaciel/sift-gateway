# Configuration Reference

All configuration keys, defaults, and environment variable mappings for Sift.

## Precedence

Environment variables (`SIFT_MCP_*`) override `DATA_DIR/state/config.json`,
which overrides compiled defaults.

## Filesystem

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `data_dir` | Path | `.sift-mcp` | `SIFT_MCP_DATA_DIR` | Root data directory |

Derived paths (not directly configurable):

| Path | Derivation |
|------|-----------|
| `state_dir` | `{data_dir}/state` |
| `resources_dir` | `{data_dir}/resources` |
| `blobs_bin_dir` | `{data_dir}/blobs/bin` |
| `tmp_dir` | `{data_dir}/tmp` |
| `logs_dir` | `{data_dir}/logs` |
| `config_json_path` | `{data_dir}/state/config.json` |
| `upstream_secrets_dir` | `{data_dir}/state/upstream_secrets/` |

## Database

Sift uses SQLite as its database backend.

## SQLite

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `sqlite_busy_timeout_ms` | int | `5000` | `SIFT_MCP_SQLITE_BUSY_TIMEOUT_MS` | SQLite busy retry timeout (ms) |

Derived path:

| Path | Derivation |
|------|-----------|
| `sqlite_path` | `{state_dir}/gateway.db` |

Runtime behavior:

- SQLite runs in WAL mode.

## Envelope storage

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `envelope_jsonb_mode` | enum | `full` | `SIFT_MCP_ENVELOPE_JSONB_MODE` | `full`, `minimal_for_large`, `none` |
| `envelope_jsonb_minimize_threshold_bytes` | int | `1000000` | `SIFT_MCP_ENVELOPE_JSONB_MINIMIZE_THRESHOLD_BYTES` | Threshold for minimal JSONB mode |
| `envelope_canonical_encoding` | enum | `gzip` | `SIFT_MCP_ENVELOPE_CANONICAL_ENCODING` | `gzip`, `none` |

## Ingest caps

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_inbound_request_bytes` | int | `10000000` | `SIFT_MCP_MAX_INBOUND_REQUEST_BYTES` | Max inbound request size |
| `max_upstream_error_capture_bytes` | int | `100000` | `SIFT_MCP_MAX_UPSTREAM_ERROR_CAPTURE_BYTES` | Max upstream error text captured |
| `max_json_part_parse_bytes` | int | `50000000` | `SIFT_MCP_MAX_JSON_PART_PARSE_BYTES` | Max JSON part parse budget |

## Storage caps

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_binary_blob_bytes` | int | `500000000` | `SIFT_MCP_MAX_BINARY_BLOB_BYTES` | Max single binary blob size |
| `max_payload_total_bytes` | int | `1000000000` | `SIFT_MCP_MAX_PAYLOAD_TOTAL_BYTES` | Max payload size |
| `max_total_storage_bytes` | int | `10000000000` | `SIFT_MCP_MAX_TOTAL_STORAGE_BYTES` | Max total storage quota |

## Quota enforcement

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `quota_enforcement_enabled` | bool | `true` | `SIFT_MCP_QUOTA_ENFORCEMENT_ENABLED` | Enable storage quota enforcement |
| `quota_prune_batch_size` | int | `100` | `SIFT_MCP_QUOTA_PRUNE_BATCH_SIZE` | Soft-delete batch size per prune pass |
| `quota_max_prune_rounds` | int | `5` | `SIFT_MCP_QUOTA_MAX_PRUNE_ROUNDS` | Max prune passes before failing request |
| `quota_hard_delete_grace_seconds` | int | `0` | `SIFT_MCP_QUOTA_HARD_DELETE_GRACE_SECONDS` | Grace period before hard delete |

## Full mapping

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_full_map_bytes` | int | `10000000` | `SIFT_MCP_MAX_FULL_MAP_BYTES` | Max envelope bytes for full mapping |
| `max_in_memory_mapping_bytes` | int | `derived from memory capacity (clamped to 50MB-512MB)` | `SIFT_MCP_MAX_IN_MEMORY_MAPPING_BYTES` | Max inline JSON bytes allowed for in-memory mapping path |
| `max_root_discovery_k` | int | `3` | `SIFT_MCP_MAX_ROOT_DISCOVERY_K` | Max discovered root arrays |

## Partial mapping budgets

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_bytes_read_partial_map` | int | `50000000` | `SIFT_MCP_MAX_BYTES_READ_PARTIAL_MAP` | Max bytes read during sampling |
| `max_compute_steps_partial_map` | int | `5000000` | `SIFT_MCP_MAX_COMPUTE_STEPS_PARTIAL_MAP` | Max compute steps during sampling |
| `max_depth_partial_map` | int | `64` | `SIFT_MCP_MAX_DEPTH_PARTIAL_MAP` | Max traversal depth |
| `max_records_sampled_partial` | int | `100` | `SIFT_MCP_MAX_RECORDS_SAMPLED_PARTIAL` | Max sampled records per root |
| `max_record_bytes_partial` | int | `100000` | `SIFT_MCP_MAX_RECORD_BYTES_PARTIAL` | Max bytes per sampled record |
| `max_leaf_paths_partial` | int | `500` | `SIFT_MCP_MAX_LEAF_PATHS_PARTIAL` | Max discovered leaf paths |
| `max_root_discovery_depth` | int | `5` | `SIFT_MCP_MAX_ROOT_DISCOVERY_DEPTH` | Max discovery depth for roots |

## Retrieval budgets

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_items` | int | `1000` | `SIFT_MCP_MAX_ITEMS` | Max response items |
| `max_bytes_out` | int | `5000000` | `SIFT_MCP_MAX_BYTES_OUT` | Max response bytes |
| `max_wildcards` | int | `10000` | `SIFT_MCP_MAX_WILDCARDS` | Max wildcard expansions |
| `max_compute_steps` | int | `1000000` | `SIFT_MCP_MAX_COMPUTE_STEPS` | Max retrieval compute steps |
| `passthrough_max_bytes` | int | `8192` | `SIFT_MCP_PASSTHROUGH_MAX_BYTES` | Max serialized mirrored-response size to return raw (`0` disables) |

## Outbound secret redaction

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `secret_redaction_enabled` | bool | `true` | `SIFT_MCP_SECRET_REDACTION_ENABLED` | Enable outbound response secret redaction |
| `secret_redaction_fail_closed` | bool | `false` | `SIFT_MCP_SECRET_REDACTION_FAIL_CLOSED` | Return INTERNAL when redaction cannot run |
| `secret_redaction_max_scan_bytes` | int | `32768` | `SIFT_MCP_SECRET_REDACTION_MAX_SCAN_BYTES` | Max UTF-8 bytes scanned per string value |
| `secret_redaction_placeholder` | string | `[REDACTED_SECRET]` | `SIFT_MCP_SECRET_REDACTION_PLACEHOLDER` | Replacement token for redacted values |

Disable outbound redaction:

```bash
export SIFT_MCP_SECRET_REDACTION_ENABLED=false
```

## JSONPath

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_jsonpath_length` | int | `4096` | `SIFT_MCP_MAX_JSONPATH_LENGTH` | Max JSONPath string length |
| `max_path_segments` | int | `64` | `SIFT_MCP_MAX_PATH_SEGMENTS` | Max JSONPath segments |
| `max_wildcard_expansion_total` | int | `10000` | `SIFT_MCP_MAX_WILDCARD_EXPANSION_TOTAL` | Max wildcard expansion results |

## Search and lineage-query limits

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `artifact_search_max_limit` | int | `200` | `SIFT_MCP_ARTIFACT_SEARCH_MAX_LIMIT` | Max search `limit` |
| `related_query_max_artifacts` | int | `256` | `SIFT_MCP_RELATED_QUERY_MAX_ARTIFACTS` | Max artifacts in `scope=all_related` query |

Lineage query rules for `artifact(action="query")`:

- `query_kind` is required and must be one of `describe|get|select|search|code`.
- `query_kind=describe|get|select` requires `artifact_id`.
- `query_kind=code` requires `artifact_id` (single) or `artifact_ids` (multi).
- `scope` applies only to `describe|get|select` and defaults to `all_related`.
- `scope=single` restricts execution to the anchor artifact.
- `query_kind=search` rejects `artifact_id` and `scope`.
- `query_kind=code` ignores `scope` and always uses all-related semantics.
- `query_kind=code` returns all results in a single response (no pagination); output is bounded by `max_bytes_out`.
- `query_kind=code` runtime failures can include `details.traceback` (up to 2000 chars).

For `query_kind=select`, lineage merge is strict by root signature. If related
artifacts expose incompatible schemas at the requested `root_path`, query fails
with `INVALID_ARGUMENT` (`details.code = INCOMPATIBLE_LINEAGE_SCHEMA`).

## Code query runtime

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `code_query_enabled` | bool | `true` | `SIFT_MCP_CODE_QUERY_ENABLED` | Enable `query_kind=code` |
| `code_query_allowed_import_roots` | list[string] \| null | `null` | `SIFT_MCP_CODE_QUERY_ALLOWED_IMPORT_ROOTS` | Explicit import-root allowlist for code runtime; when `null`, built-in defaults are used |
| `code_query_timeout_seconds` | float | `8.0` | `SIFT_MCP_CODE_QUERY_TIMEOUT_SECONDS` | Subprocess wall-clock timeout |
| `code_query_max_memory_mb` | int | `512` | `SIFT_MCP_CODE_QUERY_MAX_MEMORY_MB` | Best-effort subprocess memory cap |
| `code_query_max_input_records` | int | `100000` | `SIFT_MCP_CODE_QUERY_MAX_INPUT_RECORDS` | Max root records passed to code runtime |
| `code_query_max_input_bytes` | int | `50000000` | `SIFT_MCP_CODE_QUERY_MAX_INPUT_BYTES` | Max serialized runtime input size |

Example env override:

```bash
SIFT_MCP_CODE_QUERY_ALLOWED_IMPORT_ROOTS='["math","json","jmespath","numpy","pandas"]'
```

### Installing packages for code queries

Sift runs in an isolated Python environment (e.g. via `pipx` or `uv tool`).
Packages installed in your system Python are not available to code queries.
Use the built-in install command to add packages into Sift's own environment:

```bash
# Install into Sift's environment and update the allowlist
sift-mcp install pandas scipy

# Uninstall and remove from allowlist
sift-mcp uninstall scipy
```

These commands:

1. Run `pip install` (or `pip uninstall`) using Sift's own Python interpreter.
2. Add (or remove) the package root to the gateway's
   `code_query_allowed_import_roots` config so the import is permitted.

For convenience, common data-science packages are available as an install
extra:

```bash
pipx install "sift-mcp[data-science]"   # pandas, numpy, jmespath
```

`query_kind=code` is intended for trusted environments. Policy checks
reduce risk but do not provide full OS-level sandboxing.

## Cursor

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `cursor_ttl_minutes` | int | `60` | `SIFT_MCP_CURSOR_TTL_MINUTES` | Cursor TTL |

## Auto-pagination

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `auto_paginate_max_pages` | int | `10` | `SIFT_MCP_AUTO_PAGINATE_MAX_PAGES` | Max pages to merge (`0` disables) |
| `auto_paginate_max_records` | int | `1000` | `SIFT_MCP_AUTO_PAGINATE_MAX_RECORDS` | Approximate record budget before stopping |
| `auto_paginate_timeout_seconds` | float | `30.0` | `SIFT_MCP_AUTO_PAGINATE_TIMEOUT_SECONDS` | Loop timeout |

Auto-pagination applies to mirrored tool calls with upstream pagination state.
When enabled, Sift fetches additional upstream pages and merges them into one
artifact before returning.

## Miscellaneous

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `binary_probe_bytes` | int | `65536` | `SIFT_MCP_BINARY_PROBE_BYTES` | Bytes used for binary detection |
| `select_missing_as_null` | bool | `false` | `SIFT_MCP_SELECT_MISSING_AS_NULL` | Missing select fields become `null` |

## Upstream configuration

In config files, upstreams must be declared via `mcpServers`
(or VS Code `mcp.servers`).
Environment variables may override the resolved `upstreams` structure.

Nested env-var pattern:

- `SIFT_MCP_UPSTREAMS__<INDEX>__<FIELD>`

Examples:

- `SIFT_MCP_UPSTREAMS__0__PREFIX=github`
- `SIFT_MCP_UPSTREAMS__0__ARGS=["-y","@modelcontextprotocol/server-github"]`

Upstream fields:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `prefix` | str | required | Tool namespace prefix |
| `transport` | enum | required | `stdio` or `http` |
| `command` | str | null | stdio command |
| `args` | list[str] | `[]` | stdio args |
| `env` | dict[str,str] | `{}` | stdio env |
| `url` | str | null | http URL |
| `headers` | dict[str,str] | `{}` | http headers |
| `semantic_salt_headers` | list[str] | `[]` | Non-secret header keys used in identity |
| `semantic_salt_env_keys` | list[str] | `[]` | Non-secret env keys used in identity |
| `pagination` | object | null | Upstream pagination config |
| `auto_paginate_max_pages` | int | null | Per-upstream override for gateway auto-pagination page cap |
| `auto_paginate_max_records` | int | null | Per-upstream override for gateway auto-pagination record cap |
| `auto_paginate_timeout_seconds` | float | null | Per-upstream override for gateway auto-pagination timeout |
| `passthrough_allowed` | bool | `true` | Allow small mirrored responses to return raw for this upstream |
| `secret_ref` | str | null | Reference to upstream secret file |
| `inherit_parent_env` | bool | `false` | Inherit full parent env for stdio |
| `external_user_id` | str | null | Stable user identity for upstream auth persistence (`auto` generates UUID) |

### MCP Client Formats

Sift accepts:

- `mcpServers` (Claude Desktop, Cursor, Claude Code, Windsurf)
- `mcp.servers` (VS Code)
- `context_servers` (Zed)

```json
{
  "mcpServers": {
    "github": {
      "command": "/usr/local/bin/mcp-github",
      "args": ["--config", "github.json"],
      "_gateway": {
        "secret_ref": "github",
        "inherit_parent_env": false,
        "external_user_id": "auto",
        "passthrough_allowed": true,
        "auto_paginate_max_pages": 5,
        "auto_paginate_max_records": 500,
        "auto_paginate_timeout_seconds": 15.0,
        "pagination": {
          "strategy": "cursor",
          "cursor_response_path": "$.paging.cursors.after",
          "cursor_param_name": "after",
          "has_more_response_path": "$.paging.next"
        }
      }
    }
  }
}
```

Transport inference:

- `command` present -> `stdio`
- `url` present -> `http`

Legacy `upstreams` array format is not supported.

### `_gateway.pagination`

Defines how Sift detects and advances upstream pagination.

Supported strategies:

- `cursor`
- `offset`
- `page_number`
- `param_map`

Validation behavior:

- `cursor` requires `cursor_response_path` and `cursor_param_name`.
- `offset` requires `offset_param_name`, `page_size_param_name`, and
  `has_more_response_path`.
- `page_number` requires `page_param_name` and `has_more_response_path`.
- `param_map` requires non-empty `next_params_response_paths`.

Pagination layers are explicit:

- Retrieval-layer continuation: `artifact(action="query", query_kind=..., cursor=...)`
- Upstream-page continuation: `artifact(action="next_page", artifact_id=...)`

## Mirrored response passthrough

For mirrored tool calls, Sift always persists/map-indexes responses. Return
shape is controlled by passthrough settings:

- if serialized response size <= `passthrough_max_bytes` and
  `upstream.passthrough_allowed=true`, Sift returns the raw upstream result.
- otherwise Sift returns the gateway artifact handle payload.

Passthrough is automatically disabled when a mirrored call still has upstream
pages remaining (`pagination.has_more=true`) or when gateway auto-pagination
merged additional pages.

### `_gateway.secret_ref`

References `{data_dir}/state/upstream_secrets/{ref}.json` (created by
`sift-mcp init`, mode 0600). Inline `env`/`headers` and `secret_ref` cannot
be used together for the same upstream.

### `_gateway.inherit_parent_env`

Controls stdio env inheritance:

- `false` (default): allowlisted env only
- `true`: full parent env

Merge order:

1. Base env (allowlist or full parent)
2. Secret file `env` (if `secret_ref` set)
3. Inline `env`

### `_gateway.external_user_id`

Persistent identity value used for upstream auth persistence:

- `null`/unset: disabled
- `"auto"`: generate and persist UUID
- any other string: use value as-is

For stdio upstreams, Sift appends `--external-user-id <value>` at launch time
unless args already provide it.

## Sync metadata (`_gateway_sync`)

After `sift-mcp init --from <path-or-shortcut>`, Sift stores sync metadata in
`{data_dir}/state/config.json`. On startup (except `--check`), it
imports newly added upstreams from the source config, externalizes secrets,
then rewrites source config back to gateway-only.

If `_gateway_sync.data_dir` points to another data directory, Sift follows that
redirect only when the target `state/config.json` exists and is valid.

## `upstream add` target resolution

`sift-mcp upstream add` accepts:

- `--from <path-or-shortcut>`: resolve source, then use explicit `--data-dir`
  if provided, otherwise source-pinned gateway `--data-dir` when present.
- no `--from`: use explicit `--data-dir` when provided.

## Server runtime flags

| Flag | Default | Description |
|------|---------|-------------|
| `--transport` | `stdio` | `stdio`, `sse`, `streamable-http` |
| `--host` | `127.0.0.1` | HTTP bind address |
| `--port` | `8080` | HTTP bind port |
| `--path` | `/mcp` | HTTP route path |
| `--auth-token` | none | Bearer token for non-local HTTP binds |
| `--data-dir` | auto | Data directory override |
| `--check` | flag | Validate config/DB/FS/upstreams and exit |

`--auth-token` also reads `SIFT_MCP_AUTH_TOKEN`.

Runtime `data_dir` resolution order:

1. `--data-dir` (if provided)
2. `SIFT_MCP_DATA_DIR` environment variable
3. default `.sift-mcp`

## URL mode security

| Bind address | Auth required |
|--------------|---------------|
| `127.0.0.1`, `localhost`, `::1` | No |
| Any other (for example `0.0.0.0`) | Yes (`--auth-token` or `SIFT_MCP_AUTH_TOKEN`) |

## Constants

| Constant | Value |
|----------|-------|
| `WORKSPACE_ID` | `"local"` |
| `CANONICALIZER_VERSION` | `"jcs_rfc8785_v1"` |
| `MAPPER_VERSION` | `"mapper_v1"` |
| `TRAVERSAL_CONTRACT_VERSION` | `"traversal_v1"` |
| `CURSOR_VERSION` | `"cursor_v1"` |
| `PRNG_VERSION` | `"prng_xoshiro256ss_v1"` |
