# Configuration Reference

> All configuration keys, their types, defaults, and environment variable mappings.

## Precedence

Environment variables (`SIDEPOUCH_MCP_*`) > `DATA_DIR/state/config.json` > compiled defaults.

---

## Filesystem

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `data_dir` | Path | `.sidepouch` | `SIDEPOUCH_MCP_DATA_DIR` | Root data directory |

**Derived paths** (not directly configurable):

| Path | Derivation |
|------|-----------|
| `state_dir` | `{data_dir}/state` |
| `resources_dir` | `{data_dir}/resources` |
| `blobs_bin_dir` | `{data_dir}/blobs/bin` |
| `tmp_dir` | `{data_dir}/tmp` |
| `logs_dir` | `{data_dir}/logs` |
| `secrets_path` | `{data_dir}/state/secrets.json` |
| `config_json_path` | `{data_dir}/state/config.json` |

## Database backend

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `db_backend` | enum | `sqlite` | `SIDEPOUCH_MCP_DB_BACKEND` | Database backend: `sqlite` (default, zero-config) or `postgres` |

The gateway supports two database backends:

- **SQLite** (default) — zero-dependency, stores data at `{state_dir}/gateway.db`. Suitable for local development and single-user deployments.
- **PostgreSQL** — production-grade, requires a running Postgres instance. Set `db_backend=postgres` and configure the DSN below.

## SQLite

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `sqlite_busy_timeout_ms` | int | 5000 | `SIDEPOUCH_MCP_SQLITE_BUSY_TIMEOUT_MS` | SQLite BUSY retry timeout (ms) |

**Derived paths:**

| Path | Derivation |
|------|-----------|
| `sqlite_path` | `{state_dir}/gateway.db` |

SQLite uses WAL mode for concurrent read access. Advisory locks are no-op (always acquired). `SKIP LOCKED` clauses are stripped automatically.

## Postgres

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `postgres_dsn` | str | `postgresql://localhost:5432/sidepouch` | `SIDEPOUCH_MCP_POSTGRES_DSN` | Connection string |
| `postgres_pool_min` | int | 2 | `SIDEPOUCH_MCP_POSTGRES_POOL_MIN` | Min pool connections |
| `postgres_pool_max` | int | 10 | `SIDEPOUCH_MCP_POSTGRES_POOL_MAX` | Max pool connections |
| `postgres_statement_timeout_ms` | int | 30000 | `SIDEPOUCH_MCP_POSTGRES_STATEMENT_TIMEOUT_MS` | Statement timeout (ms) |

## Envelope storage

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `envelope_jsonb_mode` | enum | `full` | `SIDEPOUCH_MCP_ENVELOPE_JSONB_MODE` | JSONB strategy: `full`, `minimal_for_large`, `none` |
| `envelope_jsonb_minimize_threshold_bytes` | int | 1000000 | `SIDEPOUCH_MCP_ENVELOPE_JSONB_MINIMIZE_THRESHOLD_BYTES` | Byte threshold for minimal JSONB |
| `envelope_canonical_encoding` | enum | `zstd` | `SIDEPOUCH_MCP_ENVELOPE_CANONICAL_ENCODING` | Compression: `zstd`, `gzip`, `none` |

## Ingest caps

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_inbound_request_bytes` | int | 10000000 | `SIDEPOUCH_MCP_MAX_INBOUND_REQUEST_BYTES` | Max request body (10 MB) |
| `max_upstream_error_capture_bytes` | int | 100000 | `SIDEPOUCH_MCP_MAX_UPSTREAM_ERROR_CAPTURE_BYTES` | Max error capture |
| `max_json_part_parse_bytes` | int | 50000000 | `SIDEPOUCH_MCP_MAX_JSON_PART_PARSE_BYTES` | Oversized JSON threshold (50 MB) |

## Storage caps

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_binary_blob_bytes` | int | 500000000 | `SIDEPOUCH_MCP_MAX_BINARY_BLOB_BYTES` | Max single binary blob (500 MB) |
| `max_payload_total_bytes` | int | 1000000000 | `SIDEPOUCH_MCP_MAX_PAYLOAD_TOTAL_BYTES` | Max total payload (1 GB) |
| `max_total_storage_bytes` | int | 10000000000 | `SIDEPOUCH_MCP_MAX_TOTAL_STORAGE_BYTES` | Max total storage (10 GB) |

## Full mapping

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_full_map_bytes` | int | 10000000 | `SIDEPOUCH_MCP_MAX_FULL_MAP_BYTES` | Full mapping size limit (10 MB) |
| `max_root_discovery_k` | int | 3 | `SIDEPOUCH_MCP_MAX_ROOT_DISCOVERY_K` | Max roots to discover |

## Partial mapping budgets

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_bytes_read_partial_map` | int | 50000000 | `SIDEPOUCH_MCP_MAX_BYTES_READ_PARTIAL_MAP` | Max bytes read (50 MB) |
| `max_compute_steps_partial_map` | int | 5000000 | `SIDEPOUCH_MCP_MAX_COMPUTE_STEPS_PARTIAL_MAP` | Max compute steps |
| `max_depth_partial_map` | int | 64 | `SIDEPOUCH_MCP_MAX_DEPTH_PARTIAL_MAP` | Max traversal depth |
| `max_records_sampled_partial` | int | 100 | `SIDEPOUCH_MCP_MAX_RECORDS_SAMPLED_PARTIAL` | Max sampled records |
| `max_record_bytes_partial` | int | 100000 | `SIDEPOUCH_MCP_MAX_RECORD_BYTES_PARTIAL` | Max bytes per record |
| `max_leaf_paths_partial` | int | 500 | `SIDEPOUCH_MCP_MAX_LEAF_PATHS_PARTIAL` | Max leaf paths |
| `max_root_discovery_depth` | int | 5 | `SIDEPOUCH_MCP_MAX_ROOT_DISCOVERY_DEPTH` | Max root discovery depth |

## Mapping mode

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `mapping_mode` | enum | `hybrid` | `SIDEPOUCH_MCP_MAPPING_MODE` | Execution: `async`, `hybrid`, `sync` |

## Retrieval budgets

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_items` | int | 1000 | `SIDEPOUCH_MCP_MAX_ITEMS` | Max items per response |
| `max_bytes_out` | int | 5000000 | `SIDEPOUCH_MCP_MAX_BYTES_OUT` | Max response bytes (5 MB) |
| `max_wildcards` | int | 10000 | `SIDEPOUCH_MCP_MAX_WILDCARDS` | Max wildcard expansions |
| `max_compute_steps` | int | 1000000 | `SIDEPOUCH_MCP_MAX_COMPUTE_STEPS` | Max retrieval compute steps |

## JSONPath

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `max_jsonpath_length` | int | 4096 | `SIDEPOUCH_MCP_MAX_JSONPATH_LENGTH` | Max JSONPath string length |
| `max_path_segments` | int | 64 | `SIDEPOUCH_MCP_MAX_PATH_SEGMENTS` | Max path segments (depth) |
| `max_wildcard_expansion_total` | int | 10000 | `SIDEPOUCH_MCP_MAX_WILDCARD_EXPANSION_TOTAL` | Max total wildcard results |

## Search

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `artifact_search_max_limit` | int | 200 | `SIDEPOUCH_MCP_ARTIFACT_SEARCH_MAX_LIMIT` | Max search limit parameter |

## Passthrough mode

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `passthrough_max_bytes` | int | 8192 | `SIDEPOUCH_MCP_PASSTHROUGH_MAX_BYTES` | Max payload bytes for passthrough (8 KB); `0` = disabled |

Results below this threshold are returned as raw upstream responses (gateway is transparent). Results at or above this threshold return handle-only. Binary responses never passthrough regardless of size. Passthrough results are persisted asynchronously for audit/durability. See also `passthrough_allowed` per-upstream.

## Cursor

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `cursor_ttl_minutes` | int | 60 | `SIDEPOUCH_MCP_CURSOR_TTL_MINUTES` | Cursor TTL in minutes |
| `where_canonicalization_mode` | enum | `raw_string` | `SIDEPOUCH_MCP_WHERE_CANONICALIZATION_MODE` | Where clause mode: `raw_string`, `canonical_ast` |

## Miscellaneous

| Key | Type | Default | Env var | Description |
|-----|------|---------|---------|-------------|
| `binary_probe_bytes` | int | 65536 | `SIDEPOUCH_MCP_BINARY_PROBE_BYTES` | Bytes to probe for binary detection (64 KB) |
| `select_missing_as_null` | bool | false | `SIDEPOUCH_MCP_SELECT_MISSING_AS_NULL` | Treat missing fields as null |
| `advisory_lock_timeout_ms` | int | 5000 | `SIDEPOUCH_MCP_ADVISORY_LOCK_TIMEOUT_MS` | Advisory lock timeout (ms) |

## Upstream configuration

Upstreams are configured as an array. Env var pattern: `SIDEPOUCH_MCP_UPSTREAMS__<INDEX>__<FIELD>`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `prefix` | str | (required) | Tool namespace prefix |
| `transport` | str | (required) | `stdio` or `http` |
| `command` | str | null | Command for stdio transport |
| `args` | list[str] | `[]` | CLI args for stdio |
| `env` | dict | `{}` | Env vars for stdio |
| `url` | str | null | URL for http transport |
| `headers` | dict | `{}` | HTTP headers |
| `semantic_salt_headers` | list[str] | `[]` | Non-secret headers for upstream identity |
| `semantic_salt_env_keys` | list[str] | `[]` | Env keys affecting upstream identity |
| `strict_schema_reuse` | bool | true | Require schema hash match for reuse |
| `passthrough_allowed` | bool | true | Allow passthrough mode for this upstream |
| `dedupe_exclusions` | list[str] | `[]` | JSONPath exclusions for dedupe hash |

### mcpServers format

The gateway also accepts the standard `mcpServers` dict format (Claude Desktop, Cursor, Claude Code):

```json
{
  "mcpServers": {
    "github": {
      "command": "/usr/local/bin/mcp-github",
      "args": ["--config", "github.json"],
      "env": {}
    }
  }
}
```

Transport is inferred: `command` present → stdio, `url` present → http. Gateway-specific extensions go in a `_gateway` namespace within each server entry.

Cannot mix `mcpServers` and legacy `upstreams` in the same config.

## Constants

| Constant | Value |
|----------|-------|
| `WORKSPACE_ID` | `"local"` |
| `CANONICALIZER_VERSION` | `"jcs_rfc8785_v1"` |
| `MAPPER_VERSION` | `"mapper_v1"` |
| `TRAVERSAL_CONTRACT_VERSION` | `"traversal_v1"` |
| `CURSOR_VERSION` | `"cursor_v1"` |
| `PRNG_VERSION` | `"prng_xoshiro256ss_v1"` |
