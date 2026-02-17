# Sift — Design Specification v1.9

> **Status**: Locked — this document is the authoritative reference for the v1.9 implementation.

## 1. Overview

Sift is a local, single-tenant MCP proxy that:

1. Discovers tools exposed by upstream MCP servers (stdio or HTTP transport).
2. Mirrors each tool as `{prefix}.{tool}` with identical schema — no injected fields.
3. Intercepts every tool call, forwards it upstream, and wraps the result in a **durable artifact envelope** stored to Postgres and the local filesystem.
4. Returns the result to the caller: small payloads are returned raw (**passthrough mode**); large payloads return a **handle** with artifact ID, inline schema-first metadata (`mapping` + `schemas`), and a usage hint.
5. Generates a **deterministic inventory** (full or partial schema mapping) for each artifact's JSON payload.
6. Provides a consolidated **`artifact`** retrieval tool with
   actions (`query`, `next_page`) using signed cursor pagination.
   Query behavior is explicit via `query_kind=describe|get|select|search|code`.
   For non-search kinds, `scope` defaults to `all_related`.

### Design invariants

- **Single workspace**: `WORKSPACE_ID = "local"` — all PKs include it.
- **Determinism**: identical inputs produce identical mappings, traversals, and cursors.
- **Bounded responses**: every retrieval tool enforces item, byte, compute, and wildcard caps.
- **Crash safety**: filesystem writes are atomic (temp → fsync → rename); DB writes use transactions.
- **Always store**: even upstream errors produce an error-envelope artifact.

## 2. Storage model

### 2.1 Filesystem (content-addressed)

```
DATA_DIR/
  blobs/bin/<ab>/<cd>/<sha256_hex>   # binary blobs (images, oversized JSON, etc.)
  resources/                          # internal resource copies
  state/config.json                   # persisted config
  state/secrets.json                  # cursor signing secrets
  tmp/                                # atomic write staging
  logs/                               # structured JSON logs
```

Binary blobs are content-addressed by `sha256(raw_bytes)`. Writes are atomic: write to `tmp/`, fsync, rename into final path.

### 2.2 Postgres schema

Core tables (all PKs include `workspace_id`):

| Table | Purpose |
|-------|---------|
| `sessions` | Client session tracking with `last_seen_at` |
| `binary_blobs` | Registry of content-addressed binary files |
| `payload_blobs` | Compressed canonical envelope bytes + JSONB projection |
| `payload_hash_aliases` | Deduplication aliases mapping `alias_hash` → `payload_hash_full` |
| `payload_binary_refs` | Links payloads to their binary blob dependencies |
| `artifacts` | Artifact metadata: request identity, mapping state, lifecycle |
| `artifact_refs` | Per-session artifact visibility (first/last seen) |
| `artifact_roots` | Mapping roots discovered by full/partial mapping |

Ordering is always by `created_seq DESC` (identity column).

## 3. Envelope normalization

Upstream MCP responses are normalized into a canonical envelope shape:

- **Content parts**: `json`, `text`, `resource_ref`, `binary_ref`
- **Error block**: present iff `ok = false`
- **Oversized JSON rule**: if a JSON part exceeds `max_json_part_parse_bytes`, it is stored as a `binary_ref` with `mime = application/json` and a warning is recorded in `meta.warnings`
- Raw binary bytes are never stored in the envelope — always as `binary_ref`

## 4. Canonical JSON and hashing

- **Canonicalization**: RFC 8785 (JCS) — deterministic key ordering, UTF-8, number formatting
- **Decimal-safe parsing**: JSON floats parsed as `Decimal`, NaN/Infinity rejected
- **Payload hash**: `payload_hash_full = sha256(uncompressed_canonical_bytes)`
- **Compression**: canonical bytes stored with `zstd` (default), `gzip`, or `none`

## 5. Request identity and caching

- `upstream_instance_id`: semantic identity of the upstream (excludes secrets)
- `canonical_args_bytes`: RFC 8785 canonical JSON of stripped/validated args
- `request_key = sha256(upstream_instance_id | prefix | tool | canonical_args_bytes)`
- **Reserved arg stripping**: keys matching `_gateway_*` are removed before hashing/forwarding
- **Stampede lock**: `pg_advisory_lock` derived from `sha256(request_key)` with configurable timeout
- **Reuse**: by `request_key` (latest by `created_seq DESC`), gated by schema hash if `strict_schema_reuse`

Reuse control via `_gateway_context.allow_reuse` (boolean):

- `false` (default): every upstream call is fresh — no request-key dedup
- `true`: opt-in to request-key dedup; advisory lock + reuse check active

## 6. Mapping system

### 6.1 Full mapping

For payloads ≤ `max_full_map_bytes`: parse fully, discover up to K roots (K=3), build deterministic inventory, write `artifact_roots`.

### 6.2 Partial mapping (streaming, deterministic)

For large or byte-backed payloads:

- Streaming via ijson — budgets enforced (bytes, compute steps, depth, records, record size, leaf paths)
- **Deterministic reservoir sampling**: seed = `sha256(payload_hash_full | root_path | map_budget_fingerprint)`
- Oversize sampled elements skipped and counted (bias invariant)
- `sample_indices` stored sorted ascending, only materialized indices included
- `count_estimate` only when `stop_reason = none` and closing array observed
- If `stop_reason != none`: prefix coverage = true, count_estimate = null
- **Fingerprint**: `map_budget_fingerprint = sha256(budgets + versions)` — changes invalidate cursors

### 6.3 Worker safety

Conditional update: `deleted_at IS NULL AND map_status IN (pending, stale) AND generation = snapshot_generation`. Otherwise discard.

## 7. Retrieval and traversal contract

- **Version**: `traversal_v1`
- **Arrays**: ascending index order
- **Objects**: lexicographic key order
- **Wildcards**: follow container type rules
- **Partial mode**: sampled indices enumerated ascending
- **JSONPath subset**: `$`, `.name`, `['..']`, `[n]`, `[*]` — caps on length, segments, wildcard expansion
- **select_paths**: normalized, deduplicated, sorted lexicographically; `select_paths_hash = sha256(canonical_bytes(sorted_paths))`

## 8. Cursor contract

- **Format**: `cur.<version>.<payload_b64u>.<signature_b64u>`
- **Signing**: HMAC-SHA256 over RFC 8785 canonical payload bytes
- **Payload fields**: cursor_version, traversal_contract_version, workspace_id, artifact_id, tool, where_canonicalization_mode, mapper_version, position_state, issued_at, expires_at; retrieval bindings (for example target/jsonpath/select hashes); optional sample_set_hash; and for lineage-scoped queries `scope`, `anchor_artifact_id`, `related_set_hash`
- **Binding checks on resume**: tool, artifact_id, workspace_id, traversal_contract_version, mapper_version, where_canonicalization_mode (if enabled)
- **Staleness triggers**: any binding mismatch, sample_set_hash mismatch, `related_set_hash` mismatch, version increments
- **TTL**: configurable `cursor_ttl_minutes` (default 60); expired cursors raise `CursorExpiredError`
- **Secret rotation**: multiple active versions; newest signs, all active verify

## 9. Tool surface

### Gateway tools

| Tool | Purpose |
|------|---------|
| `gateway_status` | Health, versions, budgets, connectivity |
| `artifact` | Consolidated retrieval tool with `action` parameter |

#### `artifact` actions

| Action | Purpose |
|--------|---------|
| `query` | Retrieval/search entrypoint; requires explicit `query_kind` |
| `next_page` | Fetch next upstream page using stored pagination state |

#### `query_kind` (required when `action="query"`)

| `query_kind` | Purpose |
|--------------|---------|
| `describe` | Lineage-aware root catalog and compatibility summary |
| `get` | Retrieve envelope/jsonpath values or mapped root catalog |
| `select` | Project/filter records from mapped roots |
| `search` | Session-scoped artifact listing (`artifact_refs`) |
| `code` | Execute generated Python over lineage-merged root records |

#### Scope model

- `scope` applies to `query_kind=describe|get|select` and defaults to `all_related`.
- `all_related` resolves the full visible lineage component (anchor + ancestors + descendants).
- `single` restricts execution to the anchor artifact only.
- `query_kind=search` does not accept `artifact_id` or `scope`.
- `query_kind=code` always executes with all-related lineage semantics; provided `scope` is ignored.

#### `query_kind=code` execution contract

- Required args: `root_path`, `code`, and either `artifact_id` (single artifact) or `artifact_ids` (multi-artifact).
- Optional args: `params` (object).
- Disallowed args: `target`, `jsonpath`, `select_paths`, `where`, `order_by`, `distinct`, `count_only`, `filters`.
- Runtime entrypoint supports:
  - `run(data, schema, params)` for single-artifact queries.
  - `run(artifacts, schemas, params)` for multi-artifact queries.
- Input records are root-scoped and lineage-merged. Dict records include injected `_locator`; scalar/list records are wrapped as `{\"_locator\": ..., \"value\": ...}`.
- For multi-artifact queries, `artifacts` and `schemas` are dictionaries keyed by requested artifact id.
- Return contract: any JSON-serializable value; non-list values are normalized to a single-item list.
- Runtime uses subprocess isolation with deterministic env (`PYTHONHASHSEED=0`, `TZ=UTC`), timeout, memory cap, and input-size guards.
- Default import roots include stdlib helpers (`math`, `statistics`, `decimal`, `datetime`, `re`, `itertools`, `collections`, `functools`, `operator`, `heapq`, `json`, `csv`, `io`, `string`, `textwrap`, `copy`, `typing`, `dataclasses`, `enum`, `fractions`, `bisect`, `pprint`, `uuid`, `base64`, `struct`, `array`, `numbers`, `cmath`, `random`, `secrets`, `fnmatch`, `difflib`, `html`, `urllib`) plus `jmespath`, `pandas`, and `numpy`. The `io` module restricts file-backed classes; only `StringIO` and `BytesIO` are usable. The `urllib` module is restricted to `urllib.parse` only; `urllib.request` and other submodules are blocked.
- Import roots can be explicitly configured via `code_query_allowed_import_roots`.
- Code query responses are unpaginated: all rows are returned in one response.
- Output is bounded only by `max_bytes_out`; oversize responses fail with `RESPONSE_TOO_LARGE`.
- Runtime failures may include `details.traceback` (truncated to 2000 chars) with line numbers.
- When sampled mapping data is included, responses set `sampled_only=true` and include explicit warnings.

#### Strict merge contract (why queries do not silently break)

- `select` computes a root signature per included artifact from: `root_path`, `root_shape`, canonicalized `fields_top` (keys/types), and `map_kind`.
- If multiple distinct signatures are present for the requested `root_path`, query fails fast with `INVALID_ARGUMENT` and details code `INCOMPATIBLE_LINEAGE_SCHEMA`.
- Artifacts missing the requested `root_path` are skipped (with warnings/counts), not treated as incompatible.
- Merged rows include provenance via `_locator.artifact_id`.

### Response shape

Retrieval tools return compatibility fields:
`{items, truncated, cursor, omitted, stats}` plus:

```json
{
  "pagination": {
    "layer": "artifact_retrieval",
    "retrieval_status": "PARTIAL|COMPLETE",
    "partial_reason": "CURSOR_AVAILABLE|null",
    "has_more": true,
    "next_cursor": "cur_..."
  }
}
```

Mirrored upstream tool responses include:

```json
{
  "pagination": {
    "layer": "upstream",
    "retrieval_status": "PARTIAL|COMPLETE",
    "partial_reason": "MORE_PAGES_AVAILABLE|SIGNAL_INCONCLUSIVE|CONFIG_MISSING|NEXT_TOKEN_MISSING|null",
    "has_more": true,
    "page_number": 0,
    "next_action": {
      "tool": "artifact",
      "arguments": {"action": "next_page", "artifact_id": "art_..."}
    },
    "warning": "INCOMPLETE_RESULT_SET|null",
    "has_next_page": true,
    "hint": "..."
  }
}
```

Completion semantics are fail-closed: do not claim full completeness
until `pagination.retrieval_status == "COMPLETE"`.

For `query_kind=code`, pagination fields are omitted (`truncated=false`, no cursor) because results are returned in a single response.

## 10. Session tracking and touch policy

- **Creation**: touches `artifacts.last_referenced_at`
- **Retrieval/describe**: touches if not deleted; else returns GONE
- **Search**: does NOT touch `last_referenced_at` (only session/artifact_refs)
- **Cache reuse invariant**: a reused handle MUST be attached to the caller
  session (`artifact_refs`) before returning. Returned handles are immediately
  retrievable in that session.

## 11. Pruning

- **Soft delete**: `SKIP LOCKED`, rechecks predicates, sets `deleted_at`, increments generation
- **Hard delete**: cascades through artifact_roots → artifact_refs → unreferenced payload_blobs → unreferenced binary_blobs → filesystem cleanup
- **Reconciler**: finds orphan filesystem blobs not referenced in DB

## 12. Observability

- **Logging**: structlog JSON with correlation fields (session_id, artifact_id, request_key, payload_hash_full)
- **Metrics**: cache hits, alias hits, upstream calls, oversize JSON count, partial map stop_reason distribution, cursor stale reasons, advisory lock timeouts
- **Determinism debug**: map_budget_fingerprint, map_backend_id, prng_version, sample_set_hash logged on cursor issue/verify

## 13. Version constants

| Constant | Value |
|----------|-------|
| `WORKSPACE_ID` | `local` |
| `CANONICALIZER_VERSION` | `jcs_rfc8785_v1` |
| `MAPPER_VERSION` | `mapper_v1` |
| `TRAVERSAL_CONTRACT_VERSION` | `traversal_v1` |
| `CURSOR_VERSION` | `cursor_v1` |
| `PRNG_VERSION` | `prng_xoshiro256ss_v1` |

## 14. Response model: passthrough vs handle-only

The gateway uses a two-tier response model based on payload size:

| Payload size | Mode | LLM sees | Storage | Mapping |
|---|---|---|---|---|
| < `passthrough_max_bytes` (default 8192) | **passthrough** | Raw upstream result (transparent) | Async, best-effort | Skipped |
| >= `passthrough_max_bytes` | **handle-only** | `artifact_id` + cache metadata + `mapping` + `schemas` + usage hint | Sync | Yes |

### 14.1 Passthrough mode

When the normalized envelope payload is smaller than `passthrough_max_bytes`, the gateway returns the raw upstream MCP response directly to the caller. From the LLM's perspective the gateway is invisible — the response looks identical to calling the upstream server without the gateway in the path.

- **Size threshold**: Configurable globally via `passthrough_max_bytes` (default 8192 bytes, `0` = passthrough disabled). Per-upstream opt-out via `passthrough_allowed = false`.
- **Async persist**: Passthrough results are still persisted (envelope + payload) for audit and durability, but persistence happens asynchronously and is best-effort. The caller does not wait for storage to complete.
- **No mapping**: The mapping pipeline (full or partial) is skipped entirely for passthrough results. Retrieval tools will not have mapping data for these artifacts until/unless a background re-map occurs.
- **Binary exclusion**: Responses containing binary refs (`binary_ref` content parts) never qualify for passthrough, regardless of payload size. Binary content always follows the handle-only path.

### 14.2 Handle-only mode

Payloads at or above the passthrough threshold follow the handle-only path: the envelope is stored synchronously, the mapping pipeline runs (full or partial depending on payload size), and the caller receives a response containing:

- **`artifact_id`** and **cache metadata** (reuse status, request key).
- cache metadata includes `reused`, `request_key`, `reason`,
  `artifact_id_origin` (`cache|fresh`), and `allow_reuse` (boolean).
- **`mapping`**: inline mapping metadata (`map_kind`, `map_status`, mapper/traversal versions, and determinism linkage).
- **`schemas`**: inline compact schema list (legend-driven key aliases) with per-field type/required/nullable counts, observed-count defaults, compact example values, sampled `distinct_values` (max 10), sampled `cardinality`, and determinism hashes.
  - Compaction rule: if exactly one schema root has the highest `coverage.observed_records`, only that primary schema is returned.
  - If highest coverage ties, all tied schemas are returned.
- **`schema_legend`**: one-time key map describing compact aliases used in `schemas`.
  - Key examples: `rp`=`root_path`, `f`=`fields`, `oc`=`observed_count`, `e`=`example_value`, `tr`=`example_truncated_chars`.
  - `field.oc` is omitted when equal to `schema.fd.oc` (field default).
  - Truncated examples use `e` + numeric `tr` instead of repeated prose (no repeated `"N more chars truncated"` strings).
- **`usage_hint`**: a heuristic natural language hint (no LLM) describing what the artifact contains, which fields are available, and which retrieval action to call next (`query_kind="select"` / `query_kind="code"` for arrays, `query_kind="get"` for dicts). For code queries, hints include dynamically detected third-party package availability for the current runtime policy/environment.

This eliminates the need for a separate describe call — the LLM can go directly from the tool response to `artifact(action="query", query_kind="select")`. `query_kind="describe"` remains available for explicit post-hoc lineage/compatibility inspection.

## 15. Artifact query references

Top-level string arguments in mirrored tool calls are inspected for
artifact references. Matched references are resolved server-side
before the arguments are forwarded to the upstream tool.

### 15.1 Syntax

| Pattern | Resolves to |
|---------|-------------|
| `art_<32hex>` | Full JSON/text payload |
| `art_<32hex>:$.path` | JSONPath query result |

Detection rules:

1. Match `^art_[0-9a-f]{32}` prefix.
2. If that is the full string: bare ref (resolve full payload).
3. If followed by `:$`: split on first `:`, parse remainder as JSONPath.
4. Anything else: not a reference, pass through unchanged.

Nested values (inside dicts or lists) are never inspected.

### 15.2 Resolution

For each detected reference:

1. Fetch the artifact envelope (reuses `FETCH_ARTIFACT_SQL`).
2. Validate: not deleted, not binary-only.
3. Extract JSON target via `extract_json_target`.
4. If JSONPath query present: evaluate via `evaluate_jsonpath`.
   - Empty match list → `ResolveError(NOT_FOUND)`.
   - Single match → return the scalar (unwrapped).
   - Multiple matches → return the list.
5. Substitute the resolved value into the argument dict.

### 15.3 Caching invariant

Request identity hashes use the pre-resolution arguments (pointer
strings). Same reference string = same cache key, regardless of
the artifact's current content.

### 15.4 DB-less mode

Resolution is skipped entirely when no database is configured
(no artifacts to resolve).

## 16. Workflow recipes

### 16.1 Large mirrored result retrieval

1. Call mirrored tool and receive `artifact_id`.
2. Use inline `schemas` to pick `root_path` (typically the single returned schema, or the highest-coverage schema in ties).
3. Call `artifact(action="query", query_kind="select")` with cursor paging.
4. Continue until `pagination.retrieval_status == "COMPLETE"`.

### 16.2 Upstream page chaining

1. Inspect mirrored response `pagination.layer = "upstream"`.
2. If `has_next_page`, call `artifact(action="next_page")` with current `artifact_id`.
3. Repeat until `pagination.retrieval_status == "COMPLETE"`.
