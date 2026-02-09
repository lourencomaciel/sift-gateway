# MCP Artifact Gateway — Design Specification v1.9

> **Status**: Locked — this document is the authoritative reference for the v1.9 implementation.

## 1. Overview

The MCP Artifact Gateway is a local, single-tenant MCP proxy that:

1. Discovers tools exposed by upstream MCP servers (stdio or HTTP transport).
2. Mirrors each tool as `{prefix}.{tool}` with identical schema — no injected fields.
3. Intercepts every tool call, forwards it upstream, and wraps the result in a **durable artifact envelope** stored to Postgres and the local filesystem.
4. Returns the result to the caller: small payloads are returned raw (**passthrough mode**); large payloads return a compact **handle** (artifact ID + cache metadata).
5. Generates a **deterministic inventory** (full or partial schema mapping) for each artifact's JSON payload.
6. Provides bounded, deterministic **retrieval** tools (`artifact.get`, `artifact.select`, `artifact.describe`, `artifact.find`, `artifact.search`, `artifact.chain_pages`) with signed cursor pagination.

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
- **Payload fields**: cursor_version, traversal_contract_version, workspace_id, artifact_id, tool, where_canonicalization_mode, mapper_version, position_state, issued_at, expires_at, optional sample_set_hash
- **Binding checks on resume**: tool, artifact_id, workspace_id, traversal_contract_version, mapper_version, where_canonicalization_mode (if enabled)
- **Staleness triggers**: any binding mismatch, sample_set_hash mismatch, version increments
- **TTL**: configurable `cursor_ttl_minutes` (default 60); expired cursors raise `CursorExpiredError`
- **Secret rotation**: multiple active versions; newest signs, all active verify

## 9. Tool surface

### Gateway tools

| Tool | Purpose |
|------|---------|
| `gateway.status` | Health, versions, budgets, connectivity |
| `artifact.search` | List artifacts via `artifact_refs` (no touch on `last_referenced_at`) |
| `artifact.get` | Retrieve envelope or mapped data with JSONPath; touches `last_referenced_at` |
| `artifact.select` | Select paths from mapped roots; partial mode returns sampled-only subset |
| `artifact.describe` | Mapping metadata with partial mapping disclosures |
| `artifact.find` | Search within mapped data; sample-only unless indexed |
| `artifact.chain_pages` | Paginate chain sequences ordered by `chain_seq ASC, created_seq ASC` |

### Response shape

All retrieval tools return: `{items, truncated, cursor, omitted, stats}`.

## 10. Session tracking and touch policy

- **Creation**: touches `artifacts.last_referenced_at`
- **Retrieval/describe**: touches if not deleted; else returns GONE
- **Search**: does NOT touch `last_referenced_at` (only session/artifact_refs)

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
| >= `passthrough_max_bytes` | **handle-only** | `artifact_id` + cache metadata | Sync | Yes |

### 14.1 Passthrough mode

When the normalized envelope payload is smaller than `passthrough_max_bytes`, the gateway returns the raw upstream MCP response directly to the caller. From the LLM's perspective the gateway is invisible — the response looks identical to calling the upstream server without the gateway in the path.

- **Size threshold**: Configurable globally via `passthrough_max_bytes` (default 8192 bytes, `0` = passthrough disabled). Per-upstream opt-out via `passthrough_allowed = false`.
- **Async persist**: Passthrough results are still persisted (envelope + payload) for audit and durability, but persistence happens asynchronously and is best-effort. The caller does not wait for storage to complete.
- **No mapping**: The mapping pipeline (full or partial) is skipped entirely for passthrough results. Retrieval tools will not have mapping data for these artifacts until/unless a background re-map occurs.
- **Binary exclusion**: Responses containing binary refs (`binary_ref` content parts) never qualify for passthrough, regardless of payload size. Binary content always follows the handle-only path.

### 14.2 Handle-only mode

Payloads at or above the passthrough threshold follow the existing handle-only path: the envelope is stored synchronously, the mapping pipeline runs (full or partial depending on payload size), and the caller receives a compact handle containing the `artifact_id` and cache metadata. The LLM uses retrieval tools (`artifact.get`, `artifact.select`, etc.) to access the stored content.
