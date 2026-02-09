# Task Plan: MCP Artifact Gateway (Python) Full Implementation v1.9 (Local Single-Tenant)

## Goal
Build a complete, production-grade, local single-tenant MCP gateway in Python that proxies upstream MCP tools, stores every tool result as a durable artifact envelope (disk + Postgres), returns compact handles, generates deterministic inventories, and enforces bounded deterministic retrieval with signed cursors.

## Current Phase
Phase 16: Documentation + Packaging (unit + integration tests complete)

## Phase Roadmap (High-Level)
- [ ] Phase 1: Repo skeleton, entrypoint, boot sequence
- [x] Phase 2: Config, constants, lifecycle
- [x] Phase 3: Postgres schema + migrations
- [x] Phase 4: Filesystem blob store + resource store
- [x] Phase 5: Canonicalization + hashing + compression
- [x] Phase 6: Envelope normalization + payload storage
- [x] Phase 7: Upstream discovery + mirroring + artifact creation
- [x] Phase 8: Mapping system (full + partial)
- [x] Phase 9: Query + traversal + retrieval core
- [x] Phase 10: Cursor signing + binding + staleness
- [x] Phase 11: MCP tool surface
- [x] Phase 12: Session tracking + touch policy
- [x] Phase 13: Pruning + retention + cleanup
- [x] Phase 14: Observability + metrics
- [x] Phase 15: Test suite + done gates
- [ ] Phase 16: Documentation + packaging (README updated, docker-compose improved)

## Completion Checklists (Spec v1.9)
These are copied from the spec checklists. Mark these items complete as implementation progresses.

## Implementation checklist:

Below is a completion-grade implementation checklist for MCP Artifact Gateway (Python) Full Implementation Spec v1.9 Local Single-Tenant. It is written as everything that must exist in code plus everything that must be demonstrably true at runtime, mapped directly to the v1.9 spec.

## 1) Repo shape and bootability

- [x] A single runnable entrypoint exists (for example `python -m mcp_gateway` or `mcp-gateway serve`) that:
  - [x] Loads config.
  - [x] Validates filesystem paths.
  - [x] Connects Postgres and validates migrations.
  - [x] Connects to every configured upstream (stdio and http).
  - [x] Discovers upstream tool lists.
  - [x] Starts an MCP server exposing:
    - [x] mirrored tools `{prefix}.{tool}`
    - [x] retrieval tools (`gateway.status`, `artifact.search`, `artifact.get`, `artifact.select`, `artifact.describe`, `artifact.find`, `artifact.chain_pages`)
- [x] A fail fast startup mode exists:
  - [x] If DB is unreachable or migrations missing: server does not start (or starts with gateway unhealthy and refuses mirrored calls with `INTERNAL`).
  - [x] If DATA_DIR or required subdirs cannot be created/written: server does not start (or starts unhealthy and refuses mirrored calls with `INTERNAL`).
- [x] A clear module boundary exists (names are illustrative, not mandatory):
  - [x] `config/` (schema + loader + defaults)
  - [x] `db/` (psycopg3 pool, migrations, queries)
  - [x] `fs/` (DATA_DIR layout, atomic writes, blob paths, resource copies)
  - [x] `canonical/` (RFC 8785 canonicalizer + hashing + compression)
  - [x] `upstream/` (clients for stdio/http MCP, discovery, schema parsing) → `mcp/upstream.py`
  - [x] `gateway/` (request handling, reserved arg stripping, reuse logic, artifact creation) → `artifacts/`, `cache/`, `mcp/mirror.py`
  - [x] `retrieval/` (jsonpath evaluation, select/projection, cursor handling)
  - [x] `mapping/` (full mapping, partial mapping, worker, sampling)
  - [x] `prune/` (soft delete, hard delete, blob cleanup, reconciliation) → `jobs/`
  - [x] `tests/` (unit + integration)

---

## 2) Configuration and constants

- [x] A config model exists that includes (at minimum):
  - [x] `DATA_DIR` and derived directories (`tmp/`, `logs/`, `state/`, `resources/`, `blobs/bin/...`)
  - [x] Postgres DSN + pool sizing + statement timeouts
  - [x] Upstream definitions:
    - [x] `prefix`, `transport` (http/stdio), endpoint config, semantic salts
    - [x] optional tool-level dedupe exclusions (JSONPath subset)
    - [x] tool-level reuse gating (strict schema hash match default)
    - [x] tool-level inline eligibility (default allow)
  - [x] Envelope storage config:
    - [x] `envelope_jsonb_mode` = `full|minimal_for_large|none`
    - [x] `envelope_jsonb_minimize_threshold_bytes`
    - [x] canonical byte compression: `zstd|gzip|none`
  - [x] Hard limits / budgets:
    - [x] inbound request cap
    - [x] upstream error capture cap
    - [x] `max_json_part_parse_bytes` (oversized JSON becomes byte-backed)
    - [x] `max_full_map_bytes`
    - [x] partial map budgets: `max_bytes_read_partial_map`, `max_compute_steps_partial_map`, `max_depth_partial_map`, `max_records_sampled_partial`, `max_record_bytes_partial`, `max_leaf_paths_partial`, `max_root_discovery_depth`
    - [x] retrieval budgets: `max_items`, `max_bytes_out`, `max_wildcards`, `max_compute_steps`
    - [x] `artifact_search_max_limit`
    - [x] storage caps: `max_binary_blob_bytes`, `max_payload_total_bytes`, `max_total_storage_bytes`
  - [x] Cursor settings:
    - [x] cursor TTL minutes
    - [x] active secret versions + signing secret version
    - [x] cursor_version constant
  - [x] Version constants:
    - [x] `canonicalizer_version`
    - [x] `mapper_version`
    - [x] `traversal_contract_version`
    - [x] `where_canonicalization_mode` (`raw_string` default, `canonical_ast` optional)
    - [x] `prng_version` constant
- [x] The system hardcodes and enforces `workspace_id = "local"` everywhere (no multi-tenant behavior leaks in).

---

## 3) Database migrations and schema correctness

- [x] Migrations exist to create exactly the v1.9 tables and constraints:
  - [x] `sessions`
  - [x] `binary_blobs`
  - [x] `payload_blobs`
  - [x] `payload_hash_aliases`
  - [x] `payload_binary_refs`
  - [x] `artifacts`
  - [x] `artifact_refs`
  - [x] `artifact_roots`
  - [x] Addendum C table: `artifact_samples`
- [x] Every PK, FK, unique constraint, and index in the spec exists.
- [x] All relevant CHECK constraints exist:
  - [x] enum-like fields (`map_kind`, `map_status`, `index_status`, encoding values, non-negative sizes)
- [x] Migrations are idempotent and ordered; a fresh database can be brought to current schema in one command.
- [x] Advisory lock usage for request stampede exists (two 32-bit keys derived from `sha256(request_key)`), with timeout and metrics/logging.

---

## 4) Filesystem layout and durability

- [x] On startup, gateway ensures these directories exist under `DATA_DIR`:
  - [x] `state/`
  - [x] `resources/` (if internal copies enabled)
  - [x] `blobs/bin/`
  - [x] `tmp/`
  - [x] `logs/` (if used)
- [x] Binary storage is content-addressed:
  - [x] `binary_hash = sha256(raw_bytes).hexdigest()`
  - [x] `blob_id = "bin_" + binary_hash[:32]`
  - [x] Path = `BIN_DIR / h[0:2] / h[2:4] / binary_hash`
- [x] Atomic write procedure exists and is used:
  - [x] temp file in same directory
  - [x] fsync temp file
  - [x] atomic rename to final path
- [x] Existing blob handling exists:
  - [x] verifies size matches expected `byte_count`
  - [x] optional probe hashes supported and persisted (`probe_head_hash`, `probe_tail_hash`, `probe_bytes`)
- [x] Resource refs support two durabilities:
  - [x] `internal`: copy bytes into `DATA_DIR/resources/...` and require `content_hash`
  - [x] `external_ref`: do not copy; `content_hash` optional best effort

---

## 5) Canonicalization, hashing, compression, numeric safety

- [x] RFC 8785 canonical JSON implementation exists and is used for:
  - [x] forwarded args canonicalization
  - [x] upstream tool schema canonicalization
  - [x] envelope canonicalization
  - [x] cursor payload canonicalization
  - [x] record hashing in `artifact_samples`
- [x] Numeric parsing rules are enforced:
  - [x] floats parsed as Decimal (no Python float drift)
  - [x] NaN/Infinity rejected
  - [x] canonicalization never sees Python floats
- [x] Payload identity is correct:
  - [x] `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`
  - [x] `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling)
- [x] Canonical bytes storage works:
  - [x] `envelope_canonical_encoding` stored (`zstd|gzip|none`)
  - [x] `envelope_canonical_bytes` stored (compressed)
  - [x] `envelope_canonical_bytes_len` stored (uncompressed length)
- [x] Dedupe hash is implemented and explicitly does not define storage identity:
  - [x] tool-configured JSONPath exclusions apply only to dedupe computation
  - [x] alias table `payload_hash_aliases` is populated and used only for reuse lookup

## 5) Canonicalization, hashing, compression, numeric safety (duplicate section in spec)

- [x] RFC 8785 canonical JSON implementation exists and is used for:
  - [x] forwarded args canonicalization
  - [x] upstream tool schema canonicalization
  - [x] envelope canonicalization
  - [x] cursor payload canonicalization
  - [x] record hashing in `artifact_samples`
- [x] Numeric parsing rules are enforced:
  - [x] floats parsed as Decimal (no Python float drift)
  - [x] NaN/Infinity rejected
  - [x] canonicalization never sees Python floats
- [x] Payload identity is correct:
  - [x] `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`
  - [x] `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling)
- [x] Canonical bytes storage works:
  - [x] `envelope_canonical_encoding` stored (`zstd|gzip|none`)
  - [x] `envelope_canonical_bytes` stored (compressed)
  - [x] `envelope_canonical_bytes_len` stored (uncompressed length)
- [x] Dedupe hash is implemented and explicitly does not define storage identity:
  - [x] tool-configured JSONPath exclusions apply only to dedupe computation
  - [x] alias table `payload_hash_aliases` is populated and used only for reuse lookup

---

## 6) Upstream discovery, mirroring, and reserved arg stripping

- [x] Upstream tool discovery at startup:
  - [x] fetch tool list from each upstream
  - [x] expose mirrored tools as `{prefix}.{tool}` with identical schema/docs (no injected fields)
- [x] Reserved gateway args stripping is exact and tested:
  - [x] remove keys equal to `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq`
  - [x] remove any key whose name begins with exact prefix `_gateway_`
  - [x] remove nothing else (example: `gateway_url` must not be stripped)
- [x] Upstream instance identity exists and excludes secrets:
  - [x] `upstream_instance_id = sha256(canonical_semantic_identity_bytes)[:32]`
  - [x] includes transport + stable endpoint identity + prefix/name + optional semantic salt
  - [x] excludes rotating auth headers, tokens, secret env values, private key paths
  - [x] optional `upstream_auth_fingerprint` stored for debugging but excluded from request identity
- [x] `request_key` is computed exactly:
  - [x] based on `upstream_instance_id`, prefix, tool_name, canonical args bytes
  - [x] canonical args bytes computed after stripping reserved keys and validating against upstream schema
  - [x] `request_args_hash` and `request_args_prefix` persisted with caps

---

## 7) Artifact creation pipeline (mirrored tool calls)

For every mirrored tool call `{prefix}.{tool}(args)`:

- [x] Validate `_gateway_context.session_id` exists, else `INVALID_ARGUMENT`.
- [x] Determine `cache_mode` default `allow`.
- [x] Strip reserved gateway args (exact rules).
- [x] Validate forwarded args against upstream schema.
- [x] Canonicalize forwarded args and compute `request_key`.
- [x] Acquire advisory lock for stampede control (with timeout behavior).
- [x] Reuse behavior when `cache_mode != fresh`:
  - [x] request_key latest candidate chosen by `created_seq desc`
  - [x] optional dedupe alias reuse constrained to same `(upstream_instance_id, tool)`
  - [x] reuse requires:
    - [x] not deleted, not expired
    - [x] schema hash match if strict reuse enabled
  - [x] response indicates `meta.cache.reused=true` + reason + reused artifact id
- [ ] Call upstream tool, capture success or failure.
- [x] Normalize into envelope (always):
  - [x] status ok or error, with error shape present on error
  - [x] content parts support `json`, `text`, `resource_ref`, `binary_ref` (and alias `image_ref`)
  - [x] binary bytes never stored inline, only refs
- [x] Oversized JSON handling at ingest:
  - [x] if any JSON part size > `max_json_part_parse_bytes`:
    - [x] do not parse into structured value
    - [x] store raw bytes as `binary_ref` with JSON mime (and encoding)
    - [x] replace JSON content entry with `binary_ref` descriptor
    - [x] add warning in `meta.warnings` with original part index + encoding
- [x] Produce canonical envelope bytes, compute payload hashes, compress canonical bytes.
- [x] Insert or upsert `payload_blobs`.
- [x] Insert `binary_blobs` and `payload_binary_refs` for every blob reference.
- [x] Insert optional `payload_hash_aliases` rows (dedupe).
- [x] Insert `artifacts` row with:
  - [x] monotonic `created_seq`
  - [x] mapping fields: `map_kind='none'`, `map_status='pending'` initially, `mapper_version` set
  - [x] `index_status='off'` unless enabled
  - [x] sizes persisted (`payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`)
  - [x] `last_referenced_at=now()`
- [x] Update session tracking:
  - [x] `sessions` upsert with `last_seen_at=now()`
  - [x] `artifact_refs` upsert for `(session_id, artifact_id)`
- [x] Return contract (Addendum A):
  - [x] returns a handle-only result by default
  - [x] returns handle+inline envelope only when thresholds satisfied and policy allows
  - [ ] if gateway itself unhealthy (DB/fs), return `INTERNAL` and do not claim artifact creation

---

## 8) Gateway tool response contracts

- [x] All tool responses follow one of:
  - [x] `gateway_tool_result` for success returns (mirrored tools)
  - [x] uniform `gateway_error` for failures (all tools)
- [x] Handle includes required metadata:
  - [x] ids, created_seq, session_id, tool ids, hash ids, byte sizes, mapping/index status, contains_binary_refs, status
- [x] Warnings propagation works:
  - [x] warnings in response include gateway warnings inserted into envelope meta
- [x] Error codes are implemented and used correctly:
  - [x] `INVALID_ARGUMENT`, `NOT_FOUND`, `GONE`, `INTERNAL`, `CURSOR_INVALID`, `CURSOR_EXPIRED`, `CURSOR_STALE`, `BUDGET_EXCEEDED`, `UNSUPPORTED`

---

## 9) Retrieval tools and deterministic bounds

### 9.1 gateway.status

- [x] `gateway.status()` returns:
  - [x] upstream connectivity snapshot
  - [x] DB ok / migrations ok
  - [x] filesystem paths ok
  - [x] version constants: canonicalizer, mapper, traversal contract, cursor version
  - [x] where canonicalization mode
  - [x] partial mapping backend id + prng version
  - [x] all configured limits/budgets
  - [x] cursor TTL and active secret versions

### 9.2 artifact.search

- [x] Requires `_gateway_context.session_id`.
- [x] Reads only from `artifact_refs` for that session (discovery uses refs exclusively).
- [x] Filters implemented (Addendum B), including:
  - [x] include_deleted, status, source_tool_prefix, source_tool, upstream_instance_id, request_key, payload_hash_full, parent_artifact_id, has_binary_refs, created_seq range, created_at range
- [x] Ordering implemented:
  - [x] `created_seq_desc` default
  - [x] `last_seen_desc` optional
- [x] Search touches only:
  - [x] `sessions.last_seen_at`
  - [x] `artifact_refs.last_seen_at`
  - [x] does not touch `artifacts.last_referenced_at`
- [x] Pagination cursor for search exists and is bound to session_id + order_by + last position.

### 9.3 JSONPath subset support

- [x] JSONPath subset grammar implemented exactly:
  - [x] `$`, `.name`, `['...']` with escapes, `[n]`, `[*]`
  - [x] no filters
- [x] Caps enforced:
  - [x] max path length
  - [x] max segments
  - [x] max wildcard expansion total
- [x] Deterministic traversal contract implemented:
  - [x] arrays ascend
  - [x] objects key lex ascend
  - [x] wildcard expansions obey above
  - [x] partial sample enumeration uses ascending sample indices

### 9.4 artifact.get

- [x] Requires session_id.
- [x] Supports:
  - [x] `target=envelope` (jsonpath evaluated on envelope root)
  - [x] `target=mapped` (requires map_status ready and map_kind full/partial)
- [x] If envelope jsonb is minimized or none:
  - [x] reconstruct by parsing canonical bytes, within compute budgets
- [x] Bounded deterministic output:
  - [x] max_bytes_out / max_items / max_compute enforced
  - [x] deterministic truncation emits `truncated=true` + cursor + omitted metadata
- [x] Touch semantics:
  - [x] if not deleted: touches `artifacts.last_referenced_at`
  - [x] always updates `artifact_refs.last_seen_at` and `sessions.last_seen_at`
  - [x] if deleted: returns `GONE`

### 9.5 artifact.describe

- [x] Returns:
  - [x] mapping status/kind + mapper_version
  - [x] roots inventory + fields_top
  - [x] partial mapping fields: sampled-only, prefix coverage indicator, stop_reason, sampled_prefix_len, sampled_record_count, skipped_oversize_records
  - [x] count_estimate only when known under the stated rules
- [x] Touch semantics same as retrieval.

### 9.6 artifact.select

- [x] Inputs:
  - [x] artifact_id, root_path, select_paths (set semantics), where optional, limits, cursor
- [x] select_paths canonicalization implemented:
  - [x] whitespace removal, canonical escaping/quotes
  - [x] relative paths must not start with `$`
  - [x] sorted lexicographically, duplicates removed
  - [x] `select_paths_hash = sha256(canonical_json(array))`
- [x] where hashing implemented per server mode:
  - [x] raw_string default: exact UTF-8 bytes
  - [ ] canonical_ast optional: parse + canonicalize
  - [x] server reports mode in `gateway.status`, cursor binds to it
- [x] Full mapping behavior:
  - [x] bounded scan in deterministic order with cursor continuation
- [x] Partial mapping behavior:
  - [x] sampled-only scan:
    - [x] enumerate sample indices ascending
    - [x] evaluate where and select_paths only on sampled records
    - [x] returns `sampled_only=true`, `sample_indices_used`, `sampled_prefix_len`
- [x] Output projection contract (Addendum F):
  - [x] each item has `_locator` and `projection`
  - [x] projection keys are canonicalized select paths, emitted in lex order
  - [x] missing path behavior respects config `select_missing_as_null`

### 9.7 artifact.find

- [x] Works in sample-only mode unless indexing is enabled.
- [x] Deterministic output and bounded truncation with cursor.

### 9.8 artifact.chain_pages

- [x] Chain ordering is correct:
  - [x] `chain_seq asc`, then `created_seq asc`
- [x] Chain seq allocation exists when not provided, with retry and uniqueness constraint.

---

## 10) Mapping implementation (full + partial) and worker safety

### 10.1 Mapping scheduler

- [x] mapping_mode implemented: `async|hybrid|sync` (default hybrid)
- [x] Artifacts created with map_status pending cause mapping work to be scheduled.

### 10.2 JSON part selection scoring

- [x] Deterministic scoring implemented; tie-break by part index ascending.
- [x] Stores `mapped_part_index` on artifact.

### 10.3 Full mapping

When selected JSON part is <= max_full_map_bytes:

- [x] parse fully
- [x] discover up to K roots (K=3)
- [x] build deterministic inventory:
  - [x] roots entries written to `artifact_roots`
  - [x] `map_kind=full`, `map_status=ready`

### 10.4 Partial mapping trigger

- [x] If JSON part too large OR stored as `binary_ref application/json(+encoding)`:
  - [x] partial mapping runs
  - [x] `map_kind=partial`

### 10.5 Partial mapping core requirements

- [x] Byte-backed streaming input supported:
  - [x] from JSON binary blob (required when oversized at ingest)
  - [x] from text JSON (bounded)
  - [x] from re-canonicalized bytes for small structured values (bounded)
- [x] Budgets enforced during streaming:
  - [x] max bytes read
  - [x] max compute steps (stream events)
  - [x] max depth
  - [x] max sampled records N
  - [x] max per-record bytes
  - [x] max leaf paths
  - [x] root discovery depth cap
- [x] stop_reason tracked:
  - [x] none | max_bytes | max_compute | max_depth | parse_error
- [x] Prefix coverage semantics enforced:
  - [x] if stop_reason != none:
    - [x] count_estimate is null
    - [x] root_shape.prefix_coverage=true
    - [x] inventory coverage computed vs prefix
- [x] map_backend_id and prng_version:
  - [x] map_backend_id computed exactly from python version + ijson backend name + version
  - [x] prng_version is a code constant
  - [x] both returned by status and stored on artifacts
- [x] map_budget_fingerprint computed and stored:
  - [x] includes mapper version, traversal contract, backend id, prng version, all budgets
  - [x] if changes, previous partial mapping marked stale and cursors become stale
- [x] root_path normalization:
  - [x] absolute path starting with `$`
  - [x] uses `.name` when identifier is valid; otherwise bracket form with canonical escaping
  - [x] no wildcards
  - [x] format change requires traversal_contract_version bump (enforced as policy)
- [x] streaming skip contract implemented:
  - [x] ability to skip unselected subtrees without building full trees
  - [x] compute steps count all events processed, including skipped

### 10.6 Deterministic reservoir sampling

- [x] Reservoir sampling is one-pass and prefix-bounded:
  - [x] seed = sha256(payload_hash_full + "|" + root_path + "|" + map_budget_fingerprint)
  - [x] PRNG deterministic and versioned
  - [x] selected indices maintained uniformly over processed prefix indices
- [x] Bias invariant is explicit and implemented:
  - [x] oversize/depth-violating records are skipped and counted
  - [x] sample_indices include only successfully materialized records
- [x] sampled_prefix_len is computed correctly:
  - [x] counts element boundaries successfully recognized, including skipped/non-materialized
  - [x] parse_error mid-element uses last fully recognized index + 1
- [x] count_estimate rules enforced:
  - [x] set only if stop_reason==none AND array close observed

### 10.7 Persisted samples (Addendum C)

- [x] `artifact_samples` table is used for partial samples:
  - [x] one row per materialized sampled record index
  - [x] record hash stored as sha256(RFC8785(record))
- [x] `artifact_roots.sample_indices` exactly matches sample indices present in `artifact_samples` (sorted).
- [x] Updates are atomic:
  - [x] replace sample rows + sample_indices within a transaction per `(artifact_id, root_key)`
- [x] Partial retrieval depends on artifact_samples:
  - [x] `artifact.select` loads records from artifact_samples
  - [x] corruption detection: if indices exist but sample rows missing -> `INTERNAL` with details

### 10.8 Worker safety and races

- [x] Worker writes are conditional:
  - [x] artifact not deleted
  - [x] map_status in (pending, stale)
  - [x] generation matches
- [x] If conditional update affects 0 rows, worker discards results.
- [x] map_error stored on failure with enough detail to debug.

---

## 11) Cursor system (signing, binding, staleness)

- [x] Cursor format implemented:
  - [x] `base64url(payload_bytes) + "." + base64url(hmac)`
  - [x] unpadded base64url
- [x] Cursor payload canonicalization is RFC 8785 (Addendum D):
  - [x] signature input is exactly the canonical payload bytes
- [x] Secrets stored at `DATA_DIR/state/secrets.json` with:
  - [x] `cursor_ttl_minutes`
  - [x] `active_secrets[]` version + b64 key
  - [x] `signing_secret_version` present in active list
  - [x] keys >= 32 random bytes
- [x] Verification logic:
  - [x] parse payload bytes
  - [x] expiration check -> `CURSOR_EXPIRED`
  - [x] secret version missing -> `CURSOR_INVALID`
  - [x] constant-time HMAC compare -> else `CURSOR_INVALID`
  - [x] binding checks enforce `CURSOR_STALE` for:
    - [x] where_canonicalization_mode mismatch
    - [x] traversal_contract_version mismatch
    - [x] artifact_generation mismatch
    - [x] partial sample_set_hash mismatch
    - [x] partial map_budget_fingerprint mismatch
- [x] Cursor binding fields exist exactly per tool:
  - [x] get binds target + normalized_jsonpath
  - [x] select binds root_path + select_paths_hash + where_hash
- [x] Partial mode cursor binding includes:
  - [x] map_budget_fingerprint (required)
  - [x] sample_set_hash computed from DB sample indices and compared

---

## 12) where DSL implementation (Addendum E)

- [x] Parser exists for the specified grammar (OR/AND/NOT, parentheses, comparisons).
- [x] Relative path evaluation uses JSONPath subset (must not start with `$`).
- [x] Missing path semantics implemented:
  - [x] comparisons false except special `!= null` semantics (as defined)
- [x] Wildcard semantics:
  - [x] existential: any match satisfies
  - [x] bounded by max wildcard expansion
- [x] Type semantics implemented exactly:
  - [x] numeric comparisons require numeric operands
  - [x] string comparisons lexicographic by codepoint
  - [x] boolean only supports = and !=
- [x] Compute accounting exists and is deterministic:
  - [x] increments per path segment and expansions and comparison op
  - [x] deterministic short-circuiting

---

## 13) Retention, pruning, and cleanup correctness

- [x] Touch policy implemented exactly:
  - [x] creation touches `artifacts.last_referenced_at`
  - [x] retrieval/describe touches if not deleted
  - [x] search does not touch last_referenced_at
- [x] Soft delete job exists:
  - [x] selects with SKIP LOCKED
  - [x] predicate rechecked on update
  - [x] sets deleted_at and increments generation
  - [x] does not remove payloads yet
- [x] Hard delete job exists:
  - [x] deletes eligible artifacts
  - [x] cascades remove `artifact_roots`, `artifact_refs`, `artifact_samples`
  - [x] deletes unreferenced `payload_blobs`
  - [x] cascades remove `payload_binary_refs`
  - [x] deletes `binary_blobs` unreferenced by payload_binary_refs
  - [x] removes corresponding filesystem blob files
  - [x] optional reconciliation: detects orphan files on disk and can report/remove
- [ ] Quota enforcement exists:
  - [ ] storage cap breach triggers prune behavior (as configured)

---

## 14) Indexing (even if off by default)

- [x] Code supports `index_status` lifecycle:
  - [x] off | pending | ready | partial | failed
- [x] `artifact.find` respects sample-only unless index enabled rule.
- [x] If indexing is truly out of project scope for now, code still must:
  - [x] store `index_status` fields
  - [x] return consistent behavior when off

---

## 15) Observability and debug-ability

- [x] Structured logging exists (structlog or equivalent) for:
  - [x] startup discovery per upstream
  - [x] request_key computation (hashes only, no secrets)
  - [x] reuse decision: hit/miss and why
  - [x] artifact creation path including:
    - [x] envelope sizes
    - [x] oversized JSON offload events
    - [x] binary blob writes and dedupe hits
  - [x] mapping runs (full/partial), budgets, stop_reason, counts
  - [x] cursor validation failures categorized (invalid/expired/stale)
  - [x] pruning operations and bytes reclaimed
- [x] Metrics counters exist (can be simple internal counters):
  - [x] advisory lock timeouts
  - [x] upstream call latency and error types
  - [x] mapping latency and stop reasons
  - [x] prune deletions and disk bytes reclaimed

---

## 16) Test suite completion criteria (must pass)

At minimum, tests exist and pass for:

- [x] RFC 8785 canonicalization vectors + numeric edge cases.
- [x] Compression roundtrip integrity: compressed canonical bytes decompress to same bytes and hash matches.
- [x] Reserved arg stripping removes only `_gateway_*` keys and explicit reserved names.
- [x] Oversized JSON ingest becomes byte-backed binary_ref and is used for streaming mapping.
- [x] Partial mapping determinism:
  - [x] same payload + same budgets => same sample_indices + same root inventory
  - [x] map_budget_fingerprint mismatch => stale behavior
- [x] Prefix coverage semantics:
  - [x] stop_reason != none => count_estimate null, prefix_coverage true, sampled_prefix_len correct
- [x] Sampling bias invariant:
  - [x] oversize records skipped and counted; sample_indices exclude them
- [x] Cursor determinism:
  - [x] same request and position => same cursor payload (before HMAC) and valid verification
- [x] CURSOR_STALE conditions:
  - [x] sample_set mismatch
  - [x] where_canonicalization_mode mismatch
  - [x] traversal_contract_version mismatch
  - [x] artifact_generation mismatch
- [x] Session discovery correctness:
  - [x] artifact.search only returns artifacts in artifact_refs for that session
  - [x] new artifact appears immediately
- [x] Cleanup correctness:
  - [x] payload_binary_refs prevents orphaning
  - [x] hard delete removes filesystem blobs only when unreferenced

Integration tests (strongly recommended to count as done):

- [x] A local upstream MCP stub (http or stdio) that can return:
  - [x] small JSON, large JSON, text, errors, and binary payload
- [x] End-to-end:
  - [x] mirrored call -> artifact created -> artifact.search finds it -> artifact.get retrieves envelope -> mapping runs -> artifact.describe shows roots -> artifact.select returns projections -> cursor pagination works

---

## 17) Done means done runtime validation script

- [ ] A single command exists that executes a deterministic smoke test suite against a fresh DATA_DIR + fresh DB schema:
  - [ ] starts gateway
  - [ ] registers one stub upstream
  - [ ] exercises:
    - [ ] caching allow/fresh
    - [ ] reuse by request_key
    - [ ] error envelope creation
    - [ ] oversized JSON offload
    - [ ] partial mapping + artifact.select sampled-only + cursor continuation
    - [ ] soft delete then hard delete then verify blobs removed
  - [ ] exits non-zero on any invariant violation

---

## Repo-shaped completion checklist:

---

## 0) Repo skeleton and contracts

### Root files

- [ ] `pyproject.toml`
  - [ ] Pins Python `>=3.11`
  - [ ] Declares deps: `fastmcp`, `psycopg[binary]` or `psycopg3`, `ijson`, `zstandard` (or `gzip` fallback), `structlog`, `orjson` (optional), `pydantic` (optional), `pytest`
  - [ ] Defines `mcp-gateway` console script entrypoint
- [x] `README.md`
  - [x] Explains local-only, single-tenant, `DATA_DIR`, Postgres DSN
  - [x] Includes quickstart: run Postgres, migrate, run gateway, call mirrored tool
- [ ] `docs/spec_v1_9.md` (copy of the spec, locked)
- [ ] `docs/traversal_contract.md` (explicit ordering rules)
- [ ] `docs/cursor_contract.md` (payload fields, binding rules, stale rules)
- [ ] `docs/config.md` (all config keys + defaults)
- [x] `.env.example`
- [x] `docker-compose.yml` (local Postgres with test DB auto-provisioning)

### Package layout

- [ ] `src/mcp_artifact_gateway/__init__.py`
- [ ] `src/mcp_artifact_gateway/main.py` (CLI entry)
- [ ] `src/mcp_artifact_gateway/app.py` (composition root: config -> db -> fs -> upstreams -> MCP server)

---

## 1) Configuration, constants, and lifecycle

### Config and limits

- [x] `src/mcp_artifact_gateway/config/settings.py`
  - [x] Loads config from (in precedence): env vars -> `DATA_DIR/state/config.json` -> defaults
  - [x] Validates all caps/budgets exist (retrieval, mapping, JSON oversize caps, storage caps)
  - [x] Exposes:
    - [x] `DATA_DIR` and derived paths (`tmp/`, `logs/`, `blobs/`, `resources/`, `state/`)
    - [x] `envelope_jsonb_mode`, `envelope_jsonb_minimize_threshold_bytes`
    - [x] `max_json_part_parse_bytes` (oversized JSON becomes byte-backed binary ref)
    - [x] partial-map budgets (the full set used in `map_budget_fingerprint`)
    - [ ] cursor TTL and secret rotation settings
- [x] `src/mcp_artifact_gateway/constants.py`
  - [x] `WORKSPACE_ID = "local"`
  - [x] `traversal_contract_version` constant
  - [x] `canonicalizer_version` constant
  - [x] `mapper_version` constant
  - [x] `prng_version` constant
  - [x] `cursor_version` constant
  - [x] Reserved key prefix: `_gateway_` and explicit reserved names

### Startup and shutdown

- [x] `src/mcp_artifact_gateway/lifecycle.py`
  - [x] Ensures directories exist, permissions ok, temp dir writable
  - [ ] DB connect + migration check
  - [ ] Upstream MCP connect + tool discovery
  - [ ] Starts mapping worker loop if enabled
  - [ ] Starts prune worker loop if enabled
  - [ ] Clean shutdown closes upstream sessions, db pool, worker tasks

Acceptance

- [ ] Running `mcp-gateway --check` prints: DB ok, FS ok, upstream ok, versions, budgets (mirrors `gateway.status`)

---

## 2) Postgres schema and migrations

### Migration framework

- [x] `src/mcp_artifact_gateway/db/migrate.py`
  - [x] Applies SQL migrations in order
  - [x] Records applied migrations (table `schema_migrations`)
  - [x] Fails hard if migrations missing

### Migration SQL

- [x] `src/mcp_artifact_gateway/db/migrations/001_init.sql`
  - [x] Creates tables exactly per spec: `sessions`, `binary_blobs`, `payload_blobs`, `payload_hash_aliases`, `payload_binary_refs`, `artifacts`, `artifact_refs`, `artifact_roots`
  - [x] All PKs include `workspace_id`
  - [x] All constraints and indexes exist (especially `created_seq` identity and ordering indexes)
- [x] `src/mcp_artifact_gateway/db/migrations/002_indexes.sql` (optional if you split)
  - [x] Adds the heavier indexes (request_key, created_seq, last_seen)

### DB access layer

- [x] `src/mcp_artifact_gateway/db/conn.py`
  - [x] psycopg3 connection pool
  - [ ] typed helpers: `tx(fn)`, `fetchone`, `fetchall`, `execute`
- [x] `src/mcp_artifact_gateway/db/repos/*.py` (split by concern)
  - [x] `sessions_repo.py`
  - [x] `payloads_repo.py`
  - [x] `artifacts_repo.py`
  - [x] `mapping_repo.py`
  - [x] `prune_repo.py`

Acceptance

- [ ] `pytest -k migrations` can create a new DB, migrate, and verify all columns/indexes exist
- [ ] `created_seq desc` is the only latest selector everywhere it matters

---

## 3) Filesystem blob store (content-addressed) and atomic writes

### Binary store

- [x] `src/mcp_artifact_gateway/fs/blob_store.py`
  - [x] `put_bytes(raw_bytes, mime) -> BinaryRef`:
    - [x] `binary_hash = sha256(raw_bytes)`
    - [x] path = `DATA_DIR/blobs/bin/ab/cd/<binary_hash>`
    - [x] atomic write: temp in same dir -> fsync -> rename
    - [x] if exists: verify size, optional probe head/tail hashes
  - [x] `open_stream(binary_hash) -> IO[bytes]` for partial mapping byte-backed reads
  - [x] MIME normalization: lowercase, strip params, alias map

### Resource store (optional internal copy)

- [x] `src/mcp_artifact_gateway/fs/resource_store.py`
  - [x] Supports `resource_ref` durability rules (`internal` copies under `DATA_DIR/resources`)

Acceptance

- [ ] Blob writes are crash-safe: kill process mid-write never leaves partial final file
- [ ] `binary_blobs` rows match filesystem reality (byte_count and path)

---

## 4) Canonical JSON and hashing (no float drift)

### Canonicalizer

- [x] `src/mcp_artifact_gateway/canon/rfc8785.py`
  - [x] `canonical_bytes(obj) -> bytes` implementing RFC 8785
  - [x] Deterministic key ordering, UTF-8, number formatting
- [x] `src/mcp_artifact_gateway/canon/decimal_json.py`
  - [x] JSON loader that parses floats as `Decimal`, rejects NaN/Infinity
  - [x] Ensures canonicalization never sees Python float

### Hash utilities

- [x] `src/mcp_artifact_gateway/util/hashing.py`
  - [x] `sha256_hex(bytes)`, `sha256_trunc(bytes, n)`
  - [x] `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`

Acceptance

- [ ] RFC 8785 test vectors pass
- [ ] Same envelope object always yields identical canonical bytes across runs

---

## 5) Envelope normalization and oversized JSON rule

### Envelope model

- [x] `src/mcp_artifact_gateway/envelope/model.py`
  - [x] Typed dataclasses or pydantic models for:
    - [x] `Envelope`, `ContentPartJson`, `ContentPartText`, `ContentPartResourceRef`, `ContentPartBinaryRef`, `ErrorBlock`
- [x] `src/mcp_artifact_gateway/envelope/normalize.py`
  - [x] Converts upstream MCP response into canonical envelope shape
  - [x] Ensures: ok implies no error, error implies error present
  - [x] Never stores raw binary bytes in envelope

### Oversized JSON handling (byte-backed)

- [x] `src/mcp_artifact_gateway/envelope/oversize.py`
  - [x] If any JSON part exceeds `max_json_part_parse_bytes`:
    - [x] do not parse
    - [x] store raw bytes as `binary_ref` with `mime = application/json` (optionally `+encoding`)
    - [x] replace that part with a `binary_ref` descriptor
    - [x] add a warning in `meta.warnings` with original part index and encoding

Acceptance

- [ ] A 200MB JSON result does not allocate 200MB Python objects
- [ ] Partial mapping can later read the JSON from the binary blob stream

---

## 6) Payload storage (compressed canonical bytes) and integrity rule

### Payload persistence

- [x] `src/mcp_artifact_gateway/storage/payload_store.py`
  - [x] `compress(bytes) -> (encoding, compressed, uncompressed_len)`
  - [x] Supports `zstd|gzip|none`
  - [x] Writes `payload_blobs` row with:
    - [x] `envelope_canonical_bytes` compressed
    - [x] `envelope_canonical_bytes_len`
    - [x] `payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`
    - [x] `contains_binary_refs`
    - [x] `canonicalizer_version`
  - [x] Enforces integrity:
    - [x] `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))`
- [x] JSONB storage mode implemented:
  - [x] `full`
  - [x] `minimal_for_large` projection
  - [x] `none` projection

Acceptance

- [x] Payload retrieval can reconstruct envelope from canonical bytes even if jsonb is minimal/none

---

## 7) Artifact creation flow (mirroring, caching, stampede lock)

### Upstream discovery + mirroring

- [x] `src/mcp_artifact_gateway/mcp/upstream.py`
  - [x] Connects to each upstream MCP (stdio/http)
  - [x] Fetches tool list at startup
- [x] `src/mcp_artifact_gateway/mcp/mirror.py`
  - [x] Exposes mirrored tools as `{prefix}.{tool}` with identical schema/docs, no injected fields
  - [x] Strips reserved keys before schema validation and forwarding:
    - [x] exact keys: `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq`
    - [x] any key starting with `_gateway_`
    - [x] nothing else

### Request identity

- [x] `src/mcp_artifact_gateway/request_identity.py`
  - [x] Computes `upstream_instance_id` (semantic identity excluding secrets)
  - [x] Computes `canonical_args_bytes` via RFC 8785 after reserved stripping and schema validation
  - [x] `request_key = sha256(upstream_instance_id|prefix|tool|canonical_args_bytes)`
  - [x] Persists `request_args_hash` and capped `request_args_prefix`

### Stampede lock and reuse

- [x] `src/mcp_artifact_gateway/cache/reuse.py`
  - [x] Advisory lock: derive two 32-bit keys from `sha256(request_key)` and `pg_advisory_lock` with timeout
  - [x] If `cache_mode != fresh`, tries reuse by `request_key` latest (`created_seq desc`)
  - [x] Strict gating by schema hash unless configured otherwise
  - [x] Optional dedupe alias reuse (`payload_hash_aliases`) constrained to same upstream_instance_id + tool

### Artifact write

- [x] `src/mcp_artifact_gateway/artifacts/create.py`
  - [x] Implements the full step sequence in Section 9.1
  - [x] Always stores an artifact even on upstream error/timeout (error envelope)
  - [x] Inserts:
    - [x] payload blob row
    - [x] payload_binary_refs rows
    - [x] artifact row with `map_status=pending`, `map_kind=none` initially
    - [x] artifact_refs row and session last_seen update

Acceptance

- [ ] With DB and FS healthy, any upstream failure still yields a stored error artifact and returns a handle
- [ ] If DB or FS required path unavailable, gateway returns INTERNAL and does not claim artifact creation

---

## 8) Mapping system (full and partial)

### Mapping orchestrator

- [x] `src/mcp_artifact_gateway/mapping/runner.py`
  - [x] Picks JSON part to map deterministically with tie-break by part index
  - [x] Decides full vs partial:
    - [x] full if size <= `max_full_map_bytes`
    - [x] partial if too large or stored as `binary_ref application/json(+encoding)`
  - [x] Stores results in `artifact_roots`, updates artifact mapping columns

### Full mapper

- [x] `src/mcp_artifact_gateway/mapping/full.py`
  - [x] Parses fully, discovers up to K roots (K=3), builds deterministic inventory, writes `artifact_roots`

### Partial mapper (streaming, deterministic)

- [x] `src/mcp_artifact_gateway/mapping/partial.py`
  - [x] Consumes byte stream only (binary_ref stream preferred)
  - [x] Enforces budgets and emits `stop_reason`
  - [x] Computes and stores:
    - [x] `map_backend_id` derived from python + ijson backend+version
    - [x] `prng_version` constant
    - [x] `map_budget_fingerprint` hash over budgets + versions
  - [x] Root path normalization rules and no wildcards in root_path
  - [x] Streaming skip contract: can discard subtrees; compute steps count all events
  - [x] Deterministic reservoir sampling:
    - [x] seed = sha256(payload_hash_full|root_path|map_budget_fingerprint)
    - [x] reservoir algorithm exactly as specified
    - [x] oversize sampled elements are skipped and counted (bias invariant)
    - [x] sampled_prefix_len semantics
    - [x] `sample_indices` stored sorted ascending and includes only materialized indices
    - [x] count_estimate only when stop_reason none and closing array observed
  - [x] Inventory derivation from sampled records with caps
  - [x] If stop_reason != none:
    - [x] prefix coverage true
    - [x] count_estimate null

### Worker safety

- [x] `src/mcp_artifact_gateway/mapping/worker.py`
  - [x] Async/hybrid/sync modes supported
  - [x] Conditional update safety:
    - [x] deleted_at null
    - [x] map_status in (pending, stale)
    - [x] generation matches snapshot
    - [x] else discard results

Acceptance

- [ ] Partial mapping deterministic across runs given identical payload and budgets (fingerprint unchanged)
- [ ] Remapping with different budgets marks old mapping stale for mapped ops and cursors

---

## 9) Retrieval: JSONPath, select_paths, where hashing, traversal contract

### JSONPath subset + canonicalization

- [x] `src/mcp_artifact_gateway/query/jsonpath.py`
  - [x] Parser for allowed grammar only: `$`, `.name`, `['..']`, `[n]`, `[*]`
  - [x] Caps: length, segments, wildcard expansion total
- [x] `src/mcp_artifact_gateway/query/select_paths.py`
  - [x] Normalizes each path and rejects absolute `$` for select_paths
  - [x] Sorts lexicographically, dedupes, computes `select_paths_hash`
- [x] `src/mcp_artifact_gateway/query/where_hash.py`
  - [x] Implements `where_canonicalization_mode`:
    - [x] raw_string hash mode
    - [ ] canonical_ast mode with commutative sort and numeric/string normalization
  - [x] Exposes mode via `gateway.status()`

### Traversal contract

- [x] `src/mcp_artifact_gateway/retrieval/traversal.py`
  - [x] Arrays index ascending, objects keys lex asc
  - [x] Wildcard expansions obey same ordering
  - [x] Partial mode enumerates sampled indices ascending

Acceptance

- [ ] Given same artifact and same query, pagination yields identical item boundaries and cursors

---

## 10) Cursor signing, binding, and staleness

### Secrets

- [x] `src/mcp_artifact_gateway/cursor/secrets.py`
  - [x] Loads secret set from `DATA_DIR/state/secrets.json`
  - [x] Tracks active secret versions: newest signs, all active verify
- [x] `src/mcp_artifact_gateway/cursor/hmac.py`
  - [x] Format: `base64url(payload_json) + "." + base64url(hmac)`
  - [x] Enforces TTL and expires_at

### Cursor payload enforcement

- [x] `src/mcp_artifact_gateway/cursor/payload.py`
  - [x] Includes all required fields in Section 14.2
  - [x] Verifies server `where_canonicalization_mode` matches cursor else CURSOR_STALE

### Partial cursor binding

- [x] `src/mcp_artifact_gateway/cursor/sample_set_hash.py`
  - [x] Computes `sample_set_hash` from root_path + stored sample_indices + map_budget_fingerprint + mapper_version
  - [x] Verification recomputes from DB and mismatch => CURSOR_STALE

Acceptance

- [ ] Cursor cannot be replayed against different where mode
- [ ] Cursor from old partial mapping becomes stale after remap (different fingerprint or sample indices)

---

## 11) MCP tool surface: gateway.status and artifact tools

### Tool server

- [x] `src/mcp_artifact_gateway/mcp/server.py`
  - [x] Registers gateway tools:
    - [x] `gateway.status`
    - [x] `artifact.search`
    - [x] `artifact.get`
    - [x] `artifact.select`
    - [x] `artifact.describe`
    - [x] `artifact.find`
    - [x] `artifact.chain_pages`
  - [x] Also registers mirrored upstream tools at `{prefix}.{tool}`

### Tool implementations

- [x] `src/mcp_artifact_gateway/tools/status.py`
  - [x] Returns: upstream connectivity, DB ok, FS ok, versions, traversal_contract_version, where mode, map_backend_id/prng_version, budgets, cursor TTL, secret versions
- [x] `src/mcp_artifact_gateway/tools/artifact_search.py`
  - [x] Lists artifacts using `artifact_refs` only
  - [x] Touch policy: updates session/artifact_refs last_seen, does not touch artifact last_referenced
- [x] `src/mcp_artifact_gateway/tools/artifact_get.py`
  - [x] target `envelope` applies jsonpath on envelope root, reconstruct from canonical bytes if needed
  - [x] target `mapped` only if map_status ready and map_kind full/partial
  - [x] Touch semantics: touch last_referenced_at if not deleted, always update session/artifact_refs, else GONE
- [x] `src/mcp_artifact_gateway/tools/artifact_select.py`
  - [x] Full mapping: bounded deterministic scan
  - [x] Partial mapping: sampled-only enumeration and response includes sampled_only, sample_indices_used, sampled_prefix_len
- [x] `src/mcp_artifact_gateway/tools/artifact_describe.py`
  - [x] Includes partial mapping disclosures: sampled-only constraints, prefix coverage, stop_reason, counts
- [x] `src/mcp_artifact_gateway/tools/artifact_find.py`
  - [x] Sample-only unless index enabled
- [x] `src/mcp_artifact_gateway/tools/artifact_chain_pages.py`
  - [x] Orders by chain_seq asc then created_seq asc, allocates chain_seq with retry

### Standard bounded response shape

- [x] `src/mcp_artifact_gateway/retrieval/response.py`
  - [x] Always returns `{items, truncated, cursor, omitted, stats}`

Acceptance

- [ ] All tools require `_gateway_context.session_id` and reject missing with INVALID_ARGUMENT
- [ ] Any truncation yields deterministic cursor and position encoding per traversal contract

---

## 12) Session tracking and touch policy

### Session enforcement

- [x] `src/mcp_artifact_gateway/sessions.py`
  - [x] Creates or updates session row with last_seen_at
  - [x] Upserts artifact_refs (first_seen_at, last_seen_at)

### Touch rules

- [x] Implemented exactly:
  - [x] creation touches artifacts.last_referenced_at
  - [x] retrieval/describe touches if not deleted
  - [x] search does not touch

Acceptance

- [ ] Prune policies behave correctly because touch semantics are correct

---

## 13) Pruning, hard delete, and filesystem cleanup

### Soft delete job

- [x] `src/mcp_artifact_gateway/jobs/soft_delete.py`
  - [x] Uses SKIP LOCKED, rechecks predicates on update, sets deleted_at and generation++

### Hard delete job

- [x] `src/mcp_artifact_gateway/jobs/hard_delete.py`
  - [x] Deletes artifacts, cascades remove artifact_roots and artifact_refs
  - [x] Deletes unreferenced payload_blobs
  - [x] Deletes unreferenced binary_blobs via payload_binary_refs
  - [x] Removes filesystem blobs for removed binary_blobs

### Reconciler (optional but strongly recommended)

- [x] `src/mcp_artifact_gateway/jobs/reconcile_fs.py`
  - [x] Finds orphan files not referenced in DB and optionally removes them

Acceptance

- [ ] End-to-end: create artifacts with binaries, delete them, filesystem blobs disappear only when unreferenced

---

## 14) Observability, metrics, and determinism logging

- [x] `src/mcp_artifact_gateway/obs/logging.py`
  - [x] structlog configuration, JSON logs
  - [x] Correlation fields: session_id, artifact_id, request_key, payload_hash_full
- [x] `src/mcp_artifact_gateway/obs/metrics.py` (optional)
  - [x] Counters:
    - [x] cache hits, alias hits, upstream calls
    - [x] oversize JSON count
    - [x] partial map stop_reason distribution
    - [x] cursor stale reasons
    - [x] advisory lock timeouts
- [x] Determinism debug logs:
  - [x] map_budget_fingerprint
  - [x] map_backend_id
  - [x] prng_version
  - [x] sample_set_hash on cursor issue/verify

Acceptance

- [ ] Given a cursor stale event, logs show which binding field mismatched

---

## 15) Test suite, fixtures, and done gates

### Unit tests (must exist)

- [x] `tests/test_reserved_arg_stripping.py`
  - [x] Only `_gateway_*` removed, nothing else
- [x] `tests/test_rfc8785_vectors.py`
- [x] `tests/test_decimal_json_no_float.py`
- [x] `tests/test_payload_canonical_integrity.py`
- [x] `tests/test_oversize_json_becomes_binary_ref.py`
- [x] `tests/test_partial_mapping_determinism.py`
  - [x] same bytes + same budgets => identical sample_indices and fields_top
- [x] `tests/test_prefix_coverage_semantics.py`
  - [x] stop_reason != none implies count_estimate null and prefix coverage true
- [x] `tests/test_sampling_bias_invariant.py`
  - [x] oversize sampled elements are skipped and counted
- [x] `tests/test_cursor_sample_set_hash_binding.py`
- [x] `tests/test_cursor_where_mode_stale.py`
- [x] `tests/test_touch_policy.py`
- [x] Additional unit tests added beyond spec minimums (config loading, traversal, jsonpath, hashing, stores, bounded response, migrations)

### Integration tests (must exist)

- [x] `tests/integration/test_e2e_pipeline.py` (20 tests covering all scenarios below)
  - [x] mirrored call -> artifact created -> mapping ready -> select works -> cursor pages
  - [x] large JSON -> partial mapping -> sampled-only select works
  - [x] soft delete then hard delete cleans DB
  - [x] cache reuse, session isolation, WHERE filtering, JSONPath, chain pages, oversize reconstruction, migration idempotency, generation-safe races, status health
- [x] `tests/integration/test_postgres_runtime.py` (9 tests)
  - [x] persist + search + get, session isolation, binary refs, soft/hard delete

### Ship gate criteria

- [ ] All tests pass in CI on Linux
- [ ] A local demo script produces:
  - [ ] one small JSON artifact that returns inline envelope
  - [ ] one large JSON artifact that returns handle and supports sampled-only select
  - [ ] cursor pagination stable across two identical runs
- [ ] No tool ever returns unbounded bytes/items
- [ ] Determinism artifacts are visible: traversal_contract_version, map_budget_fingerprint, sample_set_hash appear in responses where relevant
