# Task Plan: MCP Artifact Gateway (Python) Full Implementation v1.9 (Local Single-Tenant)

## Goal
Build a complete, production-grade, local single-tenant MCP gateway in Python that proxies upstream MCP tools, stores every tool result as a durable artifact envelope (disk + Postgres), returns compact handles, generates deterministic inventories, and enforces bounded deterministic retrieval with signed cursors.

## Current Phase
Phase 1: Repo Skeleton and Bootability

## Phase Roadmap (High-Level)
- [ ] Phase 1: Repo skeleton, entrypoint, boot sequence
- [ ] Phase 2: Config, constants, lifecycle
- [ ] Phase 3: Postgres schema + migrations
- [ ] Phase 4: Filesystem blob store + resource store
- [ ] Phase 5: Canonicalization + hashing + compression
- [ ] Phase 6: Envelope normalization + payload storage
- [ ] Phase 7: Upstream discovery + mirroring + artifact creation
- [ ] Phase 8: Mapping system (full + partial)
- [ ] Phase 9: Query + traversal + retrieval core
- [ ] Phase 10: Cursor signing + binding + staleness
- [ ] Phase 11: MCP tool surface
- [ ] Phase 12: Session tracking + touch policy
- [ ] Phase 13: Pruning + retention + cleanup
- [ ] Phase 14: Observability + metrics
- [ ] Phase 15: Test suite + done gates
- [ ] Phase 16: Documentation + packaging

## Completion Checklists (Spec v1.9)
These are copied from the spec checklists. Mark these items complete as implementation progresses.

## Implementation checklist:

Below is a completion-grade implementation checklist for MCP Artifact Gateway (Python) Full Implementation Spec v1.9 Local Single-Tenant. It is written as everything that must exist in code plus everything that must be demonstrably true at runtime, mapped directly to the v1.9 spec.

## 1) Repo shape and bootability

- [ ] A single runnable entrypoint exists (for example `python -m mcp_gateway` or `mcp-gateway serve`) that:
  - [ ] Loads config.
  - [ ] Validates filesystem paths.
  - [ ] Connects Postgres and validates migrations.
  - [ ] Connects to every configured upstream (stdio and http).
  - [ ] Discovers upstream tool lists.
  - [ ] Starts an MCP server exposing:
    - [ ] mirrored tools `{prefix}.{tool}`
    - [ ] retrieval tools (`gateway.status`, `artifact.search`, `artifact.get`, `artifact.select`, `artifact.describe`, `artifact.find`, `artifact.chain_pages`)
- [ ] A fail fast startup mode exists:
  - [ ] If DB is unreachable or migrations missing: server does not start (or starts with gateway unhealthy and refuses mirrored calls with `INTERNAL`).
  - [ ] If DATA_DIR or required subdirs cannot be created/written: server does not start (or starts unhealthy and refuses mirrored calls with `INTERNAL`).
- [ ] A clear module boundary exists (names are illustrative, not mandatory):
  - [ ] `config/` (schema + loader + defaults)
  - [ ] `db/` (psycopg3 pool, migrations, queries)
  - [ ] `fs/` (DATA_DIR layout, atomic writes, blob paths, resource copies)
  - [ ] `canonical/` (RFC 8785 canonicalizer + hashing + compression)
  - [ ] `upstream/` (clients for stdio/http MCP, discovery, schema parsing)
  - [ ] `gateway/` (request handling, reserved arg stripping, reuse logic, artifact creation)
  - [ ] `retrieval/` (jsonpath evaluation, select/projection, cursor handling)
  - [ ] `mapping/` (full mapping, partial mapping, worker, sampling)
  - [ ] `prune/` (soft delete, hard delete, blob cleanup, reconciliation)
  - [ ] `tests/` (unit + integration)

---

## 2) Configuration and constants

- [ ] A config model exists that includes (at minimum):
  - [ ] `DATA_DIR` and derived directories (`tmp/`, `logs/`, `state/`, `resources/`, `blobs/bin/...`)
  - [ ] Postgres DSN + pool sizing + statement timeouts
  - [ ] Upstream definitions:
    - [ ] `prefix`, `transport` (http/stdio), endpoint config, semantic salts
    - [ ] optional tool-level dedupe exclusions (JSONPath subset)
    - [ ] tool-level reuse gating (strict schema hash match default)
    - [ ] tool-level inline eligibility (default allow)
  - [ ] Envelope storage config:
    - [ ] `envelope_jsonb_mode` = `full|minimal_for_large|none`
    - [ ] `envelope_jsonb_minimize_threshold_bytes`
    - [ ] canonical byte compression: `zstd|gzip|none`
  - [ ] Hard limits / budgets:
    - [ ] inbound request cap
    - [ ] upstream error capture cap
    - [ ] `max_json_part_parse_bytes` (oversized JSON becomes byte-backed)
    - [ ] `max_full_map_bytes`
    - [ ] partial map budgets: `max_bytes_read_partial_map`, `max_compute_steps_partial_map`, `max_depth_partial_map`, `max_records_sampled_partial`, `max_record_bytes_partial`, `max_leaf_paths_partial`, `max_root_discovery_depth`
    - [ ] retrieval budgets: `max_items`, `max_bytes_out`, `max_wildcards`, `max_compute_steps`
    - [ ] `artifact_search_max_limit`
    - [ ] storage caps: `max_binary_blob_bytes`, `max_payload_total_bytes`, `max_total_storage_bytes`
  - [ ] Cursor settings:
    - [ ] cursor TTL minutes
    - [ ] active secret versions + signing secret version
    - [ ] cursor_version constant
  - [ ] Version constants:
    - [ ] `canonicalizer_version`
    - [ ] `mapper_version`
    - [ ] `traversal_contract_version`
    - [ ] `where_canonicalization_mode` (`raw_string` default, `canonical_ast` optional)
    - [ ] `prng_version` constant
- [ ] The system hardcodes and enforces `workspace_id = "local"` everywhere (no multi-tenant behavior leaks in).

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
- [ ] Every PK, FK, unique constraint, and index in the spec exists.
- [ ] All relevant CHECK constraints exist:
  - [ ] enum-like fields (`map_kind`, `map_status`, `index_status`, encoding values, non-negative sizes)
- [ ] Migrations are idempotent and ordered; a fresh database can be brought to current schema in one command.
- [ ] Advisory lock usage for request stampede exists (two 32-bit keys derived from `sha256(request_key)`), with timeout and metrics/logging.

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

- [ ] RFC 8785 canonical JSON implementation exists and is used for:
  - [ ] forwarded args canonicalization
  - [ ] upstream tool schema canonicalization
  - [ ] envelope canonicalization
  - [x] cursor payload canonicalization
  - [ ] record hashing in `artifact_samples`
- [ ] Numeric parsing rules are enforced:
  - [ ] floats parsed as Decimal (no Python float drift)
  - [ ] NaN/Infinity rejected
  - [ ] canonicalization never sees Python floats
- [ ] Payload identity is correct:
  - [ ] `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`
  - [ ] `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling)
- [ ] Canonical bytes storage works:
  - [ ] `envelope_canonical_encoding` stored (`zstd|gzip|none`)
  - [ ] `envelope_canonical_bytes` stored (compressed)
  - [ ] `envelope_canonical_bytes_len` stored (uncompressed length)
- [ ] Dedupe hash is implemented and explicitly does not define storage identity:
  - [ ] tool-configured JSONPath exclusions apply only to dedupe computation
  - [ ] alias table `payload_hash_aliases` is populated and used only for reuse lookup

## 5) Canonicalization, hashing, compression, numeric safety (duplicate section in spec)

- [ ] RFC 8785 canonical JSON implementation exists and is used for:
  - [ ] forwarded args canonicalization
  - [ ] upstream tool schema canonicalization
  - [ ] envelope canonicalization
  - [x] cursor payload canonicalization
  - [ ] record hashing in `artifact_samples`
- [ ] Numeric parsing rules are enforced:
  - [ ] floats parsed as Decimal (no Python float drift)
  - [ ] NaN/Infinity rejected
  - [ ] canonicalization never sees Python floats
- [ ] Payload identity is correct:
  - [ ] `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`
  - [ ] `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling)
- [ ] Canonical bytes storage works:
  - [ ] `envelope_canonical_encoding` stored (`zstd|gzip|none`)
  - [ ] `envelope_canonical_bytes` stored (compressed)
  - [ ] `envelope_canonical_bytes_len` stored (uncompressed length)
- [ ] Dedupe hash is implemented and explicitly does not define storage identity:
  - [ ] tool-configured JSONPath exclusions apply only to dedupe computation
  - [ ] alias table `payload_hash_aliases` is populated and used only for reuse lookup

---

## 6) Upstream discovery, mirroring, and reserved arg stripping

- [ ] Upstream tool discovery at startup:
  - [ ] fetch tool list from each upstream
  - [ ] expose mirrored tools as `{prefix}.{tool}` with identical schema/docs (no injected fields)
- [ ] Reserved gateway args stripping is exact and tested:
  - [ ] remove keys equal to `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq`
  - [ ] remove any key whose name begins with exact prefix `_gateway_`
  - [ ] remove nothing else (example: `gateway_url` must not be stripped)
- [ ] Upstream instance identity exists and excludes secrets:
  - [ ] `upstream_instance_id = sha256(canonical_semantic_identity_bytes)[:32]`
  - [ ] includes transport + stable endpoint identity + prefix/name + optional semantic salt
  - [ ] excludes rotating auth headers, tokens, secret env values, private key paths
  - [ ] optional `upstream_auth_fingerprint` stored for debugging but excluded from request identity
- [ ] `request_key` is computed exactly:
  - [ ] based on `upstream_instance_id`, prefix, tool_name, canonical args bytes
  - [ ] canonical args bytes computed after stripping reserved keys and validating against upstream schema
  - [ ] `request_args_hash` and `request_args_prefix` persisted with caps

---

## 7) Artifact creation pipeline (mirrored tool calls)

For every mirrored tool call `{prefix}.{tool}(args)`:

- [ ] Validate `_gateway_context.session_id` exists, else `INVALID_ARGUMENT`.
- [ ] Determine `cache_mode` default `allow`.
- [ ] Strip reserved gateway args (exact rules).
- [ ] Validate forwarded args against upstream schema.
- [ ] Canonicalize forwarded args and compute `request_key`.
- [ ] Acquire advisory lock for stampede control (with timeout behavior).
- [ ] Reuse behavior when `cache_mode != fresh`:
  - [ ] request_key latest candidate chosen by `created_seq desc`
  - [ ] optional dedupe alias reuse constrained to same `(upstream_instance_id, tool)`
  - [ ] reuse requires:
    - [ ] not deleted, not expired
    - [ ] schema hash match if strict reuse enabled
  - [ ] response indicates `meta.cache.reused=true` + reason + reused artifact id
- [ ] Call upstream tool, capture success or failure.
- [ ] Normalize into envelope (always):
  - [ ] status ok or error, with error shape present on error
  - [ ] content parts support `json`, `text`, `resource_ref`, `binary_ref` (and alias `image_ref`)
  - [ ] binary bytes never stored inline, only refs
- [ ] Oversized JSON handling at ingest:
  - [ ] if any JSON part size > `max_json_part_parse_bytes`:
    - [ ] do not parse into structured value
    - [ ] store raw bytes as `binary_ref` with JSON mime (and encoding)
    - [ ] replace JSON content entry with `binary_ref` descriptor
    - [ ] add warning in `meta.warnings` with original part index + encoding
- [ ] Produce canonical envelope bytes, compute payload hashes, compress canonical bytes.
- [ ] Insert or upsert `payload_blobs`.
- [ ] Insert `binary_blobs` and `payload_binary_refs` for every blob reference.
- [ ] Insert optional `payload_hash_aliases` rows (dedupe).
- [ ] Insert `artifacts` row with:
  - [ ] monotonic `created_seq`
  - [ ] mapping fields: `map_kind='none'`, `map_status='pending'` initially, `mapper_version` set
  - [ ] `index_status='off'` unless enabled
  - [ ] sizes persisted (`payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`)
  - [ ] `last_referenced_at=now()`
- [ ] Update session tracking:
  - [ ] `sessions` upsert with `last_seen_at=now()`
  - [ ] `artifact_refs` upsert for `(session_id, artifact_id)`
- [ ] Return contract (Addendum A):
  - [ ] returns a handle-only result by default
  - [ ] returns handle+inline envelope only when thresholds satisfied and policy allows
  - [ ] if gateway itself unhealthy (DB/fs), return `INTERNAL` and do not claim artifact creation

---

## 8) Gateway tool response contracts

- [ ] All tool responses follow one of:
  - [ ] `gateway_tool_result` for success returns (mirrored tools)
  - [ ] uniform `gateway_error` for failures (all tools)
- [ ] Handle includes required metadata:
  - [ ] ids, created_seq, session_id, tool ids, hash ids, byte sizes, mapping/index status, contains_binary_refs, status
- [ ] Warnings propagation works:
  - [ ] warnings in response include gateway warnings inserted into envelope meta
- [ ] Error codes are implemented and used correctly:
  - [ ] `INVALID_ARGUMENT`, `NOT_FOUND`, `GONE`, `INTERNAL`, `CURSOR_INVALID`, `CURSOR_EXPIRED`, `CURSOR_STALE`, `BUDGET_EXCEEDED`, `UNSUPPORTED`

---

## 9) Retrieval tools and deterministic bounds

### 9.1 gateway.status

- [ ] `gateway.status()` returns:
  - [ ] upstream connectivity snapshot
  - [ ] DB ok / migrations ok
  - [ ] filesystem paths ok
  - [ ] version constants: canonicalizer, mapper, traversal contract, cursor version
  - [ ] where canonicalization mode
  - [ ] partial mapping backend id + prng version
  - [ ] all configured limits/budgets
  - [ ] cursor TTL and active secret versions

### 9.2 artifact.search

- [ ] Requires `_gateway_context.session_id`.
- [ ] Reads only from `artifact_refs` for that session (discovery uses refs exclusively).
- [ ] Filters implemented (Addendum B), including:
  - [ ] include_deleted, status, source_tool_prefix, source_tool, upstream_instance_id, request_key, payload_hash_full, parent_artifact_id, has_binary_refs, created_seq range, created_at range
- [ ] Ordering implemented:
  - [ ] `created_seq_desc` default
  - [ ] `last_seen_desc` optional
- [ ] Search touches only:
  - [ ] `sessions.last_seen_at`
  - [ ] `artifact_refs.last_seen_at`
  - [ ] does not touch `artifacts.last_referenced_at`
- [ ] Pagination cursor for search exists and is bound to session_id + order_by + last position.

### 9.3 JSONPath subset support

- [ ] JSONPath subset grammar implemented exactly:
  - [ ] `$`, `.name`, `['...']` with escapes, `[n]`, `[*]`
  - [ ] no filters
- [ ] Caps enforced:
  - [ ] max path length
  - [ ] max segments
  - [ ] max wildcard expansion total
- [ ] Deterministic traversal contract implemented:
  - [ ] arrays ascend
  - [ ] objects key lex ascend
  - [ ] wildcard expansions obey above
  - [ ] partial sample enumeration uses ascending sample indices

### 9.4 artifact.get

- [ ] Requires session_id.
- [ ] Supports:
  - [ ] `target=envelope` (jsonpath evaluated on envelope root)
  - [ ] `target=mapped` (requires map_status ready and map_kind full/partial)
- [ ] If envelope jsonb is minimized or none:
  - [ ] reconstruct by parsing canonical bytes, within compute budgets
- [ ] Bounded deterministic output:
  - [ ] max_bytes_out / max_items / max_compute enforced
  - [ ] deterministic truncation emits `truncated=true` + cursor + omitted metadata
- [ ] Touch semantics:
  - [ ] if not deleted: touches `artifacts.last_referenced_at`
  - [ ] always updates `artifact_refs.last_seen_at` and `sessions.last_seen_at`
  - [ ] if deleted: returns `GONE`

### 9.5 artifact.describe

- [ ] Returns:
  - [ ] mapping status/kind + mapper_version
  - [ ] roots inventory + fields_top
  - [ ] partial mapping fields: sampled-only, prefix coverage indicator, stop_reason, sampled_prefix_len, sampled_record_count, skipped_oversize_records
  - [ ] count_estimate only when known under the stated rules
- [ ] Touch semantics same as retrieval.

### 9.6 artifact.select

- [ ] Inputs:
  - [ ] artifact_id, root_path, select_paths (set semantics), where optional, limits, cursor
- [ ] select_paths canonicalization implemented:
  - [ ] whitespace removal, canonical escaping/quotes
  - [ ] relative paths must not start with `$`
  - [ ] sorted lexicographically, duplicates removed
  - [ ] `select_paths_hash = sha256(canonical_json(array))`
- [ ] where hashing implemented per server mode:
  - [ ] raw_string default: exact UTF-8 bytes
  - [ ] canonical_ast optional: parse + canonicalize
  - [ ] server reports mode in `gateway.status`, cursor binds to it
- [ ] Full mapping behavior:
  - [ ] bounded scan in deterministic order with cursor continuation
- [ ] Partial mapping behavior:
  - [ ] sampled-only scan:
    - [ ] enumerate sample indices ascending
    - [ ] evaluate where and select_paths only on sampled records
    - [ ] returns `sampled_only=true`, `sample_indices_used`, `sampled_prefix_len`
- [ ] Output projection contract (Addendum F):
  - [ ] each item has `_locator` and `projection`
  - [ ] projection keys are canonicalized select paths, emitted in lex order
  - [ ] missing path behavior respects config `select_missing_as_null`

### 9.7 artifact.find

- [ ] Works in sample-only mode unless indexing is enabled.
- [ ] Deterministic output and bounded truncation with cursor.

### 9.8 artifact.chain_pages

- [ ] Chain ordering is correct:
  - [ ] `chain_seq asc`, then `created_seq asc`
- [ ] Chain seq allocation exists when not provided, with retry and uniqueness constraint.

---

## 10) Mapping implementation (full + partial) and worker safety

### 10.1 Mapping scheduler

- [ ] mapping_mode implemented: `async|hybrid|sync` (default hybrid)
- [ ] Artifacts created with map_status pending cause mapping work to be scheduled.

### 10.2 JSON part selection scoring

- [ ] Deterministic scoring implemented; tie-break by part index ascending.
- [ ] Stores `mapped_part_index` on artifact.

### 10.3 Full mapping

When selected JSON part is <= max_full_map_bytes:

- [ ] parse fully
- [ ] discover up to K roots (K=3)
- [ ] build deterministic inventory:
  - [ ] roots entries written to `artifact_roots`
  - [ ] `map_kind=full`, `map_status=ready`

### 10.4 Partial mapping trigger

- [ ] If JSON part too large OR stored as `binary_ref application/json(+encoding)`:
  - [ ] partial mapping runs
  - [ ] `map_kind=partial`

### 10.5 Partial mapping core requirements

- [ ] Byte-backed streaming input supported:
  - [ ] from JSON binary blob (required when oversized at ingest)
  - [ ] from text JSON (bounded)
  - [ ] from re-canonicalized bytes for small structured values (bounded)
- [ ] Budgets enforced during streaming:
  - [ ] max bytes read
  - [ ] max compute steps (stream events)
  - [ ] max depth
  - [ ] max sampled records N
  - [ ] max per-record bytes
  - [ ] max leaf paths
  - [ ] root discovery depth cap
- [ ] stop_reason tracked:
  - [ ] none | max_bytes | max_compute | max_depth | parse_error
- [ ] Prefix coverage semantics enforced:
  - [ ] if stop_reason != none:
    - [ ] count_estimate is null
    - [ ] root_shape.prefix_coverage=true
    - [ ] inventory coverage computed vs prefix
- [ ] map_backend_id and prng_version:
  - [ ] map_backend_id computed exactly from python version + ijson backend name + version
  - [ ] prng_version is a code constant
  - [ ] both returned by status and stored on artifacts
- [ ] map_budget_fingerprint computed and stored:
  - [ ] includes mapper version, traversal contract, backend id, prng version, all budgets
  - [ ] if changes, previous partial mapping marked stale and cursors become stale
- [ ] root_path normalization:
  - [ ] absolute path starting with `$`
  - [ ] uses `.name` when identifier is valid; otherwise bracket form with canonical escaping
  - [ ] no wildcards
  - [ ] format change requires traversal_contract_version bump (enforced as policy)
- [ ] streaming skip contract implemented:
  - [ ] ability to skip unselected subtrees without building full trees
  - [ ] compute steps count all events processed, including skipped

### 10.6 Deterministic reservoir sampling

- [ ] Reservoir sampling is one-pass and prefix-bounded:
  - [ ] seed = sha256(payload_hash_full + "|" + root_path + "|" + map_budget_fingerprint)
  - [ ] PRNG deterministic and versioned
  - [ ] selected indices maintained uniformly over processed prefix indices
- [ ] Bias invariant is explicit and implemented:
  - [ ] oversize/depth-violating records are skipped and counted
  - [ ] sample_indices include only successfully materialized records
- [ ] sampled_prefix_len is computed correctly:
  - [ ] counts element boundaries successfully recognized, including skipped/non-materialized
  - [ ] parse_error mid-element uses last fully recognized index + 1
- [ ] count_estimate rules enforced:
  - [ ] set only if stop_reason==none AND array close observed

### 10.7 Persisted samples (Addendum C)

- [ ] `artifact_samples` table is used for partial samples:
  - [ ] one row per materialized sampled record index
  - [ ] record hash stored as sha256(RFC8785(record))
- [ ] `artifact_roots.sample_indices` exactly matches sample indices present in `artifact_samples` (sorted).
- [ ] Updates are atomic:
  - [ ] replace sample rows + sample_indices within a transaction per `(artifact_id, root_key)`
- [ ] Partial retrieval depends on artifact_samples:
  - [ ] `artifact.select` loads records from artifact_samples
  - [ ] corruption detection: if indices exist but sample rows missing -> `INTERNAL` with details

### 10.8 Worker safety and races

- [ ] Worker writes are conditional:
  - [ ] artifact not deleted
  - [ ] map_status in (pending, stale)
  - [ ] generation matches
- [ ] If conditional update affects 0 rows, worker discards results.
- [ ] map_error stored on failure with enough detail to debug.

---

## 11) Cursor system (signing, binding, staleness)

- [ ] Cursor format implemented:
  - [ ] `base64url(payload_bytes) + "." + base64url(hmac)`
  - [ ] unpadded base64url
- [ ] Cursor payload canonicalization is RFC 8785 (Addendum D):
  - [ ] signature input is exactly the canonical payload bytes
- [ ] Secrets stored at `DATA_DIR/state/secrets.json` with:
  - [ ] `cursor_ttl_minutes`
  - [ ] `active_secrets[]` version + b64 key
  - [ ] `signing_secret_version` present in active list
  - [ ] keys >= 32 random bytes
- [ ] Verification logic:
  - [ ] parse payload bytes
  - [ ] expiration check -> `CURSOR_EXPIRED`
  - [ ] secret version missing -> `CURSOR_INVALID`
  - [ ] constant-time HMAC compare -> else `CURSOR_INVALID`
  - [ ] binding checks enforce `CURSOR_STALE` for:
    - [ ] where_canonicalization_mode mismatch
    - [ ] traversal_contract_version mismatch
    - [ ] artifact_generation mismatch
    - [ ] partial sample_set_hash mismatch
    - [ ] partial map_budget_fingerprint mismatch
- [ ] Cursor binding fields exist exactly per tool:
  - [ ] get binds target + normalized_jsonpath
  - [ ] select binds root_path + select_paths_hash + where_hash
- [ ] Partial mode cursor binding includes:
  - [ ] map_budget_fingerprint (required)
  - [ ] sample_set_hash computed from DB sample indices and compared

---

## 12) where DSL implementation (Addendum E)

- [ ] Parser exists for the specified grammar (OR/AND/NOT, parentheses, comparisons).
- [ ] Relative path evaluation uses JSONPath subset (must not start with `$`).
- [ ] Missing path semantics implemented:
  - [ ] comparisons false except special `!= null` semantics (as defined)
- [ ] Wildcard semantics:
  - [ ] existential: any match satisfies
  - [ ] bounded by max wildcard expansion
- [ ] Type semantics implemented exactly:
  - [ ] numeric comparisons require numeric operands
  - [ ] string comparisons lexicographic by codepoint
  - [ ] boolean only supports = and !=
- [ ] Compute accounting exists and is deterministic:
  - [ ] increments per path segment and expansions and comparison op
  - [ ] deterministic short-circuiting

---

## 13) Retention, pruning, and cleanup correctness

- [ ] Touch policy implemented exactly:
  - [ ] creation touches `artifacts.last_referenced_at`
  - [ ] retrieval/describe touches if not deleted
  - [ ] search does not touch last_referenced_at
- [ ] Soft delete job exists:
  - [ ] selects with SKIP LOCKED
  - [ ] predicate rechecked on update
  - [ ] sets deleted_at and increments generation
  - [ ] does not remove payloads yet
- [ ] Hard delete job exists:
  - [ ] deletes eligible artifacts
  - [ ] cascades remove `artifact_roots`, `artifact_refs`, `artifact_samples`
  - [ ] deletes unreferenced `payload_blobs`
  - [ ] cascades remove `payload_binary_refs`
  - [ ] deletes `binary_blobs` unreferenced by payload_binary_refs
  - [ ] removes corresponding filesystem blob files
  - [ ] optional reconciliation: detects orphan files on disk and can report/remove
- [ ] Quota enforcement exists:
  - [ ] storage cap breach triggers prune behavior (as configured)

---

## 14) Indexing (even if off by default)

- [ ] Code supports `index_status` lifecycle:
  - [ ] off | pending | ready | partial | failed
- [ ] `artifact.find` respects sample-only unless index enabled rule.
- [ ] If indexing is truly out of project scope for now, code still must:
  - [ ] store `index_status` fields
  - [ ] return consistent behavior when off

---

## 15) Observability and debug-ability

- [ ] Structured logging exists (structlog or equivalent) for:
  - [ ] startup discovery per upstream
  - [ ] request_key computation (hashes only, no secrets)
  - [ ] reuse decision: hit/miss and why
  - [ ] artifact creation path including:
    - [ ] envelope sizes
    - [ ] oversized JSON offload events
    - [ ] binary blob writes and dedupe hits
  - [ ] mapping runs (full/partial), budgets, stop_reason, counts
  - [ ] cursor validation failures categorized (invalid/expired/stale)
  - [ ] pruning operations and bytes reclaimed
- [ ] Metrics counters exist (can be simple internal counters):
  - [ ] advisory lock timeouts
  - [ ] upstream call latency and error types
  - [ ] mapping latency and stop reasons
  - [ ] prune deletions and disk bytes reclaimed

---

## 16) Test suite completion criteria (must pass)

At minimum, tests exist and pass for:

- [ ] RFC 8785 canonicalization vectors + numeric edge cases.
- [ ] Compression roundtrip integrity: compressed canonical bytes decompress to same bytes and hash matches.
- [ ] Reserved arg stripping removes only `_gateway_*` keys and explicit reserved names.
- [ ] Oversized JSON ingest becomes byte-backed binary_ref and is used for streaming mapping.
- [ ] Partial mapping determinism:
  - [ ] same payload + same budgets => same sample_indices + same root inventory
  - [ ] map_budget_fingerprint mismatch => stale behavior
- [ ] Prefix coverage semantics:
  - [ ] stop_reason != none => count_estimate null, prefix_coverage true, sampled_prefix_len correct
- [ ] Sampling bias invariant:
  - [ ] oversize records skipped and counted; sample_indices exclude them
- [ ] Cursor determinism:
  - [ ] same request and position => same cursor payload (before HMAC) and valid verification
- [ ] CURSOR_STALE conditions:
  - [ ] sample_set mismatch
  - [ ] where_canonicalization_mode mismatch
  - [ ] traversal_contract_version mismatch
  - [ ] artifact_generation mismatch
- [ ] Session discovery correctness:
  - [ ] artifact.search only returns artifacts in artifact_refs for that session
  - [ ] new artifact appears immediately
- [ ] Cleanup correctness:
  - [ ] payload_binary_refs prevents orphaning
  - [ ] hard delete removes filesystem blobs only when unreferenced

Integration tests (strongly recommended to count as done):

- [ ] A local upstream MCP stub (http or stdio) that can return:
  - [ ] small JSON, large JSON, text, errors, and binary payload
- [ ] End-to-end:
  - [ ] mirrored call -> artifact created -> artifact.search finds it -> artifact.get retrieves envelope -> mapping runs -> artifact.describe shows roots -> artifact.select returns projections -> cursor pagination works

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
- [ ] `README.md`
  - [ ] Explains local-only, single-tenant, `DATA_DIR`, Postgres DSN
  - [ ] Includes quickstart: run Postgres, migrate, run gateway, call mirrored tool
- [ ] `docs/spec_v1_9.md` (copy of the spec, locked)
- [ ] `docs/traversal_contract.md` (explicit ordering rules)
- [ ] `docs/cursor_contract.md` (payload fields, binding rules, stale rules)
- [ ] `docs/config.md` (all config keys + defaults)
- [ ] `.env.example`
- [ ] `docker-compose.yml` (optional but recommended for local Postgres)

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
  - [ ] All constraints and indexes exist (especially `created_seq` identity and ordering indexes)
- [ ] `src/mcp_artifact_gateway/db/migrations/002_indexes.sql` (optional if you split)
  - [ ] Adds the heavier indexes (request_key, created_seq, last_seen)

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

- [ ] `src/mcp_artifact_gateway/storage/payload_store.py`
  - [ ] `compress(bytes) -> (encoding, compressed, uncompressed_len)`
  - [ ] Supports `zstd|gzip|none`
  - [ ] Writes `payload_blobs` row with:
    - [ ] `envelope_canonical_bytes` compressed
    - [ ] `envelope_canonical_bytes_len`
    - [ ] `payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`
    - [ ] `contains_binary_refs`
    - [ ] `canonicalizer_version`
  - [ ] Enforces integrity:
    - [ ] `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))`
- [ ] JSONB storage mode implemented:
  - [ ] `full`
  - [ ] `minimal_for_large` projection
  - [ ] `none` projection

Acceptance

- [ ] Payload retrieval can reconstruct envelope from canonical bytes even if jsonb is minimal/none

---

## 7) Artifact creation flow (mirroring, caching, stampede lock)

### Upstream discovery + mirroring

- [ ] `src/mcp_artifact_gateway/mcp/upstream.py`
  - [ ] Connects to each upstream MCP (stdio/http)
  - [ ] Fetches tool list at startup
- [ ] `src/mcp_artifact_gateway/mcp/mirror.py`
  - [ ] Exposes mirrored tools as `{prefix}.{tool}` with identical schema/docs, no injected fields
  - [ ] Strips reserved keys before schema validation and forwarding:
    - [ ] exact keys: `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq`
    - [ ] any key starting with `_gateway_`
    - [ ] nothing else

### Request identity

- [ ] `src/mcp_artifact_gateway/request_identity.py`
  - [ ] Computes `upstream_instance_id` (semantic identity excluding secrets)
  - [ ] Computes `canonical_args_bytes` via RFC 8785 after reserved stripping and schema validation
  - [ ] `request_key = sha256(upstream_instance_id|prefix|tool|canonical_args_bytes)`
  - [ ] Persists `request_args_hash` and capped `request_args_prefix`

### Stampede lock and reuse

- [ ] `src/mcp_artifact_gateway/cache/reuse.py`
  - [ ] Advisory lock: derive two 32-bit keys from `sha256(request_key)` and `pg_advisory_lock` with timeout
  - [ ] If `cache_mode != fresh`, tries reuse by `request_key` latest (`created_seq desc`)
  - [ ] Strict gating by schema hash unless configured otherwise
  - [ ] Optional dedupe alias reuse (`payload_hash_aliases`) constrained to same upstream_instance_id + tool

### Artifact write

- [ ] `src/mcp_artifact_gateway/artifacts/create.py`
  - [ ] Implements the full step sequence in Section 9.1
  - [ ] Always stores an artifact even on upstream error/timeout (error envelope)
  - [ ] Inserts:
    - [ ] payload blob row
    - [ ] payload_binary_refs rows
    - [ ] artifact row with `map_status=pending`, `map_kind=none` initially
    - [ ] artifact_refs row and session last_seen update

Acceptance

- [ ] With DB and FS healthy, any upstream failure still yields a stored error artifact and returns a handle
- [ ] If DB or FS required path unavailable, gateway returns INTERNAL and does not claim artifact creation

---

## 8) Mapping system (full and partial)

### Mapping orchestrator

- [ ] `src/mcp_artifact_gateway/mapping/runner.py`
  - [ ] Picks JSON part to map deterministically with tie-break by part index
  - [ ] Decides full vs partial:
    - [ ] full if size <= `max_full_map_bytes`
    - [ ] partial if too large or stored as `binary_ref application/json(+encoding)`
  - [ ] Stores results in `artifact_roots`, updates artifact mapping columns

### Full mapper

- [ ] `src/mcp_artifact_gateway/mapping/full.py`
  - [ ] Parses fully, discovers up to K roots (K=3), builds deterministic inventory, writes `artifact_roots`

### Partial mapper (streaming, deterministic)

- [ ] `src/mcp_artifact_gateway/mapping/partial.py`
  - [ ] Consumes byte stream only (binary_ref stream preferred)
  - [ ] Enforces budgets and emits `stop_reason`
  - [ ] Computes and stores:
    - [ ] `map_backend_id` derived from python + ijson backend+version
    - [ ] `prng_version` constant
    - [ ] `map_budget_fingerprint` hash over budgets + versions
  - [ ] Root path normalization rules and no wildcards in root_path
  - [ ] Streaming skip contract: can discard subtrees; compute steps count all events
  - [ ] Deterministic reservoir sampling:
    - [ ] seed = sha256(payload_hash_full|root_path|map_budget_fingerprint)
    - [ ] reservoir algorithm exactly as specified
    - [ ] oversize sampled elements are skipped and counted (bias invariant)
    - [ ] sampled_prefix_len semantics
    - [ ] `sample_indices` stored sorted ascending and includes only materialized indices
    - [ ] count_estimate only when stop_reason none and closing array observed
  - [ ] Inventory derivation from sampled records with caps
  - [ ] If stop_reason != none:
    - [ ] prefix coverage true
    - [ ] count_estimate null

### Worker safety

- [ ] `src/mcp_artifact_gateway/mapping/worker.py`
  - [ ] Async/hybrid/sync modes supported
  - [ ] Conditional update safety:
    - [ ] deleted_at null
    - [ ] map_status in (pending, stale)
    - [ ] generation matches snapshot
    - [ ] else discard results

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
  - [ ] Implements `where_canonicalization_mode`:
    - [x] raw_string hash mode
    - [ ] canonical_ast mode with commutative sort and numeric/string normalization
  - [ ] Exposes mode via `gateway.status()`

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
  - [ ] Verification recomputes from DB and mismatch => CURSOR_STALE

Acceptance

- [ ] Cursor cannot be replayed against different where mode
- [ ] Cursor from old partial mapping becomes stale after remap (different fingerprint or sample indices)

---

## 11) MCP tool surface: gateway.status and artifact tools

### Tool server

- [ ] `src/mcp_artifact_gateway/mcp/server.py`
  - [ ] Registers gateway tools:
    - [ ] `gateway.status`
    - [ ] `artifact.search`
    - [ ] `artifact.get`
    - [ ] `artifact.select`
    - [ ] `artifact.describe`
    - [ ] `artifact.find`
    - [ ] `artifact.chain_pages`
  - [ ] Also registers mirrored upstream tools at `{prefix}.{tool}`

### Tool implementations

- [ ] `src/mcp_artifact_gateway/tools/status.py`
  - [ ] Returns: upstream connectivity, DB ok, FS ok, versions, traversal_contract_version, where mode, map_backend_id/prng_version, budgets, cursor TTL, secret versions
- [ ] `src/mcp_artifact_gateway/tools/artifact_search.py`
  - [ ] Lists artifacts using `artifact_refs` only
  - [ ] Touch policy: updates session/artifact_refs last_seen, does not touch artifact last_referenced
- [ ] `src/mcp_artifact_gateway/tools/artifact_get.py`
  - [ ] target `envelope` applies jsonpath on envelope root, reconstruct from canonical bytes if needed
  - [ ] target `mapped` only if map_status ready and map_kind full/partial
  - [ ] Touch semantics: touch last_referenced_at if not deleted, always update session/artifact_refs, else GONE
- [ ] `src/mcp_artifact_gateway/tools/artifact_select.py`
  - [ ] Full mapping: bounded deterministic scan
  - [ ] Partial mapping: sampled-only enumeration and response includes sampled_only, sample_indices_used, sampled_prefix_len
- [ ] `src/mcp_artifact_gateway/tools/artifact_describe.py`
  - [ ] Includes partial mapping disclosures: sampled-only constraints, prefix coverage, stop_reason, counts
- [ ] `src/mcp_artifact_gateway/tools/artifact_find.py`
  - [ ] Sample-only unless index enabled
- [ ] `src/mcp_artifact_gateway/tools/artifact_chain_pages.py`
  - [ ] Orders by chain_seq asc then created_seq asc, allocates chain_seq with retry

### Standard bounded response shape

- [x] `src/mcp_artifact_gateway/retrieval/response.py`
  - [x] Always returns `{items, truncated, cursor, omitted, stats}`

Acceptance

- [ ] All tools require `_gateway_context.session_id` and reject missing with INVALID_ARGUMENT
- [ ] Any truncation yields deterministic cursor and position encoding per traversal contract

---

## 12) Session tracking and touch policy

### Session enforcement

- [ ] `src/mcp_artifact_gateway/sessions.py`
  - [ ] Creates or updates session row with last_seen_at
  - [ ] Upserts artifact_refs (first_seen_at, last_seen_at)

### Touch rules

- [ ] Implemented exactly:
  - [ ] creation touches artifacts.last_referenced_at
  - [ ] retrieval/describe touches if not deleted
  - [ ] search does not touch

Acceptance

- [ ] Prune policies behave correctly because touch semantics are correct

---

## 13) Pruning, hard delete, and filesystem cleanup

### Soft delete job

- [ ] `src/mcp_artifact_gateway/jobs/soft_delete.py`
  - [ ] Uses SKIP LOCKED, rechecks predicates on update, sets deleted_at and generation++

### Hard delete job

- [ ] `src/mcp_artifact_gateway/jobs/hard_delete.py`
  - [ ] Deletes artifacts, cascades remove artifact_roots and artifact_refs
  - [ ] Deletes unreferenced payload_blobs
  - [ ] Deletes unreferenced binary_blobs via payload_binary_refs
  - [ ] Removes filesystem blobs for removed binary_blobs

### Reconciler (optional but strongly recommended)

- [ ] `src/mcp_artifact_gateway/jobs/reconcile_fs.py`
  - [ ] Finds orphan files not referenced in DB and optionally removes them

Acceptance

- [ ] End-to-end: create artifacts with binaries, delete them, filesystem blobs disappear only when unreferenced

---

## 14) Observability, metrics, and determinism logging

- [ ] `src/mcp_artifact_gateway/obs/logging.py`
  - [ ] structlog configuration, JSON logs
  - [ ] Correlation fields: session_id, artifact_id, request_key, payload_hash_full
- [ ] `src/mcp_artifact_gateway/obs/metrics.py` (optional)
  - [ ] Counters:
    - [ ] cache hits, alias hits, upstream calls
    - [ ] oversize JSON count
    - [ ] partial map stop_reason distribution
    - [ ] cursor stale reasons
    - [ ] advisory lock timeouts
- [ ] Determinism debug logs:
  - [ ] map_budget_fingerprint
  - [ ] map_backend_id
  - [ ] prng_version
  - [ ] sample_set_hash on cursor issue/verify

Acceptance

- [ ] Given a cursor stale event, logs show which binding field mismatched

---

## 15) Test suite, fixtures, and done gates

### Unit tests (must exist)

- [ ] `tests/test_reserved_arg_stripping.py`
  - [ ] Only `_gateway_*` removed, nothing else
- [x] `tests/test_rfc8785_vectors.py`
- [x] `tests/test_decimal_json_no_float.py`
- [ ] `tests/test_payload_canonical_integrity.py`
- [x] `tests/test_oversize_json_becomes_binary_ref.py`
- [ ] `tests/test_partial_mapping_determinism.py`
  - [ ] same bytes + same budgets => identical sample_indices and fields_top
- [ ] `tests/test_prefix_coverage_semantics.py`
  - [ ] stop_reason != none implies count_estimate null and prefix coverage true
- [ ] `tests/test_sampling_bias_invariant.py`
  - [ ] oversize sampled elements are skipped and counted
- [x] `tests/test_cursor_sample_set_hash_binding.py`
- [x] `tests/test_cursor_where_mode_stale.py`
- [ ] `tests/test_touch_policy.py`
- [x] Additional unit tests added beyond spec minimums (config loading, traversal, jsonpath, hashing, stores, bounded response, migrations)

### Integration tests (must exist)

- [ ] `tests/integration/test_full_flow_small_json.py`
  - [ ] mirrored call -> artifact created -> mapping ready -> select works -> cursor pages
- [ ] `tests/integration/test_flow_large_json_partial_map.py`
  - [ ] oversize JSON stored as binary_ref -> partial mapping reads stream -> sampled-only select works
- [ ] `tests/integration/test_prune_cleanup.py`
  - [ ] soft delete then hard delete cleans DB and filesystem

### Ship gate criteria

- [ ] All tests pass in CI on Linux
- [ ] A local demo script produces:
  - [ ] one small JSON artifact that returns inline envelope
  - [ ] one large JSON artifact that returns handle and supports sampled-only select
  - [ ] cursor pagination stable across two identical runs
- [ ] No tool ever returns unbounded bytes/items
- [ ] Determinism artifacts are visible: traversal_contract_version, map_budget_fingerprint, sample_set_hash appear in responses where relevant
