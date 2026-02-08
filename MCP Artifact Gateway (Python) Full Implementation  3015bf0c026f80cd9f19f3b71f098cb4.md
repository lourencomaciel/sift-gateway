# MCP Artifact Gateway (Python) Full Implementation Spec v1.9 Local Single-Tenant

A local-only MCP gateway that proxies upstream MCP tools, stores every tool result (success or failure) as a durable artifact envelope on disk plus Postgres metadata, returns compact handles instead of large payloads, generates deterministic inventories for selective retrieval, and enforces bounded, deterministic retrieval with signed cursors. This system is explicitly single-tenant and designed to run locally.

v1.9 finalizes production-safe partial mapping with deterministic reservoir sampling, binds partial-map budgets and stored sample sets into cursors, requires byte-backed oversized JSON for streaming, preserves integrity with compressed canonical envelope bytes in Postgres, tightens reserved arg stripping, formalizes root_path, and defines canonical cursor hashing for select_paths and where.

---

## 0. Glossary

- Upstream MCP: any MCP server the gateway calls (stdio or http).
- Gateway: this project. An MCP server to clients and an MCP client to upstreams.
- MCP result: upstream output, may be multi-part (json/text/resource/binary) or error.
- Envelope: canonical stored representation of an MCP result (success or error).
- Artifact: provenance record referencing a stored envelope plus mapping/index metadata.
- Payload blob: stored envelope representation, identity by full canonical hash.
- Payload canonical bytes: RFC 8785 canonical JSON bytes used for hashing; stored compressed for integrity and stable retrieval.
- Binary blob: stored bytes on filesystem; identity by content hash.
- Session: local workflow grouping key for discovery and continuity.
- Cursor: signed and bound continuation token for truncated retrieval.
- Mapping: deterministic inventory generation from stored JSON parts (full or partial).
- Partial mapping: streaming-derived inventory and bounded sampling for oversized JSON, enabling limited selection without full parse.
- Traversal contract: explicit deterministic ordering rules for retrieval and pagination.
- map_budget_fingerprint: hash of partial mapping budgets and backend identity; used for staleness detection and cursor binding.
- map_backend_id: stable identifier for the streaming parse backend and runtime.
- prng_version: stable identifier for the deterministic PRNG algorithm used in sampling.
- sample_set_hash: hash binding partial cursors to the stored sample indices.
- sampled_prefix_len: number of root-array elements whose boundaries were successfully recognized before stopping (includes skipped and non-materialized elements).
- stop_reason: why streaming stopped (none|max_bytes|max_compute|max_depth|parse_error).
- prefix coverage: when stop_reason != none, partial mapping represents a deterministic sample of the processed prefix, not the full dataset.

---

## 1. Purpose and invariants

### 1.1 Purpose

Enable complex MCP workflows without clogging model context:

- store upstream tool outputs as artifacts
- return handles for large outputs
- allow selective retrieval by query and path without loading full payloads

### 1.2 Invariants

1. Every upstream tool call yields an envelope and is stored, including errors/timeouts.
2. Payload identity uses a full canonical hash of the exact stored envelope canonical bytes.
3. Binary bytes are stored on filesystem, not in Postgres.
4. Binary identity is content-based and deterministic.
5. Dedupe decisions use a separate dedupe hash that never determines storage identity.
6. Retrieval is bounded by bytes/items/compute budgets; truncation is structured and deterministic.
7. Cursor tokens are signed, bound, versioned, and depend on an explicit traversal contract.
8. Session discovery always includes newly created artifacts immediately.
9. Mapping may run async to avoid tail-latency inflation; worker writes are conditional and race-safe.
10. Canonicalization is strict, versioned, and locked by tests.
11. JSON numeric handling is canonicalization-safe (no float round-trip drift).
12. Binary reference tracking is complete and cleanup-safe (no JSONB scanning required for refs).
13. Oversized JSON remains queryable through partial mapping and bounded sampling.
14. Partial mapping determinism is defined relative to a recorded map_budget_fingerprint.
15. Partial mapping cursors are bound to stored sample sets (sample_set_hash), not recomputation.
16. Partial-mode samples are biased toward records that fit within max_record_bytes_partial and max_depth_partial_map; this bias is an explicit invariant.
17. If stop_reason != none, partial mapping is a deterministic sample of the processed prefix only; coverage is prefix coverage, not dataset coverage.

---

## 2. Local single-tenant model

### 2.1 Single-tenant constant workspace

- The system has exactly one workspace: workspace_id = “local”.
- No multi-tenant support and no concept of principals.

### 2.2 Access model

- Local machine access is assumed.
- All data is accessible within the local workspace.
- Session is a workflow grouping mechanism; it is not an access boundary.

---

## 3. Gateway context, sessions, and schema collision avoidance

### 3.1 Gateway-only context field

Clients provide gateway context under a reserved field:

```
"_gateway_context": {
  "session_id": "uuid-or-opaque",
  "cache_mode": "allow|fresh"
}
```

Gateway context is never forwarded upstream.

### 3.2 Session requirements

_gateway_context.session_id is required for:

- mirrored tool calls
- artifact.search
- any retrieval tool

If missing, gateway returns INVALID_ARGUMENT.

### 3.3 Session tracking

The gateway maintains:

- sessions table: created_at and last_seen_at
- artifact_refs table: which artifacts were produced/seen in a session

Session discovery uses artifact_refs exclusively.

---

## 4. Upstream MCP discovery, mirroring, and request identity

### 4.1 Upstream discovery at startup

On startup:

1. validate config, limits, and filesystem paths
2. connect to Postgres, verify migrations
3. connect to each upstream MCP
4. fetch tool list
5. expose mirrored tools as {prefix}.{tool} with identical schema and docs (no injected fields)

### 4.2 Reserved gateway args (never forwarded)

Before schema validation and upstream forwarding, the gateway strips only:

- keys equal to: _gateway_context, _gateway_parent_artifact_id, _gateway_chain_seq
- any key whose name begins with the exact prefix _gateway_

No other keys are stripped. Keys like gateway_url MUST NOT be stripped unless they match _gateway_.

### 4.3 Upstream instance identity (semantic only)

Each upstream has an immutable semantic identity that excludes rotating secrets:

upstream_instance_id = sha256(canonical_semantic_identity_bytes)[:32]

Semantic identity includes:

- transport type (stdio/http)
- endpoint identity:
    - http: url origin + stable routing identity
    - stdio: command path + argv excluding env secrets
- upstream name/prefix
- toolset version hint if available
- optional semantic salt (configurable):
    - stable, non-secret headers that affect semantics
    - stable, non-secret env key/value pairs that affect semantics
    - optional config file fingerprints (path + sha256(file bytes)) when relevant

It excludes:

- bearer tokens, API keys, rotating headers
- env values containing secrets
- file paths to private keys

Store upstream_auth_fingerprint (optional) for debugging changes; it does not affect request identity.

### 4.4 request_key definition

request_key = sha256(

upstream_instance_id + “|” + prefix + “|” + tool_name + “|” + canonical_args_bytes

)

canonical_args_bytes is RFC 8785 canonical JSON of forwarded args after:

- stripping only keys described in 4.2
- validating against upstream tool schema
- applying only explicit, tool-configured, unambiguous primitive defaults (optional feature)

Request args persistence:

- request_args_hash = sha256(canonical_args_bytes)
- request_args_prefix: UTF-8 safe prefix of canonical JSON text (capped)
- enforce caps before parsing/logging

### 4.5 Deterministic selection of “latest”

Whenever selecting “latest” among candidates, order by created_seq desc (monotonic identity column in artifacts).

---

## 5. Canonical envelope format

### 5.1 Envelope definition

Every upstream tool call is normalized to:

```
{
  "type": "mcp_envelope",
  "upstream_instance_id": "...",
  "upstream_prefix": "github",
  "tool": "search_issues",
  "status": "ok|error",
  "content": [
    { "type": "json", "value": { } },
    { "type": "text", "text": "..." },
    {
      "type": "resource_ref",
      "uri": "file://...",
      "mime": "application/pdf",
      "name": "...",
      "durability": "internal|external_ref",
      "content_hash": "sha256:..."
    },
    {
      "type": "binary_ref",
      "blob_id": "bin_...",
      "binary_hash": "...",
      "mime": "image/png",
      "byte_count": 123456
    }
  ],
  "error": {
    "code": "UPSTREAM_TIMEOUT|UPSTREAM_ERROR|TRANSPORT_ERROR|INVALID_RESPONSE|INTERNAL",
    "message": "...",
    "retryable": true,
    "upstream_trace_id": "...",
    "details": { }
  },
  "meta": {
    "upstream_pagination": { "next_cursor": "...", "has_more": true, "total": 123 },
    "warnings": []
  }
}
```

Rules:

- status=ok implies error is null/absent.
- status=error implies error is present; content may be empty/partial.
- Binary bytes never appear in envelope; only binary_ref does.
- resource_ref durability:
    - internal: gateway has copied content under DATA_DIR/resources; content_hash required.
    - external_ref: gateway did not copy; content_hash optional best-effort.
- The envelope is hashed using RFC 8785 canonical JSON bytes, not the jsonb storage form.

### 5.2 Supported content parts

- json: JSON-serializable value parsed with Decimal-safe rules (no Python float).
- text: UTF-8 text.
- resource_ref: uri + optional mime/name + durability + optional content_hash.
- binary_ref: references content-addressed filesystem blob.
- image_ref: alias of binary_ref.

### 5.3 Error envelopes (mandatory)

All upstream failures/timeouts produce a stored artifact with status=error. The gateway call returns a handle (and optionally inline small envelope) rather than failing the gateway protocol unless the gateway itself is unhealthy (DB or DATA_DIR unavailable).

---

## 6. Binary storage on filesystem (content-addressed)

### 6.1 Identity

- binary_hash = sha256(raw_bytes).hexdigest()
- blob_id = “bin_” + binary_hash[:32]

### 6.2 Filesystem layout

DATA_DIR default: .mcp_gateway/

BIN_DIR = DATA_DIR/blobs/bin

Path: BIN_DIR / h[0:2] / h[2:4] / binary_hash

### 6.3 Atomic writes

- write temp file in same directory
- fsync temp file
- atomic rename to final path

If exists:

- verify size equals expected byte_count
- optional integrity probe:
    - probe_bytes configurable (e.g., 65536)
    - store probe_head_hash and probe_tail_hash

### 6.4 Binary metadata in Postgres

binary_blobs stores: binary_hash, blob_id, byte_count, mime, fs_path, optional probe fields, created_at.

### 6.5 MIME normalization

- lowercase
- strip parameters
- alias map (config)

Mime never affects identity.

---

## 7. Payload identity, dedupe hashes, and compressed canonical bytes

### 7.1 payload_hash_full (storage identity)

payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)

envelope_canonical_bytes_uncompressed are RFC 8785 canonical JSON bytes of the envelope (no exclusions).

### 7.2 payload_hash_dedupe (reuse hash)

Optionally compute payload_hash_dedupe from an envelope copy with volatile fields removed. Used only for reuse lookup.

Volatile exclusions are opt-in per upstream tool, defined as JSONPath list (subset grammar in 12.3). Exclusions apply only to dedupe hash input.

### 7.3 Canonical bytes storage (compressed)

Store canonical bytes compressed:

- envelope_canonical_encoding = zstd|gzip|none
- envelope_canonical_bytes = compressed bytes
- envelope_canonical_bytes_len = uncompressed length

Integrity rule:

payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))

The gateway must be able to decompress and obtain exact original canonical bytes.

---

## 8. Postgres schema (self-contained) v1.9

Workspace is constant “local”. Tables include workspace_id for future-proofing, but there is no multi-tenant behavior.

### 8.1 Common conventions

- All primary keys include workspace_id.
- Timestamps are timestamptz with default now().
- Soft delete uses deleted_at in artifacts.
- Large bytes stored in Postgres are limited to envelope_canonical_bytes (compressed) and envelope jsonb (configurable).

### 8.2 Table: sessions

Columns:

- workspace_id text not null default ‘local’
- session_id text not null
- created_at timestamptz not null default now()
- last_seen_at timestamptz not null default now()

PK:

- (workspace_id, session_id)

Indexes:

- (workspace_id, last_seen_at desc)

### 8.3 Table: binary_blobs

Columns:

- workspace_id text not null default ‘local’
- binary_hash text not null
- blob_id text not null
- byte_count bigint not null
- mime text null
- fs_path text not null
- probe_head_hash text null
- probe_tail_hash text null
- probe_bytes int null
- created_at timestamptz not null default now()

PK:

- (workspace_id, binary_hash)

Unique:

- (workspace_id, blob_id)

Indexes:

- (workspace_id, created_at desc)
- (workspace_id, byte_count)

### 8.4 Table: payload_blobs

Config:

- envelope_jsonb_mode = full | minimal_for_large | none
- envelope_jsonb_minimize_threshold_bytes (used when minimal_for_large)

Columns:

- workspace_id text not null default ‘local’
- payload_hash_full text not null
- envelope jsonb not null
- envelope_canonical_encoding text not null CHECK (envelope_canonical_encoding in (‘zstd’,‘gzip’,‘none’))
- envelope_canonical_bytes bytea not null
- envelope_canonical_bytes_len int not null CHECK (envelope_canonical_bytes_len >= 0)
- canonicalizer_version text not null
- payload_json_bytes int not null CHECK (payload_json_bytes >= 0)
- payload_binary_bytes_total bigint not null CHECK (payload_binary_bytes_total >= 0)
- payload_total_bytes bigint not null CHECK (payload_total_bytes >= 0)
- contains_binary_refs boolean not null
- created_at timestamptz not null default now()

PK:

- (workspace_id, payload_hash_full)

Indexes:

- (workspace_id, created_at desc)
- (workspace_id, payload_total_bytes)

Envelope jsonb storage rules:

- full: store envelope jsonb exactly as produced.
- minimal_for_large: if payload_json_bytes > envelope_jsonb_minimize_threshold_bytes, store minimal projection:
    - type, upstream_instance_id, upstream_prefix, tool, status
    - content descriptors only (type, sizes, refs, mime, byte_count)
    - error summary fields
    - meta.warnings and upstream_pagination metadata
    - canonical bytes remain the source of truth for full reconstruction.
- none: store minimal fixed projection (same as above, always). canonical bytes are the source of truth.

Retrieval rule:

- If envelope jsonb is minimized or none, artifact.get target=envelope reconstructs by parsing canonical bytes and applies retrieval bounds.

### 8.5 Table: payload_hash_aliases

Columns:

- workspace_id text not null default ‘local’
- payload_hash_dedupe text not null
- payload_hash_full text not null
- upstream_instance_id text not null
- tool text not null
- created_at timestamptz not null default now()

PK:

- (workspace_id, payload_hash_dedupe, payload_hash_full)

FK:

- (workspace_id, payload_hash_full) references payload_blobs(workspace_id, payload_hash_full) ON DELETE CASCADE

Indexes:

- (workspace_id, payload_hash_dedupe, created_at desc)

### 8.6 Table: payload_binary_refs (mandatory)

Columns:

- workspace_id text not null default ‘local’
- payload_hash_full text not null
- binary_hash text not null
- created_at timestamptz not null default now()

PK:

- (workspace_id, payload_hash_full, binary_hash)

FKs:

- (workspace_id, payload_hash_full) references payload_blobs(workspace_id, payload_hash_full) ON DELETE CASCADE
- (workspace_id, binary_hash) references binary_blobs(workspace_id, binary_hash) ON DELETE CASCADE

Indexes:

- (workspace_id, binary_hash)
- (workspace_id, created_at desc)

### 8.7 Table: artifacts

Columns:

- workspace_id text not null default ‘local’
- artifact_id text not null
- created_seq bigint generated always as identity
- session_id text not null
- source_tool text not null
- upstream_instance_id text not null
- upstream_tool_schema_hash text null
- request_key text not null
- request_args_hash text not null
- request_args_prefix text not null
- payload_hash_full text not null
- canonicalizer_version text not null
- payload_json_bytes int not null CHECK (payload_json_bytes >= 0)
- payload_binary_bytes_total bigint not null CHECK (payload_binary_bytes_total >= 0)
- payload_total_bytes bigint not null CHECK (payload_total_bytes >= 0)
- created_at timestamptz not null default now()
- expires_at timestamptz null
- deleted_at timestamptz null
- last_referenced_at timestamptz not null default now()
- generation int not null default 1 CHECK (generation >= 1)
- parent_artifact_id text null
- chain_seq int null

Mapping:

- map_kind text not null CHECK (map_kind in (‘none’,‘full’,‘partial’))
- map_status text not null CHECK (map_status in (‘pending’,‘ready’,‘failed’,‘stale’))
- mapped_part_index int null
- mapper_version text not null
- map_budget_fingerprint text null
- map_backend_id text null
- prng_version text null
- map_error jsonb null

Indexing:

- index_status text not null CHECK (index_status in (‘off’,‘pending’,‘ready’,‘partial’,‘failed’))
- error_summary text null

PK:

- (workspace_id, artifact_id)

FKs:

- (workspace_id, session_id) references sessions(workspace_id, session_id) ON DELETE RESTRICT
- (workspace_id, payload_hash_full) references payload_blobs(workspace_id, payload_hash_full) ON DELETE RESTRICT
- (workspace_id, parent_artifact_id) references artifacts(workspace_id, artifact_id) ON DELETE SET NULL

Unique:

- (workspace_id, parent_artifact_id, chain_seq) where chain_seq is not null

Indexes:

- (workspace_id, session_id, created_seq desc)
- (workspace_id, request_key, created_seq desc)
- (workspace_id, created_seq desc)
- (workspace_id, expires_at) where deleted_at is null and expires_at is not null
- (workspace_id, deleted_at) where deleted_at is not null
- (workspace_id, last_referenced_at)
- (workspace_id, parent_artifact_id, created_seq desc)

### 8.8 Table: artifact_refs

Columns:

- workspace_id text not null default ‘local’
- session_id text not null
- artifact_id text not null
- first_seen_at timestamptz not null default now()
- last_seen_at timestamptz not null default now()

PK:

- (workspace_id, session_id, artifact_id)

FKs:

- (workspace_id, session_id) references sessions(workspace_id, session_id) ON DELETE CASCADE
- (workspace_id, artifact_id) references artifacts(workspace_id, artifact_id) ON DELETE CASCADE

Indexes:

- (workspace_id, session_id, last_seen_at desc)

### 8.9 Table: artifact_roots

Columns:

- workspace_id text not null default ‘local’
- artifact_id text not null
- root_key text not null
- root_path text not null
- count_estimate int null
- inventory_coverage double precision not null CHECK (inventory_coverage >= 0.0 and inventory_coverage <= 1.0)
- root_summary text not null
- root_score double precision not null
- root_shape jsonb not null
- fields_top jsonb not null
- examples jsonb not null
- recipes jsonb not null
- sample_indices jsonb not null

PK:

- (workspace_id, artifact_id, root_key)

FK:

- (workspace_id, artifact_id) references artifacts(workspace_id, artifact_id) ON DELETE CASCADE

Indexes:

- (workspace_id, artifact_id)
- (workspace_id, root_path)

---

## 9. Artifact creation flow

### 9.1 Mirrored tool call handling

Given {prefix}.{tool}(args…, _gateway_context, optional gateway*):

1. Validate _gateway_context.session_id.
2. cache_mode default allow.
3. Strip only reserved keys per 4.2.
4. Validate forwarded args against upstream schema.
5. Compute canonical args bytes and request_key.
6. Acquire stampede lock:
    - derive two 32-bit advisory lock keys from sha256(request_key)
    - use pg_advisory_lock(key1, key2) with timeout
    - on timeout proceed and record metric
7. If cache_mode != fresh:
    - attempt reuse by request_key latest (created_seq desc)
    - optionally via payload_hash_dedupe aliases (tool-configured)
    - reuse eligibility:
        - artifact.deleted_at is null
        - not expired
        - schema hash matches if strict reuse enabled
        - alias reuse constrained to same upstream_instance_id + tool
    - if reuse chosen: return handle and release lock
8. Call upstream tool (capture success or error).
9. Normalize into envelope:
    - parse JSON with Decimal-safe loader unless oversized handling applies
    - extract binaries to filesystem; upsert binary_blobs and payload_binary_refs
    - handle resource_ref durability and optional internal copy
10. Oversized JSON rule (byte-backed):
- If any JSON part exceeds max_json_part_parse_bytes:
    - do not parse into structured value
    - store raw bytes as binary_ref with mime application/json(+encoding)
    - replace that json content entry with a binary_ref descriptor
    - add meta warning with original_part_index and encoding
1. Produce envelope canonical bytes (RFC 8785).
2. Compute payload_hash_full and compress canonical bytes per config.
3. Compute byte metrics (payload_json_bytes, payload_binary_bytes_total, payload_total_bytes).
4. Upsert payload_blobs.
5. Upsert payload_binary_refs for each binary_ref.
6. Optionally compute payload_hash_dedupe; upsert payload_hash_aliases.
7. Insert artifacts row:
- map_kind = ‘none’ initially
- map_status = ‘pending’
- index_status = ‘off’ unless enabled
- mapper_version set (even if mapping pending)
- last_referenced_at = now()
1. Upsert artifact_refs and sessions last_seen.
2. Return inline envelope only if small and no binary_refs and inline allowed; otherwise return handle.

### 9.2 Gateway failure conditions

If gateway cannot write to Postgres or required filesystem paths, return INTERNAL and do not claim artifact creation. Otherwise, upstream failures still create artifacts as error envelopes.

---

## 10. Canonicalization and numeric handling

### 10.1 Canonicalization

RFC 8785 canonical JSON is used for:

- request args canonical bytes
- schema hashing bytes
- envelope canonical bytes

### 10.2 Numeric handling

- parse floats as Decimal
- parse ints as int by default (optional Decimal via config)
- reject NaN/Infinity
- never pass Python float into hashing or canonical serialization

### 10.3 Versioning

- canonicalizer_version constant stored in payload_blobs and artifacts.
- changes require version bump; old payloads remain retrievable.

---

## 11. Tool schema hashing and reuse gating

### 11.1 upstream_tool_schema_hash

- canonicalize upstream tool schema via RFC 8785
- upstream_tool_schema_hash = sha256(canonical_schema_bytes)

Store in artifacts.

### 11.2 Reuse gating

- default strict: require schema hash match for reuse
- configurable per tool

---

## 12. Retrieval tools (bounded, deterministic)

All tools require _gateway_context.session_id.

### 12.1 gateway.status()

Returns:

- upstream connectivity
- DB ok
- filesystem paths
- canonicalizer_version
- mapper_version
- traversal_contract_version
- where_canonicalization_mode
- map_backend_id and prng_version
- configured limits and budgets
- cursor_ttl_minutes and active cursor_secret_version list

### 12.2 artifact.search(session_id, filters)

Returns artifacts from artifact_refs for that session.

Defaults:

- exclude deleted artifacts
- ordering default created_seq desc; optional last_seen desc

On search:

- update artifact_refs.last_seen_at and sessions.last_seen_at
- do not touch artifacts.last_referenced_at

### 12.3 JSONPath subset grammar

Supported:

- $
- .name where name matches [A-Za-z_][A-Za-z0-9_]*
- [’…’] with escapes: \\, \', \n, \r, \t
- [n] integer
- [*]

No filters.

Caps:

- max_jsonpath_length
- max_path_segments
- max_wildcard_expansion_total

### 12.3.1 Canonicalization for select_paths and where (cursor binding)

select_paths semantics:

- select_paths is a set (unordered projection).
- the gateway returns projected values in canonical path order.

select_paths canonicalization:

- normalize each path (whitespace removal, canonical bracket escaping, canonical quotes)
- relative paths MUST NOT start with $
- sort lexicographically
- remove duplicates
- select_paths_hash = sha256(canonical_json(select_paths_canonical_array))

where hashing:

Config where_canonicalization_mode:

- raw_string (default): where_hash = sha256(UTF-8 bytes of where exactly as received)
- canonical_ast (optional): parse to AST, canonicalize (commutative sort, numeric literal normalization, string escape normalization, remove redundant parentheses), then where_hash = sha256(canonical_json(ast))

gateway.status() must expose the mode.

### 12.4 Traversal contract

traversal_contract_version constant (example: traversal_v1).

Rules:

- Arrays: ascending index order.
- Objects: key enumeration lexicographic ascending.
- Wildcard expansions obey these rules.
- Sampled record enumeration in partial mode is ascending sample_indices.

Any change requires cursor_version bump.

### 12.5 Standard retrieval response shape

```
{
  "items": [],
  "truncated": false,
  "cursor": null,
  "omitted": {
    "omitted_count": 0,
    "reason": "max_bytes|max_items|max_compute|max_wildcards",
    "budgets": {},
    "position": {}
  },
  "stats": {}
}
```

### 12.6 artifact.get

Inputs:

- artifact_id
- target: envelope|mapped
- jsonpath
- cursor optional
- optional limits overrides (<= server caps)

Behavior:

- envelope: evaluate jsonpath on envelope root; may reconstruct from canonical bytes if jsonb minimized.
- mapped: requires map_status=ready and map_kind in (full, partial).
- partial mapped access limited to inventory metadata and sampled records only.

Touch semantics:

- touch artifacts.last_referenced_at only if deleted_at is null
- always update artifact_refs and sessions last_seen
- else return GONE

### 12.7 artifact.select

Inputs:

- artifact_id
- root_path
- select_paths
- where optional
- limits + cursor

Full mapping:

- bounded scan in deterministic order.

Partial mapping:

- sampled-only:
    - enumerate sample_indices ascending
    - evaluate where and select_paths only on sampled records
    - return sampled_only=true, sample_indices_used, sampled_prefix_len

### 12.8 artifact.describe

Includes:

- mapping status/kind
- mapper_version
- for partial mapping: sampled-only constraints, prefix_coverage indicator, stop_reason, sampled_prefix_len, sampled_record_count, skipped_oversize_records
- root inventory and fields_top
- count_estimate only when known (see mapping rules)

Touch semantics as retrieval.

### 12.9 artifact.find

Sample-only unless index enabled.

### 12.10 artifact.chain_pages

Order by chain_seq asc then created_seq asc. Allocate chain_seq with retry if not provided.

## 13. Mapping (full + partial)

### 13.1 Mapping modes

mapping_mode = async | hybrid | sync. Default hybrid.

### 13.2 JSON part selection scoring

Deterministic score; choose mapped_part_index with tie-break by part index ascending.

### 13.3 Full mapping

If selected json part size <= max_full_map_bytes:

- parse fully
- discover up to K roots (K=3)
- build inventory deterministically
- store artifact_roots
- map_kind=full, map_status=ready

### 13.4 Partial mapping trigger

If selected json part is too large for full parse or is stored as binary_ref application/json(+encoding), run partial mapping and set map_kind=partial.

### 13.5 Partial mapping (streaming, bounded, deterministic)

13.5.1 Byte-backed requirement

Partial mapping MUST use a byte stream. Valid sources:

1. binary_ref application/json(+encoding)
2. text JSON (bounded)
3. re-canonicalized bytes for small structured json value (bounded)

If a JSON part exceeded max_json_part_parse_bytes at ingest, it MUST be stored as binary_ref and partial mapping MUST read from that blob.

13.5.2 Budgets and stop_reason

Partial mapping budgets:

- max_bytes_read_partial_map
- max_compute_steps_partial_map
- max_depth_partial_map
- max_records_sampled_partial (N)
- max_record_bytes_partial
- max_leaf_paths_partial
- max_root_discovery_depth

stop_reason:

- none | max_bytes | max_compute | max_depth | parse_error

Prefix coverage rule:

- If stop_reason != none:
    - the sample is a deterministic sample of the processed prefix only (not the dataset).
    - count_estimate MUST be null.
    - root_shape MUST include prefix_coverage=true.

13.5.3 Backend and PRNG identity (pinned)

map_backend_id MUST be:

map_backend_id = sha256(

“py=” + python_version +

“|ijson=” + ijson_backend_name +

“|ijson_ver=” + ijson_backend_version

)[:16]

prng_version is a code constant (example: prng_xoshiro256ss_v1).

Both MUST be returned by gateway.status() and stored on artifacts for partial mappings.

13.5.4 map_budget_fingerprint (mandatory for partial)

map_budget_fingerprint = sha256(canonical_json({

“mapper_version”: mapper_version,

“traversal_contract_version”: traversal_contract_version,

“map_backend_id”: map_backend_id,

“prng_version”: prng_version,

“max_bytes_read_partial_map”: …,

“max_compute_steps_partial_map”: …,

“max_depth_partial_map”: …,

“max_records_sampled_partial”: …,

“max_record_bytes_partial”: …,

“max_leaf_paths_partial”: …,

“max_root_discovery_depth”: …

}))[:32]

Store on artifacts.

Staleness:

- If remapped with different map_budget_fingerprint, prior partial mapping is stale for mapped operations and cursors.

13.5.4.1 Root path representation (stable identifier)

root_path is an absolute JSONPath-subset string that identifies the root location within the mapped JSON part.

Grammar:

- begins with $
- segments:
    - .name for identifier keys
    - [’…’] for non-identifier keys with canonical escaping per 12.3
    - [n] integer index only when the root is nested under a fixed index
- root_path MUST NOT include [*] wildcards

Examples:

- mapped part is array: root_path = $
- nested arrays: $.data.items
- special keys: $.result[‘edge-cases’].rows

Determinism:

- if a key can be .name, MUST use .name; else must use bracket form.
- root_path normalization MUST use the same escaping and canonicalization rules as 12.3.

root_path is used in cursor binding; formatting changes require traversal_contract_version bump.

13.5.4.2 Streaming skip contract and compute accounting

The streaming parser MUST support skipping unselected subtrees without building full in-memory trees.

Requirements:

- for non-selected elements, consume and discard streaming events without constructing dict/list trees
- compute_steps MUST count all streaming events processed, including those discarded during skips
- budgets are enforced over total events processed, not only materialized records

13.5.5 Deterministic reservoir sampling per root (one-pass, prefix-bounded)

Reservoir sampling provides uniform selection over the processed indices only.

For each chosen root array:

- seed = sha256(payload_hash_full + “|” + root_path + “|” + map_budget_fingerprint)
- PRNG is deterministic with prng_version

Maintain a reservoir of capacity N:

- For each element index i:
    - If reservoir size < N: include i
    - Else:
        - r = randint(0, i)
        - if r < N, replace reservoir[r] with i

Materialization and bias invariant:

- Materialize only selected elements.
- If selected element exceeds max_record_bytes_partial or max_depth_partial_map:
    - increment skipped_oversize_records
    - do not store payload for that index
- Therefore, the stored sample_indices are biased toward records within caps; this is an invariant.

Processed prefix accounting:

- sampled_prefix_len counts each root array element index whose boundaries were successfully recognized by the parser, regardless of whether it was materialized or skipped.
- If stop_reason == parse_error and the parse error occurs mid-element, sampled_prefix_len is the last fully recognized element index + 1.

Finalization:

- sample_indices MUST include only indices whose payload was materialized successfully.
- sample_indices MUST be sorted ascending for stable retrieval order.
- sampled_prefix_len MUST equal the processed prefix length defined above.
- count_estimate:
    - MAY be set only if stop_reason == none AND the parser observed the closing token of the root array.
    - MUST be null otherwise.

Coverage:

- If stop_reason == none and count_estimate known:
    - inventory_coverage = sampled_record_count / count_estimate
- Else:
    - inventory_coverage = sampled_record_count / max(1, sampled_prefix_len)
    - root_shape.prefix_coverage = (stop_reason != none)

13.5.6 Inventory derivation on sampled records

For each root:

- compute leaf paths and frequencies from materialized sampled records
- cap at max_leaf_paths_partial; tie-break by path lex order
- root_shape MUST include:
    - sampled_only=true
    - prefix_coverage boolean
    - stop_reason
    - sampled_prefix_len
    - sampled_record_count
    - skipped_oversize_records
    - bytes_read
    - compute_steps

### 13.6 Partial retrieval contract (strict)

When map_kind=partial:

- artifact.select operates only over sampled records.
- stored retrieval enumerates sample_indices ascending.
- where and select_paths evaluated only on sampled records.
- responses include sampled_only=true, sample_indices_used, sampled_prefix_len.

If prefix_coverage=true, artifact.describe MUST state:

- “This inventory and sample reflect only the processed prefix; dataset-wide conclusions are invalid.”

### 13.7 Worker safety

Worker updates are conditional:

- only if deleted_at is null
- map_status in (pending, stale)
- generation matches row at start

If update affects 0 rows, discard results.

---

## 14. Cursor tokens (signed, bound, versioned)

### 14.1 Format

base64url(payload_json) + “.” + base64url(hmac(signature))

### 14.2 Cursor payload

Includes:

- cursor_version
- cursor_secret_version
- traversal_contract_version
- workspace_id
- artifact_id
- tool (artifact.get|artifact.select|artifact.find)
- binding:
    - get: target + normalized_jsonpath
    - select: root_path + select_paths_hash + where_hash
- where_canonicalization_mode
- mapper_version
- artifact_generation
- map_kind
- map_budget_fingerprint (required for partial)
- sample_set_hash (required for partial)
- position_state
- issued_at, expires_at

select_paths_hash and where_hash MUST be computed exactly as in 12.3.1.

Verification must also enforce:

- cursor.where_canonicalization_mode == server.where_canonicalization_mode
    
    If not, reject with CURSOR_STALE.
    

### 14.3 sample_set_hash (partial mode)

sample_set_hash = sha256(canonical_json({

“root_path”: root_path,

“sample_indices”: artifact_roots.sample_indices,

“map_budget_fingerprint”: artifacts.map_budget_fingerprint,

“mapper_version”: artifacts.mapper_version

}))[:32]

Verification:

- signature valid
- artifact_generation matches
- recompute sample_set_hash from DB and compare
- mismatch => CURSOR_STALE

### 14.4 Position encoding

Full mapping: deterministic traversal continuation per traversal contract.

Partial mapping: { root_path, sample_pos }.

### 14.5 Secret rotation

Active secrets list:

- newest signs
- any active verifies

Keep old secrets longer than cursor_ttl_minutes.

---

## 15. Pruning, retention, and filesystem cleanup

### 15.1 Retention inputs

- expires_at
- last_referenced_at
- payload_total_bytes and binary usage for quotas

### 15.2 Touch policy

- creation: touch last_referenced_at
- retrieval/describe: touch if not deleted
- search: do not touch

### 15.3 Soft delete job

SKIP LOCKED selection; UPDATE with predicates rechecked; set deleted_at and generation++.

### 15.4 Hard delete job

Delete eligible artifacts; cascades remove artifact_roots and artifact_refs. Then:

- delete unreferenced payload_blobs
- payload_binary_refs cascade
- delete binary_blobs unreferenced by payload_binary_refs
- delete filesystem blobs for removed binary_blobs
- optional reconciliation for orphan files

---

## 16. Limits and DoS hardening

### 16.1 Early caps

- cap inbound request size
- cap captured upstream error bytes
- enforce max_json_part_parse_bytes: oversized JSON becomes byte-backed binary_ref

### 16.2 Storage caps

- max_binary_blob_bytes
- max_payload_total_bytes
- max_total_storage_bytes triggers aggressive prune

### 16.3 Compute caps

- retrieval: max_items, max_bytes_out, max_wildcards, max_compute_steps
- partial mapping: max_bytes_read_partial_map, max_compute_steps_partial_map, max_records_sampled_partial, max_record_bytes_partial, max_leaf_paths_partial

---

## 17. Filesystem layout

```
DATA_DIR/
  state/
    config.json
    secrets.json (optional)
  resources/         (optional internal copies)
  blobs/
    bin/
      ab/cd/<binary_hash>
  logs/
  tmp/
```

---

## 18. Libraries and structure

- Python 3.11+
- fastmcp
- psycopg3
- structlog/logging
- RFC 8785 canonicalizer module
- Decimal-safe JSON parsing
- ijson (streaming)
- zstandard or gzip
- deterministic PRNG implementation pinned by prng_version

---

## 19. Test suite requirements

- RFC 8785 vectors and numeric edge cases
- canonical bytes compression round-trip and hash integrity
- reserved arg stripping: only _gateway_* keys removed
- oversized JSON becomes binary_ref and maps via streaming
- partial mapping determinism given same map_budget_fingerprint
- prefix coverage semantics when stop_reason != none
- sampling bias invariant: oversize records skipped and counted
- cursor determinism and CURSOR_STALE on sample_set mismatch
- cursor staleness when where_canonicalization_mode differs from server mode
- session discovery correctness
- prune/touch correctness and binary cleanup via payload_binary_refs

---

## 20. Out of scope

- multi-tenant
- JSONPath filters
- regex in where DSL
- full-dataset querying of oversized JSON beyond sampled-only partial mode
- automatic pagination stitching

## Addendum A: Gateway response and handle contracts

### A.1 Mirrored tool call return schema (normative)

All mirrored tool calls `{prefix}.{tool}` MUST return **exactly one** of:

1. **Handle-only response** (default for non-trivial payloads), or
2. **Handle + inline envelope** (only when allowed by policy and size thresholds).

### A.1.1 Handle-only response shape

```json
{
  "type": "gateway_tool_result",
  "artifact": {
    "workspace_id": "local",
    "artifact_id": "art_...",
    "created_seq": 12345,
    "created_at": "2026-02-07T00:00:00Z",
    "session_id": "opaque-or-uuid",
    "source_tool": "github.search_issues",
    "upstream_instance_id": "abcd1234...",
    "payload_hash_full": "sha256hex...",
    "canonicalizer_version": "jcs_rfc8785_v1",
    "status": "ok|error",
    "payload_json_bytes": 123,
    "payload_binary_bytes_total": 0,
    "payload_total_bytes": 123,
    "contains_binary_refs": false,
    "map_kind": "none|full|partial",
    "map_status": "pending|ready|failed|stale",
    "index_status": "off|pending|ready|partial|failed"
  },
  "inline": null,
  "warnings": [],
  "meta": {
    "cache": {
      "reused": false,
      "reuse_reason": "none|request_key|dedupe_alias",
      "reused_artifact_id": null
    }
  }
}
```

Rules:

- `artifact.status` MUST match the stored envelope status for the created/reused artifact.
- `warnings` MUST include any gateway warnings (e.g., oversized JSON offloaded) that were inserted into the envelope’s `meta.warnings`.
- `meta.cache.reused=true` MUST be set when reuse occurred; `reused_artifact_id` MUST identify the reused artifact.

### A.1.2 Handle + inline envelope response shape

```json
{
  "type": "gateway_tool_result",
  "artifact": { "...same as A.1.1..." },
  "inline": {
    "envelope": { "...full envelope object..." }
  },
  "warnings": [],
  "meta": { "cache": { "...same as A.1.1..." } }
}
```

Inline rules:

- Inline is permitted only if **all** are true:
    1. `payload_json_bytes <= inline_envelope_max_json_bytes`
    2. `contains_binary_refs == false`
    3. `envelope_jsonb_mode != none` OR the server is willing to reconstruct from canonical bytes within request compute budgets
    4. tool-level config permits inline (default allow)
- If inline is not permitted, `inline` MUST be null.

### A.2 artifact_id format (normative)

`artifact_id` MUST be generated as:

- `artifact_id = "art_" + base32_ulid()` or `"art_" + uuid_v4_hex()`

Constraints:

- MUST be globally unique within the workspace.
- MUST be stable for the lifetime of the artifact row.
- MUST NOT embed secret material.

### A.3 “Small envelope” thresholds (normative)

The gateway MUST define these config values:

- `inline_envelope_max_json_bytes` (default 32_768)
- `inline_envelope_max_total_bytes` (default 65_536)

Inline eligibility MUST use `payload_json_bytes` and `payload_total_bytes` computed at ingest time.

### A.4 Retrieval tool common envelope for errors (normative)

All gateway tools (mirrored + retrieval) MUST return errors in a uniform shape:

```json
{
  "type": "gateway_error",
  "code": "INVALID_ARGUMENT|NOT_FOUND|GONE|INTERNAL|CURSOR_INVALID|CURSOR_EXPIRED|CURSOR_STALE|BUDGET_EXCEEDED|UNSUPPORTED",
  "message": "human readable",
  "details": {}
}
```

Rules:

- `GONE` MUST be returned when `deleted_at` is not null.
- `NOT_FOUND` MUST be returned when `(workspace_id, artifact_id)` does not exist.
- `INTERNAL` MUST be returned when Postgres or required filesystem paths are unavailable, and no artifact claim is made.
- `BUDGET_EXCEEDED` MUST be returned when limits prevent any meaningful output and no cursor continuation is possible.

---

## Addendum B: artifact.search filters and pagination

### B.1 artifact.search request schema (normative)

```json
{
  "filters": {
    "include_deleted": false,
    "status": "ok|error|null",
    "source_tool_prefix": "github|null",
    "source_tool": "github.search_issues|null",
    "upstream_instance_id": "abcd...|null",
    "request_key": "sha256hex|null",
    "payload_hash_full": "sha256hex|null",
    "parent_artifact_id": "art_...|null",
    "has_binary_refs": true,
    "created_seq_max": 999999999,
    "created_seq_min": 0,
    "created_at_after": "2026-02-01T00:00:00Z",
    "created_at_before": "2026-02-07T00:00:00Z"
  },
  "order_by": "created_seq_desc|last_seen_desc",
  "limit": 50,
  "cursor": null
}
```

Rules:

- `limit` MUST be capped by server config `artifact_search_max_limit` (default 200).
- Filters are ANDed.
- `status` refers to the envelope `status` stored in `payload_blobs.envelope` (or reconstructed from canonical bytes if minimized); the gateway MAY cache this in `artifacts.error_summary` for faster filtering.

### B.2 artifact.search response schema (normative)

```json
{
  "items": [
    {
      "artifact_id": "art_...",
      "created_seq": 123,
      "created_at": "2026-02-07T00:00:00Z",
      "last_seen_at": "2026-02-07T00:00:00Z",
      "source_tool": "github.search_issues",
      "upstream_instance_id": "abcd...",
      "status": "ok|error",
      "payload_total_bytes": 1234,
      "contains_binary_refs": false,
      "map_kind": "none|full|partial",
      "map_status": "pending|ready|failed|stale"
    }
  ],
  "truncated": true,
  "cursor": "..."
}
```

Pagination:

- The cursor for search MUST bind to:
    - `session_id`, `order_by`, and the last item’s `(last_seen_at, created_seq)` for last_seen ordering, or `(created_seq)` for created_seq ordering.
- Search cursor TTL and signing MUST reuse the same cursor mechanism described in section 14, with `tool = "artifact.search"` and binding fields.

Touch semantics:

- As in v1.9: search MUST update `sessions.last_seen_at` and `artifact_refs.last_seen_at`, and MUST NOT update `artifacts.last_referenced_at`.

---

## Addendum C: Sample persistence for partial mapping

### C.1 New table: artifact_samples (normative)

Purpose: persist sampled record payloads for partial mapping so that `artifact.select` can evaluate `where` and return projections.

DDL (conceptual):

- workspace_id text not null default 'local'
- artifact_id text not null
- root_key text not null
- root_path text not null
- sample_index int not null
- record jsonb not null
- record_bytes int not null CHECK (record_bytes >= 0)
- record_hash text not null
- created_at timestamptz not null default now()

Primary key:

- (workspace_id, artifact_id, root_key, sample_index)

Foreign keys:

- (workspace_id, artifact_id, root_key) references artifact_roots(workspace_id, artifact_id, root_key) ON DELETE CASCADE

Indexes:

- (workspace_id, artifact_id, root_key)
- (workspace_id, artifact_id, root_path)

Rules:

- `record_hash = sha256( RFC8785(record) )` where record canonicalization uses the same canonicalizer.
- The mapper MUST insert a row for each successfully materialized sampled record index.
- For a sampled index selected by reservoir that fails materialization due to caps, the mapper MUST NOT insert a row.

### C.2 artifact_roots.sample_indices relationship (normative)

- `artifact_roots.sample_indices` MUST contain exactly the set of `sample_index` values present in `artifact_samples` for that `(artifact_id, root_key)`, sorted ascending.
- If a mapper run produces a different sample set, it MUST:
    - replace the `artifact_samples` rows for that `(artifact_id, root_key)` atomically (transaction), and
    - update `artifact_roots.sample_indices` to match.

### C.3 Partial retrieval dependency (normative)

When `map_kind=partial`:

- `artifact.select` MUST source sampled records from `artifact_samples`, not from `artifact_roots.examples`.
- If `artifact_roots.sample_indices` is non-empty but `artifact_samples` rows are missing, `artifact.select` MUST return `INTERNAL` with details indicating mapping storage corruption.

---

## Addendum D: Cursor signing canonicalization and secret storage

### D.1 Cursor canonicalization and signature input (normative)

For any cursor:

1. Build a cursor payload object with the fields specified in section 14.2.
2. Serialize it to bytes using RFC 8785 canonical JSON:
    - `payload_bytes = RFC8785(cursor_payload_obj)` encoded as UTF-8
3. Compute signature:
    - `sig_bytes = HMAC-SHA256(secret_bytes, payload_bytes)`
4. Encode:
    - `cursor = base64url(payload_bytes) + "." + base64url(sig_bytes)`

Rules:

- The HMAC input MUST be **exactly** `payload_bytes`.
- base64url MUST be unpadded.

### D.2 Cursor verification (normative)

On verification:

- decode and parse payload_bytes as JSON
- verify `expires_at >= now()`, else `CURSOR_EXPIRED`
- select the secret by `cursor_secret_version`, else `CURSOR_INVALID`
- recompute HMAC over payload_bytes, constant-time compare, else `CURSOR_INVALID`
- verify all bindings, else `CURSOR_STALE` or `INVALID_ARGUMENT` as appropriate

Mismatch rules:

- `where_canonicalization_mode` mismatch between cursor payload and server mode MUST return `CURSOR_STALE`.
- `traversal_contract_version` mismatch MUST return `CURSOR_STALE`.
- `artifact_generation` mismatch MUST return `CURSOR_STALE`.

### D.3 Secret storage format (normative)

Secrets MUST be stored locally under:

- `DATA_DIR/state/secrets.json`

Format:

```json
{
  "cursor_ttl_minutes": 60,
  "active_secrets": [
    { "version": "v3", "hmac_sha256_key_b64": "..." },
    { "version": "v2", "hmac_sha256_key_b64": "..." }
  ],
  "signing_secret_version": "v3"
}
```

Rules:

- `signing_secret_version` MUST exist in `active_secrets`.
- Keys MUST be at least 32 random bytes.
- The gateway MUST accept verification with any `active_secrets` entry.
- Removing a secret before TTL expiry will invalidate existing cursors and is allowed; resulting failures MUST be `CURSOR_INVALID`.

---

## Addendum E: where DSL definition (minimal but complete)

### E.1 where DSL grammar (normative)

The where clause is a UTF-8 string. The gateway supports two evaluation modes; hashing mode is in v1.9 (raw_string or canonical_ast). This addendum defines the actual language.

Grammar (EBNF-style):

- expr := or_expr
- or_expr := and_expr (("OR" | "or") and_expr)*
- and_expr := not_expr (("AND" | "and") not_expr)*
- not_expr := ("NOT" | "not") not_expr | primary
- primary := comparison | "(" expr ")"
- comparison := path op literal
- op := "=" | "!=" | "<" | "<=" | ">" | ">="
- path := jsonpath_relative ; JSONPath subset, relative, MUST NOT start with "$"
- literal := number | string | "true" | "false" | "null"
- number := integer | decimal
- string := single-quoted with escapes ', \, \n, \r, \t

### E.2 Path evaluation semantics (normative)

- Paths are evaluated relative to the record root.
- If a path is missing:
    - any comparison evaluates to false, except `!= null` which evaluates to true only if the path exists and value is not null.
- If the path selects multiple values (only possible via `[*]`):
    - comparison is true if **any** selected value satisfies it (existential semantics).
    - wildcard expansion is bounded by `max_wildcard_expansion_total`.

### E.3 Type semantics (normative)

- Numeric comparisons require both operands numeric:
    - JSON numbers are handled as Decimal/int as stored.
    - string literals are never coerced to numbers.
- String comparisons are lexicographic by Unicode codepoint order.
- Boolean only supports `=` and `!=`.

### E.4 Compute accounting (normative)

Each evaluated comparison increments `compute_steps` by:

- `+1` per path segment
- `+W` for wildcard expansions where W is number of expanded members
- `+1` for the operator comparison
    
    Short-circuiting is allowed and MUST be deterministic.
    

---

## Addendum F: Projection output shape for artifact.select

### F.1 Output item schema (normative)

Each returned item MUST include a stable record locator plus the projection.

For roots that are arrays:

```json
{
  "_locator": { "type": "array_index", "index": 123 },
  "projection": {
    "a.b": 1,
    "c": "x"
  }
}
```

For roots that are objects enumerated by keys (full mapping only):

```json
{
  "_locator": { "type": "object_key", "key": "someKey" },
  "projection": { ... }
}
```

Rules:

- `projection` keys MUST be the canonicalized relative JSONPath strings from `select_paths` (post-normalization).
- The gateway MUST emit projection keys in lexicographic ascending order.
- If a selected path is missing in a record, the corresponding projection key MUST be present with value `null` only if `select_missing_as_null=true` (config, default false). If false, missing paths are omitted from that item’s projection.

### F.2 Partial mode source and order (normative)

When `map_kind=partial`:

- enumerate sampled records in ascending `sample_index` order
- `_locator.index` MUST equal `sample_index`
- response MUST include:
    - `sampled_only=true`
    - `sample_indices_used=[...]`
    - `sampled_prefix_len` from `artifact_roots.root_shape`

## Implementation checklist:

Below is a completion-grade implementation checklist for **MCP Artifact Gateway (Python) Full Implementation Spec v1.9 Local Single-Tenant**. It’s written as “everything that must exist in code” plus “everything that must be demonstrably true at runtime”, mapped directly to the v1.9 spec you pasted. 

## 1) Repo shape and bootability

- [ ]  A single runnable entrypoint exists (for example `python -m mcp_gateway` or `mcp-gateway serve`) that:
    - [ ]  Loads config.
    - [ ]  Validates filesystem paths.
    - [ ]  Connects Postgres and validates migrations.
    - [ ]  Connects to every configured upstream (stdio and http).
    - [ ]  Discovers upstream tool lists.
    - [ ]  Starts an MCP server exposing:
        - [ ]  mirrored tools `{prefix}.{tool}`
        - [ ]  retrieval tools (`gateway.status`, `artifact.search`, `artifact.get`, `artifact.select`, `artifact.describe`, `artifact.find`, `artifact.chain_pages`)
- [ ]  A “fail fast” startup mode exists:
    - [ ]  If DB is unreachable or migrations missing: server does not start (or starts with gateway unhealthy and refuses mirrored calls with `INTERNAL`).
    - [ ]  If DATA_DIR or required subdirs cannot be created/written: server does not start (or starts unhealthy and refuses mirrored calls with `INTERNAL`).
- [ ]  A clear module boundary exists (names are illustrative, not mandatory):
    - [ ]  `config/` (schema + loader + defaults)
    - [ ]  `db/` (psycopg3 pool, migrations, queries)
    - [ ]  `fs/` (DATA_DIR layout, atomic writes, blob paths, resource copies)
    - [ ]  `canonical/` (RFC 8785 canonicalizer + hashing + compression)
    - [ ]  `upstream/` (clients for stdio/http MCP, discovery, schema parsing)
    - [ ]  `gateway/` (request handling, reserved arg stripping, reuse logic, artifact creation)
    - [ ]  `retrieval/` (jsonpath evaluation, select/projection, cursor handling)
    - [ ]  `mapping/` (full mapping, partial mapping, worker, sampling)
    - [ ]  `prune/` (soft delete, hard delete, blob cleanup, reconciliation)
    - [ ]  `tests/` (unit + integration)

---

## 2) Configuration and constants

- [ ]  A config model exists that includes (at minimum):
    - [ ]  `DATA_DIR` and derived directories (`tmp/`, `logs/`, `state/`, `resources/`, `blobs/bin/...`)
    - [ ]  Postgres DSN + pool sizing + statement timeouts
    - [ ]  Upstream definitions:
        - [ ]  `prefix`, `transport` (http/stdio), endpoint config, semantic salts
        - [ ]  optional tool-level dedupe exclusions (JSONPath subset)
        - [ ]  tool-level reuse gating (strict schema hash match default)
        - [ ]  tool-level inline eligibility (default allow)
    - [ ]  Envelope storage config:
        - [ ]  `envelope_jsonb_mode` = `full|minimal_for_large|none`
        - [ ]  `envelope_jsonb_minimize_threshold_bytes`
        - [ ]  canonical byte compression: `zstd|gzip|none`
    - [ ]  Hard limits / budgets:
        - [ ]  inbound request cap
        - [ ]  upstream error capture cap
        - [ ]  `max_json_part_parse_bytes` (oversized JSON becomes byte-backed)
        - [ ]  `max_full_map_bytes`
        - [ ]  partial map budgets: `max_bytes_read_partial_map`, `max_compute_steps_partial_map`, `max_depth_partial_map`, `max_records_sampled_partial`, `max_record_bytes_partial`, `max_leaf_paths_partial`, `max_root_discovery_depth`
        - [ ]  retrieval budgets: `max_items`, `max_bytes_out`, `max_wildcards`, `max_compute_steps`
        - [ ]  `artifact_search_max_limit`
        - [ ]  storage caps: `max_binary_blob_bytes`, `max_payload_total_bytes`, `max_total_storage_bytes`
    - [ ]  Cursor settings:
        - [ ]  cursor TTL minutes
        - [ ]  active secret versions + signing secret version
        - [ ]  cursor_version constant
    - [ ]  Version constants:
        - [ ]  `canonicalizer_version`
        - [ ]  `mapper_version`
        - [ ]  `traversal_contract_version`
        - [ ]  `where_canonicalization_mode` (`raw_string` default, `canonical_ast` optional)
        - [ ]  `prng_version` constant
- [ ]  The system hardcodes and enforces `workspace_id = "local"` everywhere (no multi-tenant behavior leaks in).

---

## 3) Database migrations and schema correctness

- [ ]  Migrations exist to create **exactly** the v1.9 tables and constraints:
    - [ ]  `sessions`
    - [ ]  `binary_blobs`
    - [ ]  `payload_blobs`
    - [ ]  `payload_hash_aliases`
    - [ ]  `payload_binary_refs`
    - [ ]  `artifacts`
    - [ ]  `artifact_refs`
    - [ ]  `artifact_roots`
    - [ ]  Addendum C table: `artifact_samples`
- [ ]  Every PK, FK, unique constraint, and index in the spec exists.
- [ ]  All relevant CHECK constraints exist:
    - [ ]  enum-like fields (`map_kind`, `map_status`, `index_status`, encoding values, non-negative sizes)
- [ ]  Migrations are idempotent and ordered; a fresh database can be brought to current schema in one command.
- [ ]  Advisory lock usage for request stampede exists (two 32-bit keys derived from `sha256(request_key)`), with timeout and metrics/logging.

---

## 4) Filesystem layout and durability

- [ ]  On startup, gateway ensures these directories exist under `DATA_DIR`:
    - [ ]  `state/`
    - [ ]  `resources/` (if internal copies enabled)
    - [ ]  `blobs/bin/`
    - [ ]  `tmp/`
    - [ ]  `logs/` (if used)
- [ ]  Binary storage is content-addressed:
    - [ ]  `binary_hash = sha256(raw_bytes).hexdigest()`
    - [ ]  `blob_id = "bin_" + binary_hash[:32]`
    - [ ]  Path = `BIN_DIR / h[0:2] / h[2:4] / binary_hash`
- [ ]  Atomic write procedure exists and is used:
    - [ ]  temp file in same directory
    - [ ]  fsync temp file
    - [ ]  atomic rename to final path
- [ ]  Existing blob handling exists:
    - [ ]  verifies size matches expected `byte_count`
    - [ ]  optional probe hashes supported and persisted (`probe_head_hash`, `probe_tail_hash`, `probe_bytes`)
- [ ]  Resource refs support two durabilities:
    - [ ]  `internal`: copy bytes into `DATA_DIR/resources/...` and require `content_hash`
    - [ ]  `external_ref`: do not copy; `content_hash` optional best effort

---

## 5) Canonicalization, hashing, compression, numeric safety

- [ ]  RFC 8785 canonical JSON implementation exists and is used for:
    - [ ]  forwarded args canonicalization
    - [ ]  upstream tool schema canonicalization
    - [ ]  envelope canonicalization
    - [ ]  cursor payload canonicalization
    - [ ]  record hashing in `artifact_samples`
- [ ]  Numeric parsing rules are enforced:
    - [ ]  floats parsed as Decimal (no Python float drift)
    - [ ]  NaN/Infinity rejected
    - [ ]  canonicalization never sees Python floats
- [ ]  Payload identity is correct:
    - [ ]  `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`
    - [ ]  `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling)
- [ ]  Canonical bytes storage works:
    - [ ]  `envelope_canonical_encoding` stored (`zstd|gzip|none`)
    - [ ]  `envelope_canonical_bytes` stored (compressed)
    - [ ]  `envelope_canonical_bytes_len` stored (uncompressed length)
- [ ]  Dedupe hash is implemented and explicitly does **not** define storage identity:
    - [ ]  tool-configured JSONPath exclusions apply only to dedupe computation
    - [ ]  alias table `payload_hash_aliases` is populated and used only for reuse lookup

## 5) Canonicalization, hashing, compression, numeric safety

- [ ]  RFC 8785 canonical JSON implementation exists and is used for:
    - [ ]  forwarded args canonicalization
    - [ ]  upstream tool schema canonicalization
    - [ ]  envelope canonicalization
    - [ ]  cursor payload canonicalization
    - [ ]  record hashing in `artifact_samples`
- [ ]  Numeric parsing rules are enforced:
    - [ ]  floats parsed as Decimal (no Python float drift)
    - [ ]  NaN/Infinity rejected
    - [ ]  canonicalization never sees Python floats
- [ ]  Payload identity is correct:
    - [ ]  `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`
    - [ ]  `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling)
- [ ]  Canonical bytes storage works:
    - [ ]  `envelope_canonical_encoding` stored (`zstd|gzip|none`)
    - [ ]  `envelope_canonical_bytes` stored (compressed)
    - [ ]  `envelope_canonical_bytes_len` stored (uncompressed length)
- [ ]  Dedupe hash is implemented and explicitly does **not** define storage identity:
    - [ ]  tool-configured JSONPath exclusions apply only to dedupe computation
    - [ ]  alias table `payload_hash_aliases` is populated and used only for reuse lookup

---

## 6) Upstream discovery, mirroring, and reserved arg stripping

- [ ]  Upstream tool discovery at startup:
    - [ ]  fetch tool list from each upstream
    - [ ]  expose mirrored tools as `{prefix}.{tool}` with identical schema/docs (no injected fields)
- [ ]  Reserved gateway args stripping is exact and tested:
    - [ ]  remove keys equal to `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq`
    - [ ]  remove any key whose name begins with exact prefix `_gateway_`
    - [ ]  remove nothing else (example: `gateway_url` must not be stripped)
- [ ]  Upstream instance identity exists and excludes secrets:
    - [ ]  `upstream_instance_id = sha256(canonical_semantic_identity_bytes)[:32]`
    - [ ]  includes transport + stable endpoint identity + prefix/name + optional semantic salt
    - [ ]  excludes rotating auth headers, tokens, secret env values, private key paths
    - [ ]  optional `upstream_auth_fingerprint` stored for debugging but excluded from request identity
- [ ]  `request_key` is computed exactly:
    - [ ]  based on `upstream_instance_id`, prefix, tool_name, canonical args bytes
    - [ ]  canonical args bytes computed **after** stripping reserved keys and validating against upstream schema
    - [ ]  `request_args_hash` and `request_args_prefix` persisted with caps

---

## 7) Artifact creation pipeline (mirrored tool calls)

For every mirrored tool call `{prefix}.{tool}(args)`:

- [ ]  Validate `_gateway_context.session_id` exists, else `INVALID_ARGUMENT`.
- [ ]  Determine `cache_mode` default `allow`.
- [ ]  Strip reserved gateway args (exact rules).
- [ ]  Validate forwarded args against upstream schema.
- [ ]  Canonicalize forwarded args and compute `request_key`.
- [ ]  Acquire advisory lock for stampede control (with timeout behavior).
- [ ]  Reuse behavior when `cache_mode != fresh`:
    - [ ]  request_key latest candidate chosen by `created_seq desc`
    - [ ]  optional dedupe alias reuse constrained to same `(upstream_instance_id, tool)`
    - [ ]  reuse requires:
        - [ ]  not deleted, not expired
        - [ ]  schema hash match if strict reuse enabled
    - [ ]  response indicates `meta.cache.reused=true` + reason + reused artifact id
- [ ]  Call upstream tool, capture success or failure.
- [ ]  Normalize into envelope (always):
    - [ ]  status ok or error, with error shape present on error
    - [ ]  content parts support `json`, `text`, `resource_ref`, `binary_ref` (and alias `image_ref`)
    - [ ]  binary bytes never stored inline, only refs
- [ ]  Oversized JSON handling at ingest:
    - [ ]  if any JSON part size > `max_json_part_parse_bytes`:
        - [ ]  do not parse into structured value
        - [ ]  store raw bytes as `binary_ref` with JSON mime (and encoding)
        - [ ]  replace JSON content entry with `binary_ref` descriptor
        - [ ]  add warning in `meta.warnings` with original part index + encoding
- [ ]  Produce canonical envelope bytes, compute payload hashes, compress canonical bytes.
- [ ]  Insert or upsert `payload_blobs`.
- [ ]  Insert `binary_blobs` and `payload_binary_refs` for every blob reference.
- [ ]  Insert optional `payload_hash_aliases` rows (dedupe).
- [ ]  Insert `artifacts` row with:
    - [ ]  monotonic `created_seq`
    - [ ]  mapping fields: `map_kind='none'`, `map_status='pending'` initially, `mapper_version` set
    - [ ]  `index_status='off'` unless enabled
    - [ ]  sizes persisted (`payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`)
    - [ ]  `last_referenced_at=now()`
- [ ]  Update session tracking:
    - [ ]  `sessions` upsert with `last_seen_at=now()`
    - [ ]  `artifact_refs` upsert for `(session_id, artifact_id)`
- [ ]  Return contract (Addendum A):
    - [ ]  returns a handle-only result by default
    - [ ]  returns handle+inline envelope only when thresholds satisfied and policy allows
    - [ ]  if gateway itself unhealthy (DB/fs), return `INTERNAL` and do not claim artifact creation

---

## 8) Gateway tool response contracts

- [ ]  All tool responses follow one of:
    - [ ]  `gateway_tool_result` for success returns (mirrored tools)
    - [ ]  uniform `gateway_error` for failures (all tools)
- [ ]  Handle includes required metadata:
    - [ ]  ids, created_seq, session_id, tool ids, hash ids, byte sizes, mapping/index status, contains_binary_refs, status
- [ ]  Warnings propagation works:
    - [ ]  warnings in response include gateway warnings inserted into envelope meta
- [ ]  Error codes are implemented and used correctly:
    - [ ]  `INVALID_ARGUMENT`, `NOT_FOUND`, `GONE`, `INTERNAL`, `CURSOR_INVALID`, `CURSOR_EXPIRED`, `CURSOR_STALE`, `BUDGET_EXCEEDED`, `UNSUPPORTED`

---

## 9) Retrieval tools and deterministic bounds

### 9.1 gateway.status

- [ ]  `gateway.status()` returns:
    - [ ]  upstream connectivity snapshot
    - [ ]  DB ok / migrations ok
    - [ ]  filesystem paths ok
    - [ ]  version constants: canonicalizer, mapper, traversal contract, cursor version
    - [ ]  where canonicalization mode
    - [ ]  partial mapping backend id + prng version
    - [ ]  all configured limits/budgets
    - [ ]  cursor TTL and active secret versions

### 9.2 artifact.search

- [ ]  Requires `_gateway_context.session_id`.
- [ ]  Reads only from `artifact_refs` for that session (discovery uses refs exclusively).
- [ ]  Filters implemented (Addendum B), including:
    - [ ]  include_deleted, status, source_tool_prefix, source_tool, upstream_instance_id, request_key, payload_hash_full, parent_artifact_id, has_binary_refs, created_seq range, created_at range
- [ ]  Ordering implemented:
    - [ ]  `created_seq_desc` default
    - [ ]  `last_seen_desc` optional
- [ ]  Search touches only:
    - [ ]  `sessions.last_seen_at`
    - [ ]  `artifact_refs.last_seen_at`
    - [ ]  does NOT touch `artifacts.last_referenced_at`
- [ ]  Pagination cursor for search exists and is bound to session_id + order_by + last position.

### 9.3 JSONPath subset support

- [ ]  JSONPath subset grammar implemented exactly:
    - [ ]  `$`, `.name`, `['...']` with escapes, `[n]`, `[*]`
    - [ ]  no filters
- [ ]  Caps enforced:
    - [ ]  max path length
    - [ ]  max segments
    - [ ]  max wildcard expansion total
- [ ]  Deterministic traversal contract implemented:
    - [ ]  arrays ascend
    - [ ]  objects key lex ascend
    - [ ]  wildcard expansions obey above
    - [ ]  partial sample enumeration uses ascending sample indices

### 9.4 artifact.get

- [ ]  Requires session_id.
- [ ]  Supports:
    - [ ]  `target=envelope` (jsonpath evaluated on envelope root)
    - [ ]  `target=mapped` (requires map_status ready and map_kind full/partial)
- [ ]  If envelope jsonb is minimized or none:
    - [ ]  reconstruct by parsing canonical bytes, within compute budgets
- [ ]  Bounded deterministic output:
    - [ ]  max_bytes_out / max_items / max_compute enforced
    - [ ]  deterministic truncation emits `truncated=true` + cursor + omitted metadata
- [ ]  Touch semantics:
    - [ ]  if not deleted: touches `artifacts.last_referenced_at`
    - [ ]  always updates `artifact_refs.last_seen_at` and `sessions.last_seen_at`
    - [ ]  if deleted: returns `GONE`

### 9.5 artifact.describe

- [ ]  Returns:
    - [ ]  mapping status/kind + mapper_version
    - [ ]  roots inventory + fields_top
    - [ ]  partial mapping fields: sampled-only, prefix coverage indicator, stop_reason, sampled_prefix_len, sampled_record_count, skipped_oversize_records
    - [ ]  count_estimate only when known under the stated rules
- [ ]  Touch semantics same as retrieval.

### 9.6 artifact.select

- [ ]  Inputs:
    - [ ]  artifact_id, root_path, select_paths (set semantics), where optional, limits, cursor
- [ ]  select_paths canonicalization implemented:
    - [ ]  whitespace removal, canonical escaping/quotes
    - [ ]  relative paths must not start with `$`
    - [ ]  sorted lexicographically, duplicates removed
    - [ ]  `select_paths_hash = sha256(canonical_json(array))`
- [ ]  where hashing implemented per server mode:
    - [ ]  raw_string default: exact UTF-8 bytes
    - [ ]  canonical_ast optional: parse + canonicalize
    - [ ]  server reports mode in `gateway.status`, cursor binds to it
- [ ]  Full mapping behavior:
    - [ ]  bounded scan in deterministic order with cursor continuation
- [ ]  Partial mapping behavior:
    - [ ]  sampled-only scan:
        - [ ]  enumerate sample indices ascending
        - [ ]  evaluate where and select_paths only on sampled records
        - [ ]  returns `sampled_only=true`, `sample_indices_used`, `sampled_prefix_len`
- [ ]  Output projection contract (Addendum F):
    - [ ]  each item has `_locator` and `projection`
    - [ ]  projection keys are canonicalized select paths, emitted in lex order
    - [ ]  missing path behavior respects config `select_missing_as_null`

### 9.7 artifact.find

- [ ]  Works in sample-only mode unless indexing is enabled.
- [ ]  Deterministic output and bounded truncation with cursor.

### 9.8 artifact.chain_pages

- [ ]  Chain ordering is correct:
    - [ ]  `chain_seq asc`, then `created_seq asc`
- [ ]  Chain seq allocation exists when not provided, with retry and uniqueness constraint.

---

## 10) Mapping implementation (full + partial) and worker safety

### 10.1 Mapping scheduler

- [ ]  mapping_mode implemented: `async|hybrid|sync` (default hybrid)
- [ ]  Artifacts created with map_status pending cause mapping work to be scheduled.

### 10.2 JSON part selection scoring

- [ ]  Deterministic scoring implemented; tie-break by part index ascending.
- [ ]  Stores `mapped_part_index` on artifact.

### 10.3 Full mapping

When selected JSON part is <= max_full_map_bytes:

- [ ]  parse fully
- [ ]  discover up to K roots (K=3)
- [ ]  build deterministic inventory:
    - [ ]  roots entries written to `artifact_roots`
    - [ ]  `map_kind=full`, `map_status=ready`

### 10.4 Partial mapping trigger

- [ ]  If JSON part too large OR stored as `binary_ref application/json(+encoding)`:
    - [ ]  partial mapping runs
    - [ ]  `map_kind=partial`

### 10.5 Partial mapping core requirements

- [ ]  Byte-backed streaming input supported:
    - [ ]  from JSON binary blob (required when oversized at ingest)
    - [ ]  from text JSON (bounded)
    - [ ]  from re-canonicalized bytes for small structured values (bounded)
- [ ]  Budgets enforced during streaming:
    - [ ]  max bytes read
    - [ ]  max compute steps (stream events)
    - [ ]  max depth
    - [ ]  max sampled records N
    - [ ]  max per-record bytes
    - [ ]  max leaf paths
    - [ ]  root discovery depth cap
- [ ]  stop_reason tracked:
    - [ ]  none | max_bytes | max_compute | max_depth | parse_error
- [ ]  Prefix coverage semantics enforced:
    - [ ]  if stop_reason != none:
        - [ ]  count_estimate is null
        - [ ]  root_shape.prefix_coverage=true
        - [ ]  inventory coverage computed vs prefix
- [ ]  map_backend_id and prng_version:
    - [ ]  map_backend_id computed exactly from python version + ijson backend name + version
    - [ ]  prng_version is a code constant
    - [ ]  both returned by status and stored on artifacts
- [ ]  map_budget_fingerprint computed and stored:
    - [ ]  includes mapper version, traversal contract, backend id, prng version, all budgets
    - [ ]  if changes, previous partial mapping marked stale and cursors become stale
- [ ]  root_path normalization:
    - [ ]  absolute path starting with `$`
    - [ ]  uses `.name` when identifier is valid; otherwise bracket form with canonical escaping
    - [ ]  no wildcards
    - [ ]  format change requires traversal_contract_version bump (enforced as policy)
- [ ]  streaming skip contract implemented:
    - [ ]  ability to skip unselected subtrees without building full trees
    - [ ]  compute steps count all events processed, including skipped

### 10.6 Deterministic reservoir sampling

- [ ]  Reservoir sampling is one-pass and prefix-bounded:
    - [ ]  seed = sha256(payload_hash_full + "|" + root_path + "|" + map_budget_fingerprint)
    - [ ]  PRNG deterministic and versioned
    - [ ]  selected indices maintained uniformly over processed prefix indices
- [ ]  Bias invariant is explicit and implemented:
    - [ ]  oversize/depth-violating records are skipped and counted
    - [ ]  sample_indices include only successfully materialized records
- [ ]  sampled_prefix_len is computed correctly:
    - [ ]  counts element boundaries successfully recognized, including skipped/non-materialized
    - [ ]  parse_error mid-element uses last fully recognized index + 1
- [ ]  count_estimate rules enforced:
    - [ ]  set only if stop_reason==none AND array close observed

### 10.7 Persisted samples (Addendum C)

- [ ]  `artifact_samples` table is used for partial samples:
    - [ ]  one row per materialized sampled record index
    - [ ]  record hash stored as sha256(RFC8785(record))
- [ ]  `artifact_roots.sample_indices` exactly matches sample indices present in `artifact_samples` (sorted).
- [ ]  Updates are atomic:
    - [ ]  replace sample rows + sample_indices within a transaction per `(artifact_id, root_key)`
- [ ]  Partial retrieval depends on artifact_samples:
    - [ ]  `artifact.select` loads records from artifact_samples
    - [ ]  corruption detection: if indices exist but sample rows missing -> `INTERNAL` with details

### 10.8 Worker safety and races

- [ ]  Worker writes are conditional:
    - [ ]  artifact not deleted
    - [ ]  map_status in (pending, stale)
    - [ ]  generation matches
- [ ]  If conditional update affects 0 rows, worker discards results.
- [ ]  map_error stored on failure with enough detail to debug.

## 11) Cursor system (signing, binding, staleness)

- [ ]  Cursor format implemented:
    - [ ]  `base64url(payload_bytes) + "." + base64url(hmac)`
    - [ ]  unpadded base64url
- [ ]  Cursor payload canonicalization is RFC 8785 (Addendum D):
    - [ ]  signature input is exactly the canonical payload bytes
- [ ]  Secrets stored at `DATA_DIR/state/secrets.json` with:
    - [ ]  `cursor_ttl_minutes`
    - [ ]  `active_secrets[]` version + b64 key
    - [ ]  `signing_secret_version` present in active list
    - [ ]  keys >= 32 random bytes
- [ ]  Verification logic:
    - [ ]  parse payload bytes
    - [ ]  expiration check -> `CURSOR_EXPIRED`
    - [ ]  secret version missing -> `CURSOR_INVALID`
    - [ ]  constant-time HMAC compare -> else `CURSOR_INVALID`
    - [ ]  binding checks enforce `CURSOR_STALE` for:
        - [ ]  where_canonicalization_mode mismatch
        - [ ]  traversal_contract_version mismatch
        - [ ]  artifact_generation mismatch
        - [ ]  partial sample_set_hash mismatch
        - [ ]  partial map_budget_fingerprint mismatch
- [ ]  Cursor binding fields exist exactly per tool:
    - [ ]  get binds target + normalized_jsonpath
    - [ ]  select binds root_path + select_paths_hash + where_hash
- [ ]  Partial mode cursor binding includes:
    - [ ]  map_budget_fingerprint (required)
    - [ ]  sample_set_hash computed from DB sample indices and compared

---

## 12) where DSL implementation (Addendum E)

- [ ]  Parser exists for the specified grammar (OR/AND/NOT, parentheses, comparisons).
- [ ]  Relative path evaluation uses JSONPath subset (must not start with `$`).
- [ ]  Missing path semantics implemented:
    - [ ]  comparisons false except special `!= null` semantics (as defined)
- [ ]  Wildcard semantics:
    - [ ]  existential: any match satisfies
    - [ ]  bounded by max wildcard expansion
- [ ]  Type semantics implemented exactly:
    - [ ]  numeric comparisons require numeric operands
    - [ ]  string comparisons lexicographic by codepoint
    - [ ]  boolean only supports = and !=
- [ ]  Compute accounting exists and is deterministic:
    - [ ]  increments per path segment and expansions and comparison op
    - [ ]  deterministic short-circuiting

---

## 13) Retention, pruning, and cleanup correctness

- [ ]  Touch policy implemented exactly:
    - [ ]  creation touches `artifacts.last_referenced_at`
    - [ ]  retrieval/describe touches if not deleted
    - [ ]  search does not touch last_referenced_at
- [ ]  Soft delete job exists:
    - [ ]  selects with SKIP LOCKED
    - [ ]  predicate rechecked on update
    - [ ]  sets deleted_at and increments generation
    - [ ]  does not remove payloads yet
- [ ]  Hard delete job exists:
    - [ ]  deletes eligible artifacts
    - [ ]  cascades remove `artifact_roots`, `artifact_refs`, `artifact_samples`
    - [ ]  deletes unreferenced `payload_blobs`
    - [ ]  cascades remove `payload_binary_refs`
    - [ ]  deletes `binary_blobs` unreferenced by payload_binary_refs
    - [ ]  removes corresponding filesystem blob files
    - [ ]  optional reconciliation: detects orphan files on disk and can report/remove
- [ ]  Quota enforcement exists:
    - [ ]  storage cap breach triggers prune behavior (as configured)

---

## 14) Indexing (even if “off” by default)

- [ ]  Code supports `index_status` lifecycle:
    - [ ]  off | pending | ready | partial | failed
- [ ]  `artifact.find` respects “sample-only unless index enabled” rule.
- [ ]  If indexing is truly out of project scope for now, code still must:
    - [ ]  store `index_status` fields
    - [ ]  return consistent behavior when off

---

## 15) Observability and debug-ability

- [ ]  Structured logging exists (structlog or equivalent) for:
    - [ ]  startup discovery per upstream
    - [ ]  request_key computation (hashes only, no secrets)
    - [ ]  reuse decision: hit/miss and why
    - [ ]  artifact creation path including:
        - [ ]  envelope sizes
        - [ ]  oversized JSON offload events
        - [ ]  binary blob writes and dedupe hits
    - [ ]  mapping runs (full/partial), budgets, stop_reason, counts
    - [ ]  cursor validation failures categorized (invalid/expired/stale)
    - [ ]  pruning operations and bytes reclaimed
- [ ]  Metrics counters exist (can be simple internal counters):
    - [ ]  advisory lock timeouts
    - [ ]  upstream call latency and error types
    - [ ]  mapping latency and stop reasons
    - [ ]  prune deletions and disk bytes reclaimed

---

## 16) Test suite completion criteria (must pass)

At minimum, tests exist and pass for:

- [ ]  RFC 8785 canonicalization vectors + numeric edge cases.
- [ ]  Compression roundtrip integrity: compressed canonical bytes decompress to same bytes and hash matches.
- [ ]  Reserved arg stripping removes only `_gateway_*` keys and explicit reserved names.
- [ ]  Oversized JSON ingest becomes byte-backed binary_ref and is used for streaming mapping.
- [ ]  Partial mapping determinism:
    - [ ]  same payload + same budgets => same sample_indices + same root inventory
    - [ ]  map_budget_fingerprint mismatch => stale behavior
- [ ]  Prefix coverage semantics:
    - [ ]  stop_reason != none => count_estimate null, prefix_coverage true, sampled_prefix_len correct
- [ ]  Sampling bias invariant:
    - [ ]  oversize records skipped and counted; sample_indices exclude them
- [ ]  Cursor determinism:
    - [ ]  same request and position => same cursor payload (before HMAC) and valid verification
- [ ]  CURSOR_STALE conditions:
    - [ ]  sample_set mismatch
    - [ ]  where_canonicalization_mode mismatch
    - [ ]  traversal_contract_version mismatch
    - [ ]  artifact_generation mismatch
- [ ]  Session discovery correctness:
    - [ ]  artifact.search only returns artifacts in artifact_refs for that session
    - [ ]  new artifact appears immediately
- [ ]  Cleanup correctness:
    - [ ]  payload_binary_refs prevents orphaning
    - [ ]  hard delete removes filesystem blobs only when unreferenced

Integration tests (strongly recommended to count as “done”):

- [ ]  A local upstream MCP stub (http or stdio) that can return:
    - [ ]  small JSON, large JSON, text, errors, and binary payload
- [ ]  End-to-end:
    - [ ]  mirrored call -> artifact created -> artifact.search finds it -> artifact.get retrieves envelope -> mapping runs -> artifact.describe shows roots -> artifact.select returns projections -> cursor pagination works

---

## 17) “Done means done” runtime validation script

- [ ]  A single command exists that executes a deterministic smoke test suite against a fresh DATA_DIR + fresh DB schema:
    - [ ]  starts gateway
    - [ ]  registers one stub upstream
    - [ ]  exercises:
        - [ ]  caching allow/fresh
        - [ ]  reuse by request_key
        - [ ]  error envelope creation
        - [ ]  oversized JSON offload
        - [ ]  partial mapping + artifact.select sampled-only + cursor continuation
        - [ ]  soft delete then hard delete then verify blobs removed
    - [ ]  exits non-zero on any invariant violation

---

## Repo-shaped completion checklist:

---

## 0) Repo skeleton and contracts

### Root files

- [ ]  `pyproject.toml`
    - [ ]  Pins Python `>=3.11`
    - [ ]  Declares deps: `fastmcp`, `psycopg[binary]` or `psycopg3`, `ijson`, `zstandard` (or `gzip` fallback), `structlog`, `orjson` (optional), `pydantic` (optional), `pytest`
    - [ ]  Defines `mcp-gateway` console script entrypoint
- [ ]  `README.md`
    - [ ]  Explains local-only, single-tenant, `DATA_DIR`, Postgres DSN
    - [ ]  Includes quickstart: run Postgres, migrate, run gateway, call mirrored tool
- [ ]  `docs/spec_v1_9.md` (copy of the spec, locked)
- [ ]  `docs/traversal_contract.md` (explicit ordering rules)
- [ ]  `docs/cursor_contract.md` (payload fields, binding rules, stale rules)
- [ ]  `docs/config.md` (all config keys + defaults)
- [ ]  `.env.example`
- [ ]  `docker-compose.yml` (optional but recommended for local Postgres)

### Package layout

- [ ]  `src/mcp_artifact_gateway/__init__.py`
- [ ]  `src/mcp_artifact_gateway/main.py` (CLI entry)
- [ ]  `src/mcp_artifact_gateway/app.py` (composition root: config → db → fs → upstreams → MCP server)

---

## 1) Configuration, constants, and lifecycle

### Config and limits

- [ ]  `src/mcp_artifact_gateway/config.py`
    - [ ]  Loads config from (in precedence): env vars → `DATA_DIR/state/config.json` → defaults
    - [ ]  Validates all caps/budgets exist (retrieval, mapping, JSON oversize caps, storage caps)
    - [ ]  Exposes:
        - [ ]  `DATA_DIR` and derived paths (`tmp/`, `logs/`, `blobs/`, `resources/`, `state/`)
        - [ ]  `envelope_jsonb_mode`, `envelope_jsonb_minimize_threshold_bytes`
        - [ ]  `max_json_part_parse_bytes` (oversized JSON becomes byte-backed binary ref)
        - [ ]  partial-map budgets (the full set used in `map_budget_fingerprint`)
        - [ ]  cursor TTL and secret rotation settings
- [ ]  `src/mcp_artifact_gateway/constants.py`
    - [ ]  `WORKSPACE_ID = "local"`
    - [ ]  `traversal_contract_version` constant
    - [ ]  `canonicalizer_version` constant
    - [ ]  `mapper_version` constant
    - [ ]  `prng_version` constant
    - [ ]  `cursor_version` constant
    - [ ]  Reserved key prefix: `_gateway_` and explicit reserved names

### Startup and shutdown

- [ ]  `src/mcp_artifact_gateway/lifecycle.py`
    - [ ]  Ensures directories exist, permissions ok, temp dir writable
    - [ ]  DB connect + migration check
    - [ ]  Upstream MCP connect + tool discovery
    - [ ]  Starts mapping worker loop if enabled
    - [ ]  Starts prune worker loop if enabled
    - [ ]  Clean shutdown closes upstream sessions, db pool, worker tasks

Acceptance

- [ ]  Running `mcp-gateway --check` prints: DB ok, FS ok, upstream ok, versions, budgets (mirrors `gateway.status`)

---

## 2) Postgres schema and migrations

### Migration framework

- [ ]  `src/mcp_artifact_gateway/db/migrate.py`
    - [ ]  Applies SQL migrations in order
    - [ ]  Records applied migrations (table `schema_migrations`)
    - [ ]  Fails hard if migrations missing

### Migration SQL

- [ ]  `src/mcp_artifact_gateway/db/migrations/001_init.sql`
    - [ ]  Creates tables exactly per spec: `sessions`, `binary_blobs`, `payload_blobs`, `payload_hash_aliases`, `payload_binary_refs`, `artifacts`, `artifact_refs`, `artifact_roots`
    - [ ]  All PKs include `workspace_id`
    - [ ]  All constraints and indexes exist (especially `created_seq` identity and ordering indexes)
- [ ]  `src/mcp_artifact_gateway/db/migrations/002_indexes.sql` (optional if you split)
    - [ ]  Adds the heavier indexes (request_key, created_seq, last_seen)

### DB access layer

- [ ]  `src/mcp_artifact_gateway/db/conn.py`
    - [ ]  psycopg3 connection pool
    - [ ]  typed helpers: `tx(fn)`, `fetchone`, `fetchall`, `execute`
- [ ]  `src/mcp_artifact_gateway/db/repos/*.py` (split by concern)
    - [ ]  `sessions_repo.py`
    - [ ]  `payloads_repo.py`
    - [ ]  `artifacts_repo.py`
    - [ ]  `mapping_repo.py`
    - [ ]  `prune_repo.py`

Acceptance

- [ ]  `pytest -k migrations` can create a new DB, migrate, and verify all columns/indexes exist
- [ ]  `created_seq desc` is the only “latest” selector everywhere it matters

---

## 3) Filesystem blob store (content-addressed) and atomic writes

### Binary store

- [ ]  `src/mcp_artifact_gateway/fs/blob_store.py`
    - [ ]  `put_bytes(raw_bytes, mime) -> BinaryRef`:
        - [ ]  `binary_hash = sha256(raw_bytes)`
        - [ ]  path = `DATA_DIR/blobs/bin/ab/cd/<binary_hash>`
        - [ ]  atomic write: temp in same dir → fsync → rename
        - [ ]  if exists: verify size, optional probe head/tail hashes
    - [ ]  `open_stream(binary_hash) -> IO[bytes]` for partial mapping byte-backed reads
    - [ ]  MIME normalization: lowercase, strip params, alias map

### Resource store (optional internal copy)

- [ ]  `src/mcp_artifact_gateway/fs/resource_store.py`
    - [ ]  Supports `resource_ref` durability rules (`internal` copies under `DATA_DIR/resources`)

Acceptance

- [ ]  Blob writes are crash-safe: kill process mid-write never leaves partial final file
- [ ]  `binary_blobs` rows match filesystem reality (byte_count and path)

---

## 4) Canonical JSON and hashing (no float drift)

### Canonicalizer

- [ ]  `src/mcp_artifact_gateway/canon/rfc8785.py`
    - [ ]  `canonical_bytes(obj) -> bytes` implementing RFC 8785
    - [ ]  Deterministic key ordering, UTF-8, number formatting
- [ ]  `src/mcp_artifact_gateway/canon/decimal_json.py`
    - [ ]  JSON loader that parses floats as `Decimal`, rejects NaN/Infinity
    - [ ]  Ensures canonicalization never sees Python float

### Hash utilities

- [ ]  `src/mcp_artifact_gateway/util/hashing.py`
    - [ ]  `sha256_hex(bytes)`, `sha256_trunc(bytes, n)`
    - [ ]  `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)`

Acceptance

- [ ]  RFC 8785 test vectors pass
- [ ]  Same envelope object always yields identical canonical bytes across runs

---

## 5) Envelope normalization and oversized JSON rule

### Envelope model

- [ ]  `src/mcp_artifact_gateway/envelope/model.py`
    - [ ]  Typed dataclasses or pydantic models for:
        - `Envelope`, `ContentPartJson`, `ContentPartText`, `ContentPartResourceRef`, `ContentPartBinaryRef`, `ErrorBlock`
- [ ]  `src/mcp_artifact_gateway/envelope/normalize.py`
    - [ ]  Converts upstream MCP response into canonical envelope shape
    - [ ]  Ensures: ok implies no error, error implies error present
    - [ ]  Never stores raw binary bytes in envelope

### Oversized JSON handling (byte-backed)

- [ ]  `src/mcp_artifact_gateway/envelope/oversize.py`
    - [ ]  If any JSON part exceeds `max_json_part_parse_bytes`:
        - [ ]  do not parse
        - [ ]  store raw bytes as `binary_ref` with `mime = application/json` (optionally `+encoding`)
        - [ ]  replace that part with a `binary_ref` descriptor
        - [ ]  add a warning in `meta.warnings` with original part index and encoding

Acceptance

- [ ]  A 200MB JSON result does not allocate 200MB Python objects
- [ ]  Partial mapping can later read the JSON from the binary blob stream

---

## 6) Payload storage (compressed canonical bytes) and integrity rule

### Payload persistence

- [ ]  `src/mcp_artifact_gateway/storage/payload_store.py`
    - [ ]  `compress(bytes) -> (encoding, compressed, uncompressed_len)`
    - [ ]  Supports `zstd|gzip|none`
    - [ ]  Writes `payload_blobs` row with:
        - [ ]  `envelope_canonical_bytes` compressed
        - [ ]  `envelope_canonical_bytes_len`
        - [ ]  `payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`
        - [ ]  `contains_binary_refs`
        - [ ]  `canonicalizer_version`
    - [ ]  Enforces integrity:
        - [ ]  `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))`
- [ ]  JSONB storage mode implemented:
    - [ ]  `full`
    - [ ]  `minimal_for_large` projection
    - [ ]  `none` projection

Acceptance

- [ ]  Payload retrieval can reconstruct envelope from canonical bytes even if jsonb is minimal/none

---

## 7) Artifact creation flow (mirroring, caching, stampede lock)

### Upstream discovery + mirroring

- [ ]  `src/mcp_artifact_gateway/mcp/upstream.py`
    - [ ]  Connects to each upstream MCP (stdio/http)
    - [ ]  Fetches tool list at startup
- [ ]  `src/mcp_artifact_gateway/mcp/mirror.py`
    - [ ]  Exposes mirrored tools as `{prefix}.{tool}` with identical schema/docs, no injected fields
    - [ ]  Strips reserved keys before schema validation and forwarding:
        - exact keys: `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq`
        - any key starting with `_gateway_`
        - nothing else

### Request identity

- [ ]  `src/mcp_artifact_gateway/request_identity.py`
    - [ ]  Computes `upstream_instance_id` (semantic identity excluding secrets)
    - [ ]  Computes `canonical_args_bytes` via RFC 8785 after reserved stripping and schema validation
    - [ ]  `request_key = sha256(upstream_instance_id|prefix|tool|canonical_args_bytes)`
    - [ ]  Persists `request_args_hash` and capped `request_args_prefix`

### Stampede lock and reuse

- [ ]  `src/mcp_artifact_gateway/cache/reuse.py`
    - [ ]  Advisory lock: derive two 32-bit keys from `sha256(request_key)` and `pg_advisory_lock` with timeout
    - [ ]  If `cache_mode != fresh`, tries reuse by `request_key` latest (`created_seq desc`)
    - [ ]  Strict gating by schema hash unless configured otherwise
    - [ ]  Optional dedupe alias reuse (`payload_hash_aliases`) constrained to same upstream_instance_id + tool

### Artifact write

- [ ]  `src/mcp_artifact_gateway/artifacts/create.py`
    - [ ]  Implements the full step sequence in §9.1
    - [ ]  Always stores an artifact even on upstream error/timeout (error envelope)
    - [ ]  Inserts:
        - payload blob row
        - payload_binary_refs rows
        - artifact row with `map_status=pending`, `map_kind=none` initially
        - artifact_refs row and session last_seen update

Acceptance

- [ ]  With DB and FS healthy, any upstream failure still yields a stored error artifact and returns a handle
- [ ]  If DB or FS required path unavailable, gateway returns INTERNAL and does not claim artifact creation

---

## 8) Mapping system (full and partial)

### Mapping orchestrator

- [ ]  `src/mcp_artifact_gateway/mapping/runner.py`
    - [ ]  Picks JSON part to map deterministically with tie-break by part index
    - [ ]  Decides full vs partial:
        - [ ]  full if size <= `max_full_map_bytes`
        - [ ]  partial if too large or stored as `binary_ref application/json(+encoding)`
    - [ ]  Stores results in `artifact_roots`, updates artifact mapping columns

### Full mapper

- [ ]  `src/mcp_artifact_gateway/mapping/full.py`
    - [ ]  Parses fully, discovers up to K roots (K=3), builds deterministic inventory, writes `artifact_roots`

### Partial mapper (streaming, deterministic)

- [ ]  `src/mcp_artifact_gateway/mapping/partial.py`
    - [ ]  Consumes byte stream only (binary_ref stream preferred)
    - [ ]  Enforces budgets and emits `stop_reason`
    - [ ]  Computes and stores:
        - `map_backend_id` derived from python + ijson backend+version
        - `prng_version` constant
        - `map_budget_fingerprint` hash over budgets + versions
    - [ ]  Root path normalization rules and no wildcards in root_path
    - [ ]  Streaming skip contract: can discard subtrees; compute steps count all events
    - [ ]  Deterministic reservoir sampling:
        - seed = sha256(payload_hash_full|root_path|map_budget_fingerprint)
        - reservoir algorithm exactly as specified
        - oversize sampled elements are skipped and counted (bias invariant)
        - sampled_prefix_len semantics
        - `sample_indices` stored sorted ascending and includes only materialized indices
        - count_estimate only when stop_reason none and closing array observed
    - [ ]  Inventory derivation from sampled records with caps
    - [ ]  If stop_reason != none:
        - prefix coverage true
        - count_estimate null

### Worker safety

- [ ]  `src/mcp_artifact_gateway/mapping/worker.py`
    - [ ]  Async/hybrid/sync modes supported
    - [ ]  Conditional update safety:
        - deleted_at null
        - map_status in (pending, stale)
        - generation matches snapshot
        - else discard results

Acceptance

- [ ]  Partial mapping deterministic across runs given identical payload and budgets (fingerprint unchanged)
- [ ]  Remapping with different budgets marks old mapping stale for mapped ops and cursors

---

## 9) Retrieval: JSONPath, select_paths, where hashing, traversal contract

### JSONPath subset + canonicalization

- [ ]  `src/mcp_artifact_gateway/query/jsonpath.py`
    - [ ]  Parser for allowed grammar only: `$`, `.name`, `['..']`, `[n]`, `[*]`
    - [ ]  Caps: length, segments, wildcard expansion total
- [ ]  `src/mcp_artifact_gateway/query/select_paths.py`
    - [ ]  Normalizes each path and rejects absolute `$` for select_paths
    - [ ]  Sorts lexicographically, dedupes, computes `select_paths_hash`
- [ ]  `src/mcp_artifact_gateway/query/where_hash.py`
    - [ ]  Implements `where_canonicalization_mode`:
        - raw_string hash mode
        - canonical_ast mode with commutative sort and numeric/string normalization
    - [ ]  Exposes mode via `gateway.status()`

### Traversal contract

- [ ]  `src/mcp_artifact_gateway/retrieval/traversal.py`
    - [ ]  Arrays index ascending, objects keys lex asc
    - [ ]  Wildcard expansions obey same ordering
    - [ ]  Partial mode enumerates sampled indices ascending

Acceptance

- [ ]  Given same artifact and same query, pagination yields identical item boundaries and cursors

---

## 10) Cursor signing, binding, and staleness

### Secrets

- [ ]  `src/mcp_artifact_gateway/cursor/secrets.py`
    - [ ]  Loads secret set from `DATA_DIR/state/secrets.json`
    - [ ]  Tracks active secret versions: newest signs, all active verify
- [ ]  `src/mcp_artifact_gateway/cursor/hmac.py`
    - [ ]  Format: `base64url(payload_json) + "." + base64url(hmac)`
    - [ ]  Enforces TTL and expires_at

### Cursor payload enforcement

- [ ]  `src/mcp_artifact_gateway/cursor/payload.py`
    - [ ]  Includes all required fields in §14.2
    - [ ]  Verifies server `where_canonicalization_mode` matches cursor else CURSOR_STALE

### Partial cursor binding

- [ ]  `src/mcp_artifact_gateway/cursor/sample_set_hash.py`
    - [ ]  Computes `sample_set_hash` from root_path + stored sample_indices + map_budget_fingerprint + mapper_version
    - [ ]  Verification recomputes from DB and mismatch => CURSOR_STALE

Acceptance

- [ ]  Cursor cannot be replayed against different where mode
- [ ]  Cursor from old partial mapping becomes stale after remap (different fingerprint or sample indices)

---

## 11) MCP tool surface: gateway.status and artifact tools

### Tool server

- [ ]  `src/mcp_artifact_gateway/mcp/server.py`
    - [ ]  Registers gateway tools:
        - `gateway.status`
        - `artifact.search`
        - `artifact.get`
        - `artifact.select`
        - `artifact.describe`
        - `artifact.find`
        - `artifact.chain_pages`
    - [ ]  Also registers mirrored upstream tools at `{prefix}.{tool}`

### Tool implementations

- [ ]  `src/mcp_artifact_gateway/tools/status.py`
    - [ ]  Returns: upstream connectivity, DB ok, FS ok, versions, traversal_contract_version, where mode, map_backend_id/prng_version, budgets, cursor TTL, secret versions
- [ ]  `src/mcp_artifact_gateway/tools/artifact_search.py`
    - [ ]  Lists artifacts using `artifact_refs` only
    - [ ]  Touch policy: updates session/artifact_refs last_seen, does not touch artifact last_referenced
- [ ]  `src/mcp_artifact_gateway/tools/artifact_get.py`
    - [ ]  target `envelope` applies jsonpath on envelope root, reconstruct from canonical bytes if needed
    - [ ]  target `mapped` only if map_status ready and map_kind full/partial
    - [ ]  Touch semantics: touch last_referenced_at if not deleted, always update session/artifact_refs, else GONE
- [ ]  `src/mcp_artifact_gateway/tools/artifact_select.py`
    - [ ]  Full mapping: bounded deterministic scan
    - [ ]  Partial mapping: sampled-only enumeration and response includes sampled_only, sample_indices_used, sampled_prefix_len
- [ ]  `src/mcp_artifact_gateway/tools/artifact_describe.py`
    - [ ]  Includes partial mapping disclosures: sampled-only constraints, prefix coverage, stop_reason, counts
- [ ]  `src/mcp_artifact_gateway/tools/artifact_find.py`
    - [ ]  Sample-only unless index enabled
- [ ]  `src/mcp_artifact_gateway/tools/artifact_chain_pages.py`
    - [ ]  Orders by chain_seq asc then created_seq asc, allocates chain_seq with retry

### Standard bounded response shape

- [ ]  `src/mcp_artifact_gateway/retrieval/response.py`
    - [ ]  Always returns `{items, truncated, cursor, omitted, stats}`

Acceptance

- [ ]  All tools require `_gateway_context.session_id` and reject missing with INVALID_ARGUMENT
- [ ]  Any truncation yields deterministic cursor and position encoding per traversal contract

---

## 12) Session tracking and touch policy

### Session enforcement

- [ ]  `src/mcp_artifact_gateway/sessions.py`
    - [ ]  Creates or updates session row with last_seen_at
    - [ ]  Upserts artifact_refs (first_seen_at, last_seen_at)

### Touch rules

- [ ]  Implemented exactly:
    - [ ]  creation touches artifacts.last_referenced_at
    - [ ]  retrieval/describe touches if not deleted
    - [ ]  search does not touch

Acceptance

- [ ]  Prune policies behave correctly because touch semantics are correct

---

## 13) Pruning, hard delete, and filesystem cleanup

### Soft delete job

- [ ]  `src/mcp_artifact_gateway/jobs/soft_delete.py`
    - [ ]  Uses SKIP LOCKED, rechecks predicates on update, sets deleted_at and generation++

### Hard delete job

- [ ]  `src/mcp_artifact_gateway/jobs/hard_delete.py`
    - [ ]  Deletes artifacts, cascades remove artifact_roots and artifact_refs
    - [ ]  Deletes unreferenced payload_blobs
    - [ ]  Deletes unreferenced binary_blobs via payload_binary_refs
    - [ ]  Removes filesystem blobs for removed binary_blobs

### Reconciler (optional but strongly recommended)

- [ ]  `src/mcp_artifact_gateway/jobs/reconcile_fs.py`
    - [ ]  Finds orphan files not referenced in DB and optionally removes them

Acceptance

- [ ]  End-to-end: create artifacts with binaries, delete them, filesystem blobs disappear only when unreferenced

---

## 14) Observability, metrics, and determinism logging

- [ ]  `src/mcp_artifact_gateway/obs/logging.py`
    - [ ]  structlog configuration, JSON logs
    - [ ]  Correlation fields: session_id, artifact_id, request_key, payload_hash_full
- [ ]  `src/mcp_artifact_gateway/obs/metrics.py` (optional)
    - [ ]  Counters:
        - cache hits, alias hits, upstream calls
        - oversize JSON count
        - partial map stop_reason distribution
        - cursor stale reasons
        - advisory lock timeouts
- [ ]  Determinism debug logs:
    - [ ]  map_budget_fingerprint
    - [ ]  map_backend_id
    - [ ]  prng_version
    - [ ]  sample_set_hash on cursor issue/verify

Acceptance

- [ ]  Given a cursor stale event, logs show which binding field mismatched

---

## 15) Test suite, fixtures, and “done means done” gates

### Unit tests (must exist)

- [ ]  `tests/test_reserved_arg_stripping.py`
    - [ ]  Only `_gateway_*` removed, nothing else
- [ ]  `tests/test_rfc8785_vectors.py`
- [ ]  `tests/test_decimal_json_no_float.py`
- [ ]  `tests/test_payload_canonical_integrity.py`
- [ ]  `tests/test_oversize_json_becomes_binary_ref.py`
- [ ]  `tests/test_partial_mapping_determinism.py`
    - [ ]  same bytes + same budgets => identical sample_indices and fields_top
- [ ]  `tests/test_prefix_coverage_semantics.py`
    - [ ]  stop_reason != none implies count_estimate null and prefix coverage true
- [ ]  `tests/test_sampling_bias_invariant.py`
    - [ ]  oversize sampled elements are skipped and counted
- [ ]  `tests/test_cursor_sample_set_hash_binding.py`
- [ ]  `tests/test_cursor_where_mode_stale.py`
- [ ]  `tests/test_touch_policy.py`

### Integration tests (must exist)

- [ ]  `tests/integration/test_full_flow_small_json.py`
    - [ ]  mirrored call → artifact created → mapping ready → select works → cursor pages
- [ ]  `tests/integration/test_flow_large_json_partial_map.py`
    - [ ]  oversize JSON stored as binary_ref → partial mapping reads stream → sampled-only select works
- [ ]  `tests/integration/test_prune_cleanup.py`
    - [ ]  soft delete then hard delete cleans DB and filesystem

### “Ship gate” criteria

- [ ]  All tests pass in CI on Linux
- [ ]  A local demo script produces:
    - [ ]  one small JSON artifact that returns inline envelope
    - [ ]  one large JSON artifact that returns handle and supports sampled-only select
    - [ ]  cursor pagination stable across two identical runs
- [ ]  No tool ever returns unbounded bytes/items
- [ ]  Determinism artifacts are visible: traversal_contract_version, map_budget_fingerprint, sample_set_hash appear in responses where relevant

---