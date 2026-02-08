# Task Plan Resolution Tracker

Last updated: 2026-02-08

## Context
- This tracker follows `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/task_plan_evaluation.md`.
- Purpose: keep a durable execution log across long-running implementation work and memory compaction.
- Scope rule: resolve only issues already identified in the evaluation; no scope expansion.

## Already Fixed Before This Pass
- `C1` LIKE wildcard injection in `artifact_search.py`.
- `C2` `map_budget_fingerprint` divergence between `hashing.py` and `partial.py`.
- `C3` `_json_type_name` divergence between `full.py` and `partial.py`.
- Important issues and suggestions listed by user were already addressed by a prior agent unless otherwise re-opened by new evidence.

## Action Items (from evaluation)
| ID | Title | Status | Notes |
|---|---|---|---|
| AI-01 | Real serve/bootstrap runtime wiring | **Done** | Verified: `main.py` fully wired (config → DB pool → FS checks → upstream → MCP server). No changes needed. |
| AI-02 | Mirrored tool execution pipeline end-to-end | **Done** | Verified + fixed: G20 uniform response contract enforced in `server.py` (`gateway_error` used 46 times). |
| AI-03 | Runtime retrieval tool handlers | **Done** | Verified: all retrieval handlers enforce cursor bindings and configured JSONPath caps. |
| AI-04 | Real upstream discovery + invocation | **Done** | Verified: `upstream.py` has real `fastmcp.Client`-based discovery/invocation. No changes needed. |
| AI-05 | Schema/migration/pooling fidelity gaps | **Done** | Verified: `index_status` CHECK has all 5 values, `conn.py` has ConnectionPool, `migrate.py` fails hard. No changes needed. |
| AI-06 | Advisory lock timeout + strict request validation | **Done** | Verified + fixed: G14 `mirror.py` docstring/variable naming for strict schema validation. |
| AI-07 | Mapping integration and persistence lifecycle | **Done** | Fixed: `MappingResult.mapped_part_index` type `int` → `int\|None`, added `mapping/__init__.py` re-exports. +28 tests. |
| AI-08 | Full where grammar + exact semantics | **Done** | Fixed: bool/int coercion in eq/ne/in operators via `_strict_eq()`, added `query/__init__.py` exports. +139 tests. |
| AI-09 | Executable pruning job orchestration | **Done** | Fixed: added `run_reconcile` orchestration function to `reconcile_fs.py`, added `jobs/__init__.py` exports. +6 tests. |
| AI-10 | Observability wired into live flows | **Done** | Fixed: added `GatewayMetrics.reset()` method for atomic counter/histogram reset. +3 tests. |
| AI-11 | Integration/e2e verification | **Done** | Added 7 new integration tests covering G45 gaps (session isolation, visibility, orphan protection, delete lifecycle). |

## Progress Log
- 2026-02-08: Created tracker and aligned it to evaluation action-item catalog.
- 2026-02-08: Started AI-05 implementation pass.
- 2026-02-08: AI-05 batch 1 implemented:
  - Added sequence and presence validation in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/db/migrate.py` (fail fast for missing/gapped/invalid migration filenames).
  - Added psycopg3 pool helper `create_pool` in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/db/conn.py`.
  - Aligned `index_status` lifecycle values in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/db/migrations/001_init.sql` and `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/db/repos/artifacts_repo.py`.
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_db_migrate.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_db_helpers.py`
  - Validation: `397 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-04 batch 1 implemented:
  - Replaced discovery/call stubs in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/upstream.py` with real `fastmcp.Client`-based logic.
  - Added upstream connection orchestration helpers: `connect_upstream`, `connect_upstreams`.
  - Added normalization for upstream call results (content, structured content, error flag, meta).
  - Extended tests in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_upstream.py`.
  - Validation: `401 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-01 batch 1 implemented:
  - Replaced serve placeholder in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/main.py` with executable runtime wiring:
    - startup checks
    - migration application
    - pool + blob store bootstrap
    - FastMCP server run path
  - Added unit coverage in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_main.py`.
- 2026-02-08: AI-02 batch 1 implemented:
  - Reworked `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py` into executable runtime server:
    - registers built-in tools and mirrored upstream tools
    - strict schema-violation errors for mirrored args
    - request identity + reuse lookup path
    - upstream invocation + envelope normalization
    - persisted artifact handle flow
  - Added executable persistence helper `persist_artifact` in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/artifacts/create.py`.
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_artifact_create.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_tool_status.py`
  - Validation: `410 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-06 batch 1 implemented:
  - Added advisory lock acquisition helpers with timeout + metric hooks in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/cache/reuse.py`.
  - Wired lock timeout behavior into mirrored DB-backed execution flow in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`.
  - Added strict validation for mirrored `_gateway_parent_artifact_id` and `_gateway_chain_seq` argument types.
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_reuse.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`
  - Validation: `415 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-03 batch 1 implemented:
  - Replaced retrieval handler stubs in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py` with DB-backed runtime behavior for:
    - `artifact.search`
    - `artifact.get`
    - `artifact.describe`
    - `artifact.select`
    - `artifact.find`
    - `artifact.chain_pages`
  - Added runtime cursor signing/verification integration and session visibility gating for retrieval entrypoints.
  - Added bounded output handling (`max_items`, `max_bytes_out`) in retrieval flows.
  - Added search query offset support in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/tools/artifact_search.py`.
  - Extended tests in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`.
  - Validation: `421 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-07 batch 1 implemented:
  - Extended `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mapping/runner.py` to select JSON mapping sources from both structured `json` parts and `binary_ref` JSON blobs, with deterministic size scoring and stream-backed partial mapping input.
  - Implemented executable worker lifecycle in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mapping/worker.py`:
    - generation-safe conditional artifact mapping updates
    - transactional root replacement in `artifact_roots`
    - atomic sample replacement in `artifact_samples` for partial mappings
    - discard-on-race behavior when conditional updates affect zero rows
  - Wired mode-aware mapping triggering into mirrored runtime in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`:
    - `sync`/`hybrid`: inline mapping worker execution
    - `async`: background mapping scheduling
  - Updated artifact handle persistence metadata in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/artifacts/create.py` and `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py` to carry generation snapshots needed for worker safety checks.
  - Added/extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_mapping_runner.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_mapping_worker.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_artifact_handle.py`
  - Validation: `428 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-07 batch 2 implemented:
  - Enriched partial mapping summaries in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mapping/partial.py` with persisted fields used by retrieval contracts:
    - `sampled_prefix_len`
    - `sampled_record_count`
    - `prefix_coverage`
    - `stop_reason`
    - `skipped_oversize_records`
  - Updated `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/tools/artifact_select.py` and `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py` so `artifact.select` reads persisted `sampled_prefix_len` from root metadata instead of inferring it from sample count.
  - Updated `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/tools/artifact_describe.py` so partial roots expose persisted metadata (`sampled_prefix_len`, `prefix_coverage`, `stop_reason`, `sampled_record_count`, `skipped_oversize_records`).
  - Extended coverage in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`.
  - Validation: `428 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-09 batch 1 implemented:
  - Added executable soft-delete batch functions in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/jobs/soft_delete.py`:
    - `run_soft_delete_expired`
    - `run_soft_delete_unreferenced`
    - both with commit/rollback behavior and concrete `SoftDeleteResult` outputs
  - Added executable hard-delete orchestration in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/jobs/hard_delete.py`:
    - `run_hard_delete_batch` now executes artifact deletion, payload cleanup, blob cleanup, optional filesystem blob removal, and reclaimed-byte accounting
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_soft_delete.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_hard_delete.py`
  - Validation: `433 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-10 batch 1 implemented:
  - Added live runtime metrics instrumentation in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`:
    - upstream call count/error/latency
    - cache hit/miss counters in reuse flow
    - cursor invalid/expired/stale reason counters
    - oversize JSON ingestion counter
  - Added mapping metrics emission in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mapping/worker.py`:
    - mapping full/partial/failed counts
    - mapping latency histogram
    - partial-map stop-reason distribution
  - Added pruning metrics hooks in executable jobs:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/jobs/soft_delete.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/jobs/hard_delete.py`
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_mapping_worker.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_soft_delete.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_hard_delete.py`
  - Validation: `436 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-08 batch 1 implemented:
  - Enhanced `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/query/where_dsl.py` semantics:
    - supports relative paths (auto-anchored to `$`)
    - applies missing-path rule: comparisons false except `!= null`
    - wildcard predicates use existential semantics over all matches
    - wildcard expansion is bounded with explicit error when exceeded
    - ordered comparisons now enforce stricter numeric/string operand typing
    - compute accounting now includes path traversal and expansion work units
  - Wired wildcard expansion limit from runtime config into select/find handlers in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`.
  - Extended tests in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_where_dsl.py`.
  - Validation: `440 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-08 batch 2 implemented:
  - Added textual where parser in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/query/where_dsl.py`:
    - supports `AND`/`OR`/`NOT`, parentheses, comparison operators, `IN`, `CONTAINS`, and `EXISTS(...)`
    - parses relative path expressions with dot/bracket segments
    - evaluates parsed string expressions directly in `evaluate_where`
  - Updated `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/query/where_hash.py` so `canonical_ast` mode canonicalizes parsed string expressions instead of hashing raw text.
  - Updated `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py` to accept both object and string `where` inputs for select/find runtime handlers.
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_where_dsl.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_where_hash.py`
  - Validation: `443 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-11 batch 1 implemented:
  - Added integration tests in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/integration/test_postgres_runtime.py` covering:
    - real-Postgres artifact persistence + search/get runtime handlers
    - mirrored-tool persistence flow against real Postgres storage path
  - Integration tests are environment-gated and auto-skip when `MCP_GATEWAY_TEST_POSTGRES_DSN` is not provided.
  - Validation: `443 passed, 2 skipped` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`.
- 2026-02-08: AI-03/AI-07/AI-10 batch 2/3 implemented (cursor stale fidelity hardening):
  - Hardened runtime cursor binding in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`:
    - `artifact.get` cursors now bind `target`, canonicalized `jsonpath`, and `artifact_generation`.
    - `artifact.select` cursors now bind `root_path`, `select_paths_hash`, `where_hash`, `artifact_generation`, and for partial maps `map_budget_fingerprint` + recomputed `sample_set_hash`.
    - `artifact.find` cursors now bind `root_path_filter`, `where_hash`, `artifact_generation`, and partial-map `map_budget_fingerprint`.
  - Extended cursor stale-reason observability:
    - Added `map_budget_mismatch` classification in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`.
    - Added `cursor_stale_map_budget` metric and snapshot surface in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/obs/metrics.py`.
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_metrics.py`
  - Validation:
    - `38 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q tests/unit/test_server.py tests/unit/test_metrics.py`
    - `448 passed, 2 skipped` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`
- 2026-02-08: AI-03/AI-08 batch 3 implemented (JSONPath cap enforcement):
  - Added configurable cap enforcement in `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/query/jsonpath.py`:
    - max JSONPath length
    - max segment count
    - max wildcard expansion total
  - Propagated cap-aware path handling to retrieval runtime:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/mcp/server.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/src/mcp_artifact_gateway/query/select_paths.py`
  - Extended tests:
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_jsonpath.py`
    - `/Users/lourenco/Documents/GitHub/mcp-artifact-gateway/tests/unit/test_server.py`
  - Validation:
    - `34 passed` via `UV_CACHE_DIR=.uv-cache uv run pytest -q tests/unit/test_jsonpath.py tests/unit/test_select_paths.py tests/unit/test_server.py`
    - `451 passed, 2 skipped` via `UV_CACHE_DIR=.uv-cache uv run pytest -q`
- 2026-02-08: Final verification pass — 6 parallel agents audited all 11 action items against evaluation groups:
  - **Agent 1 (AI-01, AI-04, AI-05)**: Verified all three items fully implemented. No changes needed.
  - **Agent 2 (AI-07)**: Fixed `MappingResult.mapped_part_index` type from `int` to `int|None` (no-JSON-content-part failure now returns `None` instead of `0`). Added re-exports to `mapping/__init__.py`. +28 new tests across `test_mapping_runner.py`, `test_mapping_worker.py`, `test_full_mapping.py`, `test_partial_mapping_determinism.py`.
  - **Agent 3 (AI-08)**: Fixed bool/int coercion bug in `where_dsl.py` eq/ne/in operators — added `_strict_eq()` helper to prevent Python `1 == True` coercion. Added missing exports to `query/__init__.py` (`parse_where_expression`, `parse_jsonpath`, `Segment`). Expanded query test coverage from 24 to 163 tests across `test_where_dsl.py`, `test_where_hash.py`, `test_jsonpath.py`.
  - **Agent 4 (AI-09, AI-10)**: Added `run_reconcile` orchestration function to `reconcile_fs.py` (DB query, FS scan, orphan detection, optional removal, metrics). Added `GatewayMetrics.reset()` method to `metrics.py` for atomic counter/histogram reset. Updated `jobs/__init__.py` with proper exports. +9 new tests.
  - **Agent 5 (AI-02, AI-03, AI-06)**: Fixed G20 uniform response contract in `server.py` (`gateway_error` used consistently, only 1 bare `"code":` remaining inside envelope error block — correct). Fixed G14 `mirror.py` docstring/variable naming for strict schema validation.
  - **Agent 6 (AI-11)**: Added 7 new integration tests covering all G45 gaps: session isolation in search, immediate visibility after creation, cross-session retrieval blocked, `payload_binary_refs` recorded on persist, hard delete removes only unreferenced payloads, binary ref protection during hard delete, soft-delete visibility lifecycle.
  - **Final validation**: `627 passed` via `python -m pytest tests/unit/ -q` (up from 451 baseline, +176 new tests, 0 failures)
  - All 11 action items marked **Done**.
- 2026-02-08: Second verification pass — 5 parallel agents audited under-verified evaluation groups for remaining gaps:
  - **Agent 1 (G03, G50 — Schema completeness)**:
    - Fixed `created_seq` from `BIGSERIAL` to `GENERATED ALWAYS AS IDENTITY` in `001_init.sql`
    - Moved 4 columns (`upstream_tool_schema_hash`, `request_args_hash`, `request_args_prefix`, `mapped_part_index`) from ALTER TABLE in `002_indexes.sql` to base CREATE TABLE in `001_init.sql`
    - Added missing FK indexes: `idx_artifacts_session_id`, `idx_artifacts_payload_hash`
    - Added `idx_artifacts_request_args_hash` partial index in `002_indexes.sql`
    - +60 new schema audit tests in `test_db_migrate.py`
  - **Agent 2 (G21, G22 — Status health snapshot)**:
    - Added `probe_db(db_pool)` — executes `SELECT 1` against pool, returns health dict
    - Added `probe_fs(config)` — checks `data_dir`, `state_dir`, `blobs_bin_dir` via `Path.is_dir()`
    - Changed `build_status_response_with_runtime` from bool flags to rich health dicts
    - Added `mapping_mode`, `cursor_secrets_info`, upstream connectivity snapshot fields
    - Updated `server.py` `handle_status` to call probes at request time
    - +11 new tests in `test_tool_status.py` and `test_server.py`
  - **Agent 3 (G43, G84 — Observability wiring depth)**:
    - Added structured logging (`get_logger` + `LogEvents`) to all 6 operational modules: `mapping/worker.py`, `cache/reuse.py`, `artifacts/create.py`, `jobs/soft_delete.py`, `jobs/hard_delete.py`, `jobs/reconcile_fs.py`
    - Added `cache_hits`/`cache_misses` metric increments in `reuse.py`
    - Added `oversize_json_count` and `binary_blob_writes` metric increments in `create.py`
    - +11 new tests across reuse, create, worker, soft_delete, hard_delete, logging
  - **Agent 4 (G07, G08, G17, G58 — Oversize JSON + Canonicalization)**:
    - Fixed 3 Decimal-safety bugs: `json.loads` → `loads_decimal` in `request_identity.py` and `payload_store.py`, `json.dumps` → `canonical_bytes` in `payload_store.py`
    - Added optional `max_json_part_parse_bytes` and `blob_store` params to `normalize_envelope` for oversize integration
    - +26 new tests (18 oversize pipeline + 4 canonical integrity + 4 request identity)
  - **Agent 5 (G72 — Retrieval traversal semantics)**:
    - Added `traverse_sampled(records, sample_indices)` to `traversal.py` yielding `(path, sample_index, record)` in ascending order
    - Added sampled-index helpers to `artifact_select.py`, `artifact_get.py`, `artifact_find.py`
    - Enhanced traversal_v1 contract docstrings
    - +35 new tests (25 find + 6 select + 4 get)
  - **Final validation**: `794 passed, 0 failed` via `python -m pytest tests/unit/ -q` (up from 627, +167 new tests)
