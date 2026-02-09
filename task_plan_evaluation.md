# Task Plan Evaluation (Done Items)

Audit date: 2026-02-08

## Scope and Method
- Source of truth: all checklist entries marked `[x]` in `task_plan.md` (625 original done entries + 40 added retroactively from PR #11).
- Validation approach: static code review of implementation files + unit-test review + full unit run (`uv run pytest -q`).
- Test baseline during this audit: `397 passed` (unit-level only). Updated to `892 passed` after PR #11 additions.
- Constraint honored: no new scope was introduced; findings are tied to intended scope in `task_plan.md`.
- Retroactive additions (2026-02-09): PR #11 introduced new features (standard mcpServers format, init command, Docker auto-provisioning) tracked as groups G86-G89.

## Verdict Summary
- `Verified`: 216 (176 original + 40 from PR #11 additions)
- `Partial`: 321
- `Not Implemented`: 128

Verdict meaning:
- `Verified`: implemented and consistent with claim at current project stage.
- `Partial`: some implementation exists, but claim overstates completeness/spec fidelity/integration.
- `Not Implemented`: primarily stubbed or missing runtime path for the claimed behavior.

## Action Item Catalog
- `AI-01`: Implement a real serve/bootstrap path that wires config, DB, filesystem checks, upstream connections, and MCP server runtime (not placeholder output). *Partially advanced by PR #11: serve() now loads config, runs startup checks, applies migrations, bootstraps MCP server, and exposes `init` subcommand.*
- `AI-02`: Implement the mirrored tool execution pipeline end-to-end: reserved arg stripping, strict schema validation, request identity, reuse checks, upstream invocation, envelope persistence, and response contract.
- `AI-03`: Implement runtime handlers for retrieval tools (`artifact.search/get/select/describe/find/chain_pages`) with real DB reads/writes, cursor handling, budget enforcement, and touch policy application.
- `AI-04`: Replace upstream stubs with real stdio/http MCP discovery and tool invocation clients; propagate connectivity state into status and startup behavior.
- `AI-05`: Close schema/migration fidelity gaps: add missing lifecycle/check constraints/indexes, ensure migration completeness checks, and provide psycopg3 pooling instead of single-connection helpers.
- `AI-06`: Implement advisory lock acquisition with timeout behavior and instrumentation, and enforce strict request validation semantics where currently warning-only/helper-only.
- `AI-07`: Complete mapping integration: binary-backed partial mapping sources, transactional writes to `artifact_roots`/`artifact_samples`, stale generation handling, and runtime scheduling/worker orchestration.
- `AI-08`: Implement the full where grammar parser and exact operator/type/wildcard/compute semantics claimed by the plan; current implementation is AST-evaluator-only and does not fully match the checklist.
- `AI-09`: Turn pruning SQL/helpers into executable jobs that actually run soft delete, hard delete, payload/blob cleanup, and optional filesystem reconciliation in production flow.
- `AI-10`: Wire structured logging and metrics counters/histograms into real request/mapping/pruning/cursor flows; current observability is mostly definitions without end-to-end instrumentation.
- `AI-11`: Add integration/e2e tests with real Postgres + representative upstream MCP behavior to verify runtime semantics (not only helper-level unit assertions).

## Group-Level Findings
| Group | Lines | Done Tasks | Verdict | Feedback | Action Items | Evidence |
|---|---:|---:|---|---|---|---|
| G01 | 11-23 | 13 | Partial | Phase completion is over-claimed relative to runtime behavior; core flows remain helper/stub based. | AI-01, AI-02, AI-03, AI-11 | `src/mcp_artifact_gateway/main.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G02 | 48-58 | 11 | Partial | Module boundaries exist, but several modules do not yet deliver the claimed runtime capabilities. | AI-01, AI-04, AI-05 | `src/mcp_artifact_gateway/db/conn.py`, `src/mcp_artifact_gateway/mcp/upstream.py` |
| G03 | 101-111 | 11 | Partial | Core tables exist, but "exactly per spec" and full index/constraint coverage claims are not fully satisfied. | AI-05 | `src/mcp_artifact_gateway/db/migrations/001_init.sql` |
| G04 | 114-114 | 1 | Verified | Migration ordering/idempotency behavior is implemented and tested at helper level. | None | `src/mcp_artifact_gateway/db/migrate.py`, `tests/unit/test_db_migrate.py` |
| G05 | 115-115 | 1 | Partial | Advisory lock helpers exist, but acquisition timeout behavior and instrumentation are not wired end-to-end. | AI-06, AI-11 | `src/mcp_artifact_gateway/cache/reuse.py` |
| G06 | 121-140 | 20 | Verified | Filesystem directory creation and content-addressed blob/resource stores are implemented with atomic-write patterns. | None | `src/mcp_artifact_gateway/lifecycle.py`, `src/mcp_artifact_gateway/fs/blob_store.py`, `src/mcp_artifact_gateway/fs/resource_store.py` |
| G07 | 146-165 | 20 | Partial | Canonicalization and hashing core are implemented, but dedupe-exclusion and persistence-path claims are broader than current runtime coverage. | AI-02, AI-06, AI-11 | `src/mcp_artifact_gateway/canon/rfc8785.py`, `src/mcp_artifact_gateway/request_identity.py` |
| G08 | 169-188 | 20 | Partial | Duplicate canonicalization section has the same gap profile as lines 146-165. | AI-02, AI-06, AI-11 | `src/mcp_artifact_gateway/canon/rfc8785.py` |
| G09 | 194-196 | 3 | Not Implemented | Upstream discovery/mirroring at startup is stubbed (no live tool discovery). | AI-04, AI-01, AI-11 | `src/mcp_artifact_gateway/mcp/upstream.py` |
| G10 | 197-200 | 4 | Verified | Reserved gateway argument stripping behavior is implemented and tested. | None | `src/mcp_artifact_gateway/mcp/mirror.py`, `tests/unit/test_reserved_arg_stripping.py` |
| G11 | 201-205 | 5 | Partial | Identity computation is present, but some claims (for example persisted auth fingerprint behavior) are not fully integrated. | AI-02, AI-04 | `src/mcp_artifact_gateway/mcp/upstream.py` |
| G12 | 206-209 | 4 | Partial | Request-key helper logic exists, but full post-validation persistence/runtime semantics are not wired end-to-end. | AI-02, AI-06 | `src/mcp_artifact_gateway/request_identity.py` |
| G13 | 217-218 | 2 | Not Implemented | Mirrored-call runtime entrypoint is missing, so these pipeline steps are not executed in production flow. | AI-02, AI-11 | `src/mcp_artifact_gateway/mcp/server.py` |
| G14 | 219-221 | 3 | Partial | Helpers for stripping and identity exist, but schema validation is warning-level and not strict enforcement in runtime. | AI-02, AI-06 | `src/mcp_artifact_gateway/mcp/mirror.py`, `src/mcp_artifact_gateway/request_identity.py` |
| G15 | 222-229 | 8 | Not Implemented | Advisory lock/reuse pipeline behavior is not implemented in an executable mirrored-tool flow. | AI-02, AI-06, AI-11 | `src/mcp_artifact_gateway/cache/reuse.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G16 | 231-235 | 5 | Verified | Envelope normalization rules and part typing are implemented with validation for error/content invariants. | None | `src/mcp_artifact_gateway/envelope/normalize.py`, `src/mcp_artifact_gateway/envelope/model.py` |
| G17 | 236-241 | 6 | Partial | Oversize JSON handling exists as helper logic, but full ingest/runtime behavior and raw-byte semantics are not fully represented end-to-end. | AI-02, AI-11 | `src/mcp_artifact_gateway/envelope/oversize.py` |
| G18 | 242-253 | 12 | Not Implemented | Persistence sequence steps are defined as helpers/SQL snippets but not executed by a real mirrored-tool pipeline. | AI-02, AI-11 | `src/mcp_artifact_gateway/artifacts/create.py`, `src/mcp_artifact_gateway/db/repos/payloads_repo.py` |
| G19 | 254-256 | 3 | Partial | Response contract helpers exist but are not enforced by live mirrored-tool handlers. | AI-02, AI-03 | `src/mcp_artifact_gateway/envelope/responses.py` |
| G20 | 263-271 | 9 | Not Implemented | Uniform tool response/error contracts are not consistently produced by runtime handlers (many return NOT_IMPLEMENTED). | AI-02, AI-03, AI-11 | `src/mcp_artifact_gateway/mcp/server.py` |
| G21 | 279-282 | 4 | Not Implemented | Status health snapshot fields are currently placeholders/hardcoded instead of runtime-derived checks. | AI-03, AI-04, AI-11 | `src/mcp_artifact_gateway/tools/status.py` |
| G22 | 283-287 | 5 | Partial | Version/budget reporting exists, but backend/secret version completeness and live health linkage are incomplete. | AI-03, AI-04 | `src/mcp_artifact_gateway/tools/status.py`, `src/mcp_artifact_gateway/cursor/secrets.py` |
| G23 | 291-297 | 7 | Partial | Search argument/query-shape helpers are present, but full runtime execution/cursor behavior is not implemented. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_search.py` |
| G24 | 298-302 | 5 | Not Implemented | Search touch/cursor binding semantics are not wired in an executable search handler. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_search.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G25 | 321-333 | 13 | Not Implemented | artifact.get runtime behavior is mostly unimplemented beyond argument/precondition helpers. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_get.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G26 | 337-342 | 6 | Not Implemented | artifact.describe behavior is partially modeled in response builders but not implemented as full runtime flow. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_describe.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G27 | 346-354 | 9 | Partial | Input normalization/hash helpers exist, but full select execution semantics are only partially implemented. | AI-03, AI-08, AI-11 | `src/mcp_artifact_gateway/tools/artifact_select.py`, `src/mcp_artifact_gateway/query/select_paths.py` |
| G28 | 356-367 | 12 | Not Implemented | Full/partial select traversal/projection contracts are not implemented as executable tool behavior. | AI-03, AI-07, AI-11 | `src/mcp_artifact_gateway/tools/artifact_select.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G29 | 371-372 | 2 | Not Implemented | artifact.find runtime behavior is not implemented beyond response-shape helpers. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_find.py`, `src/mcp_artifact_gateway/mcp/server.py` |
| G30 | 376-378 | 3 | Partial | Ordering SQL exists, but chain-seq allocation/retry semantics are not fully implemented in runtime. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_chain_pages.py` |
| G31 | 386-387 | 2 | Partial | mapping_mode enum exists, but scheduling of mapping work from artifact creation is not wired end-to-end. | AI-07, AI-11 | `src/mcp_artifact_gateway/config/settings.py`, `src/mcp_artifact_gateway/mapping/worker.py` |
| G32 | 391-392 | 2 | Partial | Deterministic scoring helper exists, but mapped part index persistence/runtime usage is incomplete. | AI-07 | `src/mcp_artifact_gateway/mapping/runner.py` |
| G33 | 398-402 | 5 | Partial | Full mapping computation exists, but artifact_roots write/update lifecycle is not fully wired. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/full.py`, `src/mcp_artifact_gateway/mapping/worker.py` |
| G34 | 406-408 | 3 | Partial | Partial mapping trigger conditions are only partially implemented (not fully handling binary-backed JSON cases). | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/runner.py` |
| G35 | 412-445 | 34 | Partial | Partial mapping has strong deterministic components, but several claimed semantics are incomplete at runtime integration/spec-fidelity level. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/partial.py` |
| G36 | 449-460 | 12 | Verified | Reservoir sampling determinism and core invariants are implemented and unit-tested. | None | `src/mcp_artifact_gateway/mapping/partial.py`, `tests/unit/test_partial_mapping_determinism.py` |
| G37 | 464-471 | 8 | Not Implemented | artifact_samples persistence/consistency is specified in SQL/helpers but not wired as an executed transactional flow. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/worker.py`, `src/mcp_artifact_gateway/tools/artifact_select.py` |
| G38 | 476-481 | 6 | Partial | Conditional-update SQL/safety helpers exist, but full worker orchestration and discard/write paths are incomplete. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/worker.py` |
| G39 | 519-532 | 14 | Not Implemented | The claimed where grammar/parser and exact semantics are not fully implemented; current code is a simpler AST evaluator. | AI-08, AI-11 | `src/mcp_artifact_gateway/query/where_dsl.py` |
| G40 | 538-541 | 4 | Verified | Touch policy helper functions and SQL semantics are implemented and unit-tested. | None | `src/mcp_artifact_gateway/sessions.py`, `tests/unit/test_touch_policy.py` |
| G41 | 542-554 | 13 | Partial | Pruning SQL/helpers exist, but end-to-end job execution and deletion orchestration are not fully implemented in runtime. | AI-09, AI-11 | `src/mcp_artifact_gateway/jobs/soft_delete.py`, `src/mcp_artifact_gateway/jobs/hard_delete.py` |
| G42 | 562-567 | 6 | Partial | Index lifecycle claims exceed current schema/runtime support (for example missing pending/partial lifecycle states in DB constraint). | AI-05, AI-03 | `src/mcp_artifact_gateway/db/migrations/001_init.sql`, `src/mcp_artifact_gateway/tools/artifact_find.py` |
| G43 | 573-588 | 16 | Partial | Logging/metrics definitions exist, but instrumentation in live request/mapping/pruning/cursor paths is not fully wired. | AI-10, AI-11 | `src/mcp_artifact_gateway/obs/logging.py`, `src/mcp_artifact_gateway/obs/metrics.py` |
| G44 | 596-614 | 19 | Verified | Specified unit tests for canonicalization/mapping/cursor deterministic checks exist and pass. | None | `tests/unit/test_rfc8785_vectors.py`, `tests/unit/test_partial_mapping_determinism.py`, `tests/unit/test_cursor_where_mode_stale.py` |
| G45 | 615-619 | 5 | Partial | Session discovery and cleanup correctness are not fully proven by integration-level tests. | AI-11 | `tests/integration/__init__.py` |
| G46 | 680-687 | 8 | Verified | `config/settings.py` exists and implements the claimed configuration surface and precedence behavior. Enhanced by PR #11: now also resolves standard `mcpServers` dict format to internal `UpstreamConfig` at load time. | None | `src/mcp_artifact_gateway/config/settings.py` |
| G47 | 689-696 | 8 | Verified | `constants.py` exposes required workspace/version/reserved-key constants. | None | `src/mcp_artifact_gateway/constants.py` |
| G48 | 700-701 | 2 | Verified | `lifecycle.py` enforces directory creation and writeability checks. | None | `src/mcp_artifact_gateway/lifecycle.py` |
| G49 | 718-721 | 4 | Partial | Migration runner applies/records migrations, but "fails hard if migrations missing" is not enforced as claimed. | AI-05, AI-11 | `src/mcp_artifact_gateway/db/migrate.py` |
| G50 | 725-730 | 6 | Partial | Migration files exist, but "exact per spec" and lifecycle/index completeness claims are not fully met. | AI-05 | `src/mcp_artifact_gateway/db/migrations/001_init.sql`, `src/mcp_artifact_gateway/db/migrations/002_indexes.sql` |
| G51 | 734-735 | 2 | Partial | `db/conn.py` uses psycopg connect helper, not a pooled connection manager as claimed. | AI-05 | `src/mcp_artifact_gateway/db/conn.py` |
| G52 | 737-742 | 6 | Verified | Repository helper modules exist as planned. | None | `src/mcp_artifact_gateway/db/repos/sessions_repo.py`, `src/mcp_artifact_gateway/db/repos/payloads_repo.py` |
| G53 | 755-762 | 8 | Verified | `blob_store.py` implements content-addressing, atomic writes, probes, and stream-open behavior. | None | `src/mcp_artifact_gateway/fs/blob_store.py` |
| G54 | 766-767 | 2 | Verified | `resource_store.py` supports internal/external durability behavior. | None | `src/mcp_artifact_gateway/fs/resource_store.py` |
| G55 | 780-785 | 6 | Verified | RFC 8785 and Decimal-safe JSON loading modules are implemented and tested. | None | `src/mcp_artifact_gateway/canon/rfc8785.py`, `src/mcp_artifact_gateway/canon/decimal_json.py` |
| G56 | 789-791 | 3 | Verified | Hashing helpers (including payload hash helpers) are implemented. | None | `src/mcp_artifact_gateway/util/hashing.py` |
| G57 | 804-810 | 7 | Verified | Envelope model and normalization modules exist and enforce key invariants. | None | `src/mcp_artifact_gateway/envelope/model.py`, `src/mcp_artifact_gateway/envelope/normalize.py` |
| G58 | 814-819 | 6 | Partial | Oversize JSON helper exists but full ingest/runtime semantics remain partially implemented. | AI-02, AI-11 | `src/mcp_artifact_gateway/envelope/oversize.py` |
| G59 | 832-846 | 15 | Verified | Payload preparation/compression/integrity and JSONB modes are implemented. | None | `src/mcp_artifact_gateway/storage/payload_store.py` |
| G60 | 850-850 | 1 | Verified | Envelope reconstruction from canonical bytes is implemented. | None | `src/mcp_artifact_gateway/storage/payload_store.py` |
| G61 | 858-861 | 4 | Not Implemented | `upstream.py` discovery/call path is stubbed and not connected to real upstreams. | AI-04, AI-11 | `src/mcp_artifact_gateway/mcp/upstream.py` |
| G62 | 862-866 | 5 | Verified | Mirroring helpers and reserved arg stripping are implemented at helper level. | None | `src/mcp_artifact_gateway/mcp/mirror.py` |
| G63 | 870-874 | 5 | Partial | Request identity helpers are implemented but persistence/runtime integration is incomplete. | AI-02, AI-06 | `src/mcp_artifact_gateway/request_identity.py` |
| G64 | 878-882 | 5 | Partial | Reuse SQL/helpers exist, but lock/reuse behavior is not wired through executable mirrored calls. | AI-02, AI-06, AI-11 | `src/mcp_artifact_gateway/cache/reuse.py` |
| G65 | 886-893 | 8 | Not Implemented | `artifacts/create.py` provides building blocks, but the full creation sequence is not implemented as a callable runtime pipeline. | AI-02, AI-11 | `src/mcp_artifact_gateway/artifacts/create.py` |
| G66 | 906-911 | 6 | Partial | Mapping runner exists but does not fully satisfy binary-backed trigger/persistence expectations. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/runner.py` |
| G67 | 915-916 | 2 | Partial | Full mapper computes inventories but does not execute full DB write lifecycle itself. | AI-07 | `src/mcp_artifact_gateway/mapping/full.py`, `src/mcp_artifact_gateway/mapping/worker.py` |
| G68 | 920-939 | 20 | Partial | Partial mapper implements many deterministic internals, but several claimed runtime/spec details remain incomplete. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/partial.py` |
| G69 | 943-949 | 7 | Partial | Worker safety SQL/helpers exist, but complete worker execution/write-discard lifecycle is not implemented. | AI-07, AI-11 | `src/mcp_artifact_gateway/mapping/worker.py` |
| G70 | 962-970 | 9 | Partial | JSONPath/select/where-hash helpers exist, but some claimed caps/normalization semantics are not fully enforced as described. | AI-03, AI-08, AI-11 | `src/mcp_artifact_gateway/query/jsonpath.py`, `src/mcp_artifact_gateway/query/select_paths.py` |
| G71 | 972-972 | 1 | Verified | where canonicalization mode exposure in status is implemented. | None | `src/mcp_artifact_gateway/tools/status.py` |
| G72 | 976-979 | 4 | Partial | Deterministic traversal exists, but full retrieval-mode semantics (including sampled-only runtime behavior) are not fully integrated. | AI-03, AI-07 | `src/mcp_artifact_gateway/retrieval/traversal.py` |
| G73 | 991-996 | 6 | Verified | Cursor secrets and HMAC sign/verify format/TTL behavior are implemented and tested. | None | `src/mcp_artifact_gateway/cursor/secrets.py`, `src/mcp_artifact_gateway/cursor/hmac.py` |
| G74 | 1000-1002 | 3 | Verified | Cursor payload fields and where-mode stale check helper are implemented. | None | `src/mcp_artifact_gateway/cursor/payload.py` |
| G75 | 1006-1008 | 3 | Verified | Sample-set hash computation and binding-check helper are implemented. | None | `src/mcp_artifact_gateway/cursor/sample_set_hash.py` |
| G76 | 1021-1030 | 10 | Partial | Gateway tool registration exists, but mirrored upstream tool registration/runtime wiring is incomplete. | AI-01, AI-04, AI-11 | `src/mcp_artifact_gateway/mcp/server.py` |
| G77 | 1034-1051 | 18 | Not Implemented | Tool modules are mostly validators/query builders; end-to-end runtime behavior remains unimplemented. | AI-03, AI-11 | `src/mcp_artifact_gateway/tools/artifact_search.py`, `src/mcp_artifact_gateway/tools/artifact_get.py`, `src/mcp_artifact_gateway/tools/artifact_select.py` |
| G78 | 1055-1056 | 2 | Verified | Bounded retrieval response helper shape is implemented. | None | `src/mcp_artifact_gateway/retrieval/response.py` |
| G79 | 1069-1071 | 3 | Verified | Session and artifact ref upsert helpers are implemented. | None | `src/mcp_artifact_gateway/sessions.py` |
| G80 | 1075-1078 | 4 | Verified | Touch policy helper semantics are implemented and covered by unit tests. | None | `src/mcp_artifact_gateway/sessions.py`, `tests/unit/test_touch_policy.py` |
| G81 | 1090-1091 | 2 | Partial | Soft delete SQL exists, but runnable job orchestration/production wiring is incomplete. | AI-09, AI-11 | `src/mcp_artifact_gateway/jobs/soft_delete.py` |
| G82 | 1095-1099 | 5 | Partial | Hard delete SQL exists, but runnable job orchestration/production wiring is incomplete. | AI-09, AI-11 | `src/mcp_artifact_gateway/jobs/hard_delete.py` |
| G83 | 1103-1104 | 2 | Verified | Filesystem reconciliation helpers exist and are unit-tested. | None | `src/mcp_artifact_gateway/jobs/reconcile_fs.py`, `tests/unit/test_reconcile_fs.py` |
| G84 | 1114-1128 | 15 | Partial | Observability modules exist but are not fully integrated into active request/job code paths. | AI-10, AI-11 | `src/mcp_artifact_gateway/obs/logging.py`, `src/mcp_artifact_gateway/obs/metrics.py` |
| G85 | 1140-1155 | 16 | Verified | Planned unit test files exist and pass; however integration depth is still limited (captured separately). | None | `tests/unit/test_reserved_arg_stripping.py`, `tests/unit/test_payload_canonical_integrity.py` |

## Exhaustive Coverage Index (Every Done Task)
Each completed checklist item below maps to a reviewed group with explicit feedback and action items above.

| Plan Line | Task | Group | Verdict | Action Items |
|---:|---|---|---|---|
| 11 | Phase 2: Config, constants, lifecycle | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 12 | Phase 3: Postgres schema + migrations | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 13 | Phase 4: Filesystem blob store + resource store | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 14 | Phase 5: Canonicalization + hashing + compression | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 15 | Phase 6: Envelope normalization + payload storage | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 16 | Phase 7: Upstream discovery + mirroring + artifact creation | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 17 | Phase 8: Mapping system (full + partial) | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 18 | Phase 9: Query + traversal + retrieval core | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 19 | Phase 10: Cursor signing + binding + staleness | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 20 | Phase 11: MCP tool surface | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 21 | Phase 12: Session tracking + touch policy | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 22 | Phase 13: Pruning + retention + cleanup | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 23 | Phase 14: Observability + metrics | G01 | Partial | AI-01, AI-02, AI-03, AI-11 |
| 48 | A clear module boundary exists (names are illustrative, not mandatory): | G02 | Partial | AI-01, AI-04, AI-05 |
| 49 | `config/` (schema + loader + defaults) | G02 | Partial | AI-01, AI-04, AI-05 |
| 50 | `db/` (psycopg3 pool, migrations, queries) | G02 | Partial | AI-01, AI-04, AI-05 |
| 51 | `fs/` (DATA_DIR layout, atomic writes, blob paths, resource copies) | G02 | Partial | AI-01, AI-04, AI-05 |
| 52 | `canonical/` (RFC 8785 canonicalizer + hashing + compression) | G02 | Partial | AI-01, AI-04, AI-05 |
| 53 | `upstream/` (clients for stdio/http MCP, discovery, schema parsing) → `mcp/upstream.py` | G02 | Partial | AI-01, AI-04, AI-05 |
| 54 | `gateway/` (request handling, reserved arg stripping, reuse logic, artifact creation) → `artifacts/`, `cache/`, `mcp/mirror.py` | G02 | Partial | AI-01, AI-04, AI-05 |
| 55 | `retrieval/` (jsonpath evaluation, select/projection, cursor handling) | G02 | Partial | AI-01, AI-04, AI-05 |
| 56 | `mapping/` (full mapping, partial mapping, worker, sampling) | G02 | Partial | AI-01, AI-04, AI-05 |
| 57 | `prune/` (soft delete, hard delete, blob cleanup, reconciliation) → `jobs/` | G02 | Partial | AI-01, AI-04, AI-05 |
| 58 | `tests/` (unit + integration) | G02 | Partial | AI-01, AI-04, AI-05 |
| 101 | Migrations exist to create exactly the v1.9 tables and constraints: | G03 | Partial | AI-05 |
| 102 | `sessions` | G03 | Partial | AI-05 |
| 103 | `binary_blobs` | G03 | Partial | AI-05 |
| 104 | `payload_blobs` | G03 | Partial | AI-05 |
| 105 | `payload_hash_aliases` | G03 | Partial | AI-05 |
| 106 | `payload_binary_refs` | G03 | Partial | AI-05 |
| 107 | `artifacts` | G03 | Partial | AI-05 |
| 108 | `artifact_refs` | G03 | Partial | AI-05 |
| 109 | `artifact_roots` | G03 | Partial | AI-05 |
| 110 | Addendum C table: `artifact_samples` | G03 | Partial | AI-05 |
| 111 | Every PK, FK, unique constraint, and index in the spec exists. | G03 | Partial | AI-05 |
| 114 | Migrations are idempotent and ordered; a fresh database can be brought to current schema in one command. | G04 | Verified | None |
| 115 | Advisory lock usage for request stampede exists (two 32-bit keys derived from `sha256(request_key)`), with timeout and metrics/logging. | G05 | Partial | AI-06, AI-11 |
| 121 | On startup, gateway ensures these directories exist under `DATA_DIR`: | G06 | Verified | None |
| 122 | `state/` | G06 | Verified | None |
| 123 | `resources/` (if internal copies enabled) | G06 | Verified | None |
| 124 | `blobs/bin/` | G06 | Verified | None |
| 125 | `tmp/` | G06 | Verified | None |
| 126 | `logs/` (if used) | G06 | Verified | None |
| 127 | Binary storage is content-addressed: | G06 | Verified | None |
| 128 | `binary_hash = sha256(raw_bytes).hexdigest()` | G06 | Verified | None |
| 129 | `blob_id = "bin_" + binary_hash[:32]` | G06 | Verified | None |
| 130 | Path = `BIN_DIR / h[0:2] / h[2:4] / binary_hash` | G06 | Verified | None |
| 131 | Atomic write procedure exists and is used: | G06 | Verified | None |
| 132 | temp file in same directory | G06 | Verified | None |
| 133 | fsync temp file | G06 | Verified | None |
| 134 | atomic rename to final path | G06 | Verified | None |
| 135 | Existing blob handling exists: | G06 | Verified | None |
| 136 | verifies size matches expected `byte_count` | G06 | Verified | None |
| 137 | optional probe hashes supported and persisted (`probe_head_hash`, `probe_tail_hash`, `probe_bytes`) | G06 | Verified | None |
| 138 | Resource refs support two durabilities: | G06 | Verified | None |
| 139 | `internal`: copy bytes into `DATA_DIR/resources/...` and require `content_hash` | G06 | Verified | None |
| 140 | `external_ref`: do not copy; `content_hash` optional best effort | G06 | Verified | None |
| 146 | RFC 8785 canonical JSON implementation exists and is used for: | G07 | Partial | AI-02, AI-06, AI-11 |
| 147 | forwarded args canonicalization | G07 | Partial | AI-02, AI-06, AI-11 |
| 148 | upstream tool schema canonicalization | G07 | Partial | AI-02, AI-06, AI-11 |
| 149 | envelope canonicalization | G07 | Partial | AI-02, AI-06, AI-11 |
| 150 | cursor payload canonicalization | G07 | Partial | AI-02, AI-06, AI-11 |
| 151 | record hashing in `artifact_samples` | G07 | Partial | AI-02, AI-06, AI-11 |
| 152 | Numeric parsing rules are enforced: | G07 | Partial | AI-02, AI-06, AI-11 |
| 153 | floats parsed as Decimal (no Python float drift) | G07 | Partial | AI-02, AI-06, AI-11 |
| 154 | NaN/Infinity rejected | G07 | Partial | AI-02, AI-06, AI-11 |
| 155 | canonicalization never sees Python floats | G07 | Partial | AI-02, AI-06, AI-11 |
| 156 | Payload identity is correct: | G07 | Partial | AI-02, AI-06, AI-11 |
| 157 | `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)` | G07 | Partial | AI-02, AI-06, AI-11 |
| 158 | `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling) | G07 | Partial | AI-02, AI-06, AI-11 |
| 159 | Canonical bytes storage works: | G07 | Partial | AI-02, AI-06, AI-11 |
| 160 | `envelope_canonical_encoding` stored (`zstd\|gzip\|none`) | G07 | Partial | AI-02, AI-06, AI-11 |
| 161 | `envelope_canonical_bytes` stored (compressed) | G07 | Partial | AI-02, AI-06, AI-11 |
| 162 | `envelope_canonical_bytes_len` stored (uncompressed length) | G07 | Partial | AI-02, AI-06, AI-11 |
| 163 | Dedupe hash is implemented and explicitly does not define storage identity: | G07 | Partial | AI-02, AI-06, AI-11 |
| 164 | tool-configured JSONPath exclusions apply only to dedupe computation | G07 | Partial | AI-02, AI-06, AI-11 |
| 165 | alias table `payload_hash_aliases` is populated and used only for reuse lookup | G07 | Partial | AI-02, AI-06, AI-11 |
| 169 | RFC 8785 canonical JSON implementation exists and is used for: | G08 | Partial | AI-02, AI-06, AI-11 |
| 170 | forwarded args canonicalization | G08 | Partial | AI-02, AI-06, AI-11 |
| 171 | upstream tool schema canonicalization | G08 | Partial | AI-02, AI-06, AI-11 |
| 172 | envelope canonicalization | G08 | Partial | AI-02, AI-06, AI-11 |
| 173 | cursor payload canonicalization | G08 | Partial | AI-02, AI-06, AI-11 |
| 174 | record hashing in `artifact_samples` | G08 | Partial | AI-02, AI-06, AI-11 |
| 175 | Numeric parsing rules are enforced: | G08 | Partial | AI-02, AI-06, AI-11 |
| 176 | floats parsed as Decimal (no Python float drift) | G08 | Partial | AI-02, AI-06, AI-11 |
| 177 | NaN/Infinity rejected | G08 | Partial | AI-02, AI-06, AI-11 |
| 178 | canonicalization never sees Python floats | G08 | Partial | AI-02, AI-06, AI-11 |
| 179 | Payload identity is correct: | G08 | Partial | AI-02, AI-06, AI-11 |
| 180 | `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)` | G08 | Partial | AI-02, AI-06, AI-11 |
| 181 | `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` integrity rule verified on write (and optionally on read sampling) | G08 | Partial | AI-02, AI-06, AI-11 |
| 182 | Canonical bytes storage works: | G08 | Partial | AI-02, AI-06, AI-11 |
| 183 | `envelope_canonical_encoding` stored (`zstd\|gzip\|none`) | G08 | Partial | AI-02, AI-06, AI-11 |
| 184 | `envelope_canonical_bytes` stored (compressed) | G08 | Partial | AI-02, AI-06, AI-11 |
| 185 | `envelope_canonical_bytes_len` stored (uncompressed length) | G08 | Partial | AI-02, AI-06, AI-11 |
| 186 | Dedupe hash is implemented and explicitly does not define storage identity: | G08 | Partial | AI-02, AI-06, AI-11 |
| 187 | tool-configured JSONPath exclusions apply only to dedupe computation | G08 | Partial | AI-02, AI-06, AI-11 |
| 188 | alias table `payload_hash_aliases` is populated and used only for reuse lookup | G08 | Partial | AI-02, AI-06, AI-11 |
| 194 | Upstream tool discovery at startup: | G09 | Not Implemented | AI-04, AI-01, AI-11 |
| 195 | fetch tool list from each upstream | G09 | Not Implemented | AI-04, AI-01, AI-11 |
| 196 | expose mirrored tools as `{prefix}.{tool}` with identical schema/docs (no injected fields) | G09 | Not Implemented | AI-04, AI-01, AI-11 |
| 197 | Reserved gateway args stripping is exact and tested: | G10 | Verified | None |
| 198 | remove keys equal to `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq` | G10 | Verified | None |
| 199 | remove any key whose name begins with exact prefix `_gateway_` | G10 | Verified | None |
| 200 | remove nothing else (example: `gateway_url` must not be stripped) | G10 | Verified | None |
| 201 | Upstream instance identity exists and excludes secrets: | G11 | Partial | AI-02, AI-04 |
| 202 | `upstream_instance_id = sha256(canonical_semantic_identity_bytes)[:32]` | G11 | Partial | AI-02, AI-04 |
| 203 | includes transport + stable endpoint identity + prefix/name + optional semantic salt | G11 | Partial | AI-02, AI-04 |
| 204 | excludes rotating auth headers, tokens, secret env values, private key paths | G11 | Partial | AI-02, AI-04 |
| 205 | optional `upstream_auth_fingerprint` stored for debugging but excluded from request identity | G11 | Partial | AI-02, AI-04 |
| 206 | `request_key` is computed exactly: | G12 | Partial | AI-02, AI-06 |
| 207 | based on `upstream_instance_id`, prefix, tool_name, canonical args bytes | G12 | Partial | AI-02, AI-06 |
| 208 | canonical args bytes computed after stripping reserved keys and validating against upstream schema | G12 | Partial | AI-02, AI-06 |
| 209 | `request_args_hash` and `request_args_prefix` persisted with caps | G12 | Partial | AI-02, AI-06 |
| 217 | Validate `_gateway_context.session_id` exists, else `INVALID_ARGUMENT`. | G13 | Not Implemented | AI-02, AI-11 |
| 218 | Determine `cache_mode` default `allow`. | G13 | Not Implemented | AI-02, AI-11 |
| 219 | Strip reserved gateway args (exact rules). | G14 | Partial | AI-02, AI-06 |
| 220 | Validate forwarded args against upstream schema. | G14 | Partial | AI-02, AI-06 |
| 221 | Canonicalize forwarded args and compute `request_key`. | G14 | Partial | AI-02, AI-06 |
| 222 | Acquire advisory lock for stampede control (with timeout behavior). | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 223 | Reuse behavior when `cache_mode != fresh`: | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 224 | request_key latest candidate chosen by `created_seq desc` | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 225 | optional dedupe alias reuse constrained to same `(upstream_instance_id, tool)` | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 226 | reuse requires: | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 227 | not deleted, not expired | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 228 | schema hash match if strict reuse enabled | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 229 | response indicates `meta.cache.reused=true` + reason + reused artifact id | G15 | Not Implemented | AI-02, AI-06, AI-11 |
| 231 | Normalize into envelope (always): | G16 | Verified | None |
| 232 | status ok or error, with error shape present on error | G16 | Verified | None |
| 233 | content parts support `json`, `text`, `resource_ref`, `binary_ref` (and alias `image_ref`) | G16 | Verified | None |
| 234 | binary bytes never stored inline, only refs | G16 | Verified | None |
| 235 | Oversized JSON handling at ingest: | G16 | Verified | None |
| 236 | if any JSON part size > `max_json_part_parse_bytes`: | G17 | Partial | AI-02, AI-11 |
| 237 | do not parse into structured value | G17 | Partial | AI-02, AI-11 |
| 238 | store raw bytes as `binary_ref` with JSON mime (and encoding) | G17 | Partial | AI-02, AI-11 |
| 239 | replace JSON content entry with `binary_ref` descriptor | G17 | Partial | AI-02, AI-11 |
| 240 | add warning in `meta.warnings` with original part index + encoding | G17 | Partial | AI-02, AI-11 |
| 241 | Produce canonical envelope bytes, compute payload hashes, compress canonical bytes. | G17 | Partial | AI-02, AI-11 |
| 242 | Insert or upsert `payload_blobs`. | G18 | Not Implemented | AI-02, AI-11 |
| 243 | Insert `binary_blobs` and `payload_binary_refs` for every blob reference. | G18 | Not Implemented | AI-02, AI-11 |
| 244 | Insert optional `payload_hash_aliases` rows (dedupe). | G18 | Not Implemented | AI-02, AI-11 |
| 245 | Insert `artifacts` row with: | G18 | Not Implemented | AI-02, AI-11 |
| 246 | monotonic `created_seq` | G18 | Not Implemented | AI-02, AI-11 |
| 247 | mapping fields: `map_kind='none'`, `map_status='pending'` initially, `mapper_version` set | G18 | Not Implemented | AI-02, AI-11 |
| 248 | `index_status='off'` unless enabled | G18 | Not Implemented | AI-02, AI-11 |
| 249 | sizes persisted (`payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes`) | G18 | Not Implemented | AI-02, AI-11 |
| 250 | `last_referenced_at=now()` | G18 | Not Implemented | AI-02, AI-11 |
| 251 | Update session tracking: | G18 | Not Implemented | AI-02, AI-11 |
| 252 | `sessions` upsert with `last_seen_at=now()` | G18 | Not Implemented | AI-02, AI-11 |
| 253 | `artifact_refs` upsert for `(session_id, artifact_id)` | G18 | Not Implemented | AI-02, AI-11 |
| 254 | Return contract (Addendum A): | G19 | Partial | AI-02, AI-03 |
| 255 | returns a handle-only result by default | G19 | Partial | AI-02, AI-03 |
| 256 | returns handle+inline envelope only when thresholds satisfied and policy allows | G19 | Partial | AI-02, AI-03 |
| 263 | All tool responses follow one of: | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 264 | `gateway_tool_result` for success returns (mirrored tools) | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 265 | uniform `gateway_error` for failures (all tools) | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 266 | Handle includes required metadata: | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 267 | ids, created_seq, session_id, tool ids, hash ids, byte sizes, mapping/index status, contains_binary_refs, status | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 268 | Warnings propagation works: | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 269 | warnings in response include gateway warnings inserted into envelope meta | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 270 | Error codes are implemented and used correctly: | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 271 | `INVALID_ARGUMENT`, `NOT_FOUND`, `GONE`, `INTERNAL`, `CURSOR_INVALID`, `CURSOR_EXPIRED`, `CURSOR_STALE`, `BUDGET_EXCEEDED`, `UNSUPPORTED` | G20 | Not Implemented | AI-02, AI-03, AI-11 |
| 279 | `gateway.status()` returns: | G21 | Not Implemented | AI-03, AI-04, AI-11 |
| 280 | upstream connectivity snapshot | G21 | Not Implemented | AI-03, AI-04, AI-11 |
| 281 | DB ok / migrations ok | G21 | Not Implemented | AI-03, AI-04, AI-11 |
| 282 | filesystem paths ok | G21 | Not Implemented | AI-03, AI-04, AI-11 |
| 283 | version constants: canonicalizer, mapper, traversal contract, cursor version | G22 | Partial | AI-03, AI-04 |
| 284 | where canonicalization mode | G22 | Partial | AI-03, AI-04 |
| 285 | partial mapping backend id + prng version | G22 | Partial | AI-03, AI-04 |
| 286 | all configured limits/budgets | G22 | Partial | AI-03, AI-04 |
| 287 | cursor TTL and active secret versions | G22 | Partial | AI-03, AI-04 |
| 291 | Requires `_gateway_context.session_id`. | G23 | Partial | AI-03, AI-11 |
| 292 | Reads only from `artifact_refs` for that session (discovery uses refs exclusively). | G23 | Partial | AI-03, AI-11 |
| 293 | Filters implemented (Addendum B), including: | G23 | Partial | AI-03, AI-11 |
| 294 | include_deleted, status, source_tool_prefix, source_tool, upstream_instance_id, request_key, payload_hash_full, parent_artifact_id, has_binary_refs, created_seq range, created_at range | G23 | Partial | AI-03, AI-11 |
| 295 | Ordering implemented: | G23 | Partial | AI-03, AI-11 |
| 296 | `created_seq_desc` default | G23 | Partial | AI-03, AI-11 |
| 297 | `last_seen_desc` optional | G23 | Partial | AI-03, AI-11 |
| 298 | Search touches only: | G24 | Not Implemented | AI-03, AI-11 |
| 299 | `sessions.last_seen_at` | G24 | Not Implemented | AI-03, AI-11 |
| 300 | `artifact_refs.last_seen_at` | G24 | Not Implemented | AI-03, AI-11 |
| 301 | does not touch `artifacts.last_referenced_at` | G24 | Not Implemented | AI-03, AI-11 |
| 302 | Pagination cursor for search exists and is bound to session_id + order_by + last position. | G24 | Not Implemented | AI-03, AI-11 |
| 321 | Requires session_id. | G25 | Not Implemented | AI-03, AI-11 |
| 322 | Supports: | G25 | Not Implemented | AI-03, AI-11 |
| 323 | `target=envelope` (jsonpath evaluated on envelope root) | G25 | Not Implemented | AI-03, AI-11 |
| 324 | `target=mapped` (requires map_status ready and map_kind full/partial) | G25 | Not Implemented | AI-03, AI-11 |
| 325 | If envelope jsonb is minimized or none: | G25 | Not Implemented | AI-03, AI-11 |
| 326 | reconstruct by parsing canonical bytes, within compute budgets | G25 | Not Implemented | AI-03, AI-11 |
| 327 | Bounded deterministic output: | G25 | Not Implemented | AI-03, AI-11 |
| 328 | max_bytes_out / max_items / max_compute enforced | G25 | Not Implemented | AI-03, AI-11 |
| 329 | deterministic truncation emits `truncated=true` + cursor + omitted metadata | G25 | Not Implemented | AI-03, AI-11 |
| 330 | Touch semantics: | G25 | Not Implemented | AI-03, AI-11 |
| 331 | if not deleted: touches `artifacts.last_referenced_at` | G25 | Not Implemented | AI-03, AI-11 |
| 332 | always updates `artifact_refs.last_seen_at` and `sessions.last_seen_at` | G25 | Not Implemented | AI-03, AI-11 |
| 333 | if deleted: returns `GONE` | G25 | Not Implemented | AI-03, AI-11 |
| 337 | Returns: | G26 | Not Implemented | AI-03, AI-11 |
| 338 | mapping status/kind + mapper_version | G26 | Not Implemented | AI-03, AI-11 |
| 339 | roots inventory + fields_top | G26 | Not Implemented | AI-03, AI-11 |
| 340 | partial mapping fields: sampled-only, prefix coverage indicator, stop_reason, sampled_prefix_len, sampled_record_count, skipped_oversize_records | G26 | Not Implemented | AI-03, AI-11 |
| 341 | count_estimate only when known under the stated rules | G26 | Not Implemented | AI-03, AI-11 |
| 342 | Touch semantics same as retrieval. | G26 | Not Implemented | AI-03, AI-11 |
| 346 | Inputs: | G27 | Partial | AI-03, AI-08, AI-11 |
| 347 | artifact_id, root_path, select_paths (set semantics), where optional, limits, cursor | G27 | Partial | AI-03, AI-08, AI-11 |
| 348 | select_paths canonicalization implemented: | G27 | Partial | AI-03, AI-08, AI-11 |
| 349 | whitespace removal, canonical escaping/quotes | G27 | Partial | AI-03, AI-08, AI-11 |
| 350 | relative paths must not start with `$` | G27 | Partial | AI-03, AI-08, AI-11 |
| 351 | sorted lexicographically, duplicates removed | G27 | Partial | AI-03, AI-08, AI-11 |
| 352 | `select_paths_hash = sha256(canonical_json(array))` | G27 | Partial | AI-03, AI-08, AI-11 |
| 353 | where hashing implemented per server mode: | G27 | Partial | AI-03, AI-08, AI-11 |
| 354 | raw_string default: exact UTF-8 bytes | G27 | Partial | AI-03, AI-08, AI-11 |
| 356 | server reports mode in `gateway.status`, cursor binds to it | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 357 | Full mapping behavior: | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 358 | bounded scan in deterministic order with cursor continuation | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 359 | Partial mapping behavior: | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 360 | sampled-only scan: | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 361 | enumerate sample indices ascending | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 362 | evaluate where and select_paths only on sampled records | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 363 | returns `sampled_only=true`, `sample_indices_used`, `sampled_prefix_len` | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 364 | Output projection contract (Addendum F): | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 365 | each item has `_locator` and `projection` | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 366 | projection keys are canonicalized select paths, emitted in lex order | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 367 | missing path behavior respects config `select_missing_as_null` | G28 | Not Implemented | AI-03, AI-07, AI-11 |
| 371 | Works in sample-only mode unless indexing is enabled. | G29 | Not Implemented | AI-03, AI-11 |
| 372 | Deterministic output and bounded truncation with cursor. | G29 | Not Implemented | AI-03, AI-11 |
| 376 | Chain ordering is correct: | G30 | Partial | AI-03, AI-11 |
| 377 | `chain_seq asc`, then `created_seq asc` | G30 | Partial | AI-03, AI-11 |
| 378 | Chain seq allocation exists when not provided, with retry and uniqueness constraint. | G30 | Partial | AI-03, AI-11 |
| 386 | mapping_mode implemented: `async\|hybrid\|sync` (default hybrid) | G31 | Partial | AI-07, AI-11 |
| 387 | Artifacts created with map_status pending cause mapping work to be scheduled. | G31 | Partial | AI-07, AI-11 |
| 391 | Deterministic scoring implemented; tie-break by part index ascending. | G32 | Partial | AI-07 |
| 392 | Stores `mapped_part_index` on artifact. | G32 | Partial | AI-07 |
| 398 | parse fully | G33 | Partial | AI-07, AI-11 |
| 399 | discover up to K roots (K=3) | G33 | Partial | AI-07, AI-11 |
| 400 | build deterministic inventory: | G33 | Partial | AI-07, AI-11 |
| 401 | roots entries written to `artifact_roots` | G33 | Partial | AI-07, AI-11 |
| 402 | `map_kind=full`, `map_status=ready` | G33 | Partial | AI-07, AI-11 |
| 406 | If JSON part too large OR stored as `binary_ref application/json(+encoding)`: | G34 | Partial | AI-07, AI-11 |
| 407 | partial mapping runs | G34 | Partial | AI-07, AI-11 |
| 408 | `map_kind=partial` | G34 | Partial | AI-07, AI-11 |
| 412 | Byte-backed streaming input supported: | G35 | Partial | AI-07, AI-11 |
| 413 | from JSON binary blob (required when oversized at ingest) | G35 | Partial | AI-07, AI-11 |
| 414 | from text JSON (bounded) | G35 | Partial | AI-07, AI-11 |
| 415 | from re-canonicalized bytes for small structured values (bounded) | G35 | Partial | AI-07, AI-11 |
| 416 | Budgets enforced during streaming: | G35 | Partial | AI-07, AI-11 |
| 417 | max bytes read | G35 | Partial | AI-07, AI-11 |
| 418 | max compute steps (stream events) | G35 | Partial | AI-07, AI-11 |
| 419 | max depth | G35 | Partial | AI-07, AI-11 |
| 420 | max sampled records N | G35 | Partial | AI-07, AI-11 |
| 421 | max per-record bytes | G35 | Partial | AI-07, AI-11 |
| 422 | max leaf paths | G35 | Partial | AI-07, AI-11 |
| 423 | root discovery depth cap | G35 | Partial | AI-07, AI-11 |
| 424 | stop_reason tracked: | G35 | Partial | AI-07, AI-11 |
| 425 | none \| max_bytes \| max_compute \| max_depth \| parse_error | G35 | Partial | AI-07, AI-11 |
| 426 | Prefix coverage semantics enforced: | G35 | Partial | AI-07, AI-11 |
| 427 | if stop_reason != none: | G35 | Partial | AI-07, AI-11 |
| 428 | count_estimate is null | G35 | Partial | AI-07, AI-11 |
| 429 | root_shape.prefix_coverage=true | G35 | Partial | AI-07, AI-11 |
| 430 | inventory coverage computed vs prefix | G35 | Partial | AI-07, AI-11 |
| 431 | map_backend_id and prng_version: | G35 | Partial | AI-07, AI-11 |
| 432 | map_backend_id computed exactly from python version + ijson backend name + version | G35 | Partial | AI-07, AI-11 |
| 433 | prng_version is a code constant | G35 | Partial | AI-07, AI-11 |
| 434 | both returned by status and stored on artifacts | G35 | Partial | AI-07, AI-11 |
| 435 | map_budget_fingerprint computed and stored: | G35 | Partial | AI-07, AI-11 |
| 436 | includes mapper version, traversal contract, backend id, prng version, all budgets | G35 | Partial | AI-07, AI-11 |
| 437 | if changes, previous partial mapping marked stale and cursors become stale | G35 | Partial | AI-07, AI-11 |
| 438 | root_path normalization: | G35 | Partial | AI-07, AI-11 |
| 439 | absolute path starting with `$` | G35 | Partial | AI-07, AI-11 |
| 440 | uses `.name` when identifier is valid; otherwise bracket form with canonical escaping | G35 | Partial | AI-07, AI-11 |
| 441 | no wildcards | G35 | Partial | AI-07, AI-11 |
| 442 | format change requires traversal_contract_version bump (enforced as policy) | G35 | Partial | AI-07, AI-11 |
| 443 | streaming skip contract implemented: | G35 | Partial | AI-07, AI-11 |
| 444 | ability to skip unselected subtrees without building full trees | G35 | Partial | AI-07, AI-11 |
| 445 | compute steps count all events processed, including skipped | G35 | Partial | AI-07, AI-11 |
| 449 | Reservoir sampling is one-pass and prefix-bounded: | G36 | Verified | None |
| 450 | seed = sha256(payload_hash_full + "\|" + root_path + "\|" + map_budget_fingerprint) | G36 | Verified | None |
| 451 | PRNG deterministic and versioned | G36 | Verified | None |
| 452 | selected indices maintained uniformly over processed prefix indices | G36 | Verified | None |
| 453 | Bias invariant is explicit and implemented: | G36 | Verified | None |
| 454 | oversize/depth-violating records are skipped and counted | G36 | Verified | None |
| 455 | sample_indices include only successfully materialized records | G36 | Verified | None |
| 456 | sampled_prefix_len is computed correctly: | G36 | Verified | None |
| 457 | counts element boundaries successfully recognized, including skipped/non-materialized | G36 | Verified | None |
| 458 | parse_error mid-element uses last fully recognized index + 1 | G36 | Verified | None |
| 459 | count_estimate rules enforced: | G36 | Verified | None |
| 460 | set only if stop_reason==none AND array close observed | G36 | Verified | None |
| 464 | `artifact_samples` table is used for partial samples: | G37 | Not Implemented | AI-07, AI-11 |
| 465 | one row per materialized sampled record index | G37 | Not Implemented | AI-07, AI-11 |
| 466 | record hash stored as sha256(RFC8785(record)) | G37 | Not Implemented | AI-07, AI-11 |
| 467 | `artifact_roots.sample_indices` exactly matches sample indices present in `artifact_samples` (sorted). | G37 | Not Implemented | AI-07, AI-11 |
| 468 | Updates are atomic: | G37 | Not Implemented | AI-07, AI-11 |
| 469 | replace sample rows + sample_indices within a transaction per `(artifact_id, root_key)` | G37 | Not Implemented | AI-07, AI-11 |
| 470 | Partial retrieval depends on artifact_samples: | G37 | Not Implemented | AI-07, AI-11 |
| 471 | `artifact.select` loads records from artifact_samples | G37 | Not Implemented | AI-07, AI-11 |
| 476 | Worker writes are conditional: | G38 | Partial | AI-07, AI-11 |
| 477 | artifact not deleted | G38 | Partial | AI-07, AI-11 |
| 478 | map_status in (pending, stale) | G38 | Partial | AI-07, AI-11 |
| 479 | generation matches | G38 | Partial | AI-07, AI-11 |
| 480 | If conditional update affects 0 rows, worker discards results. | G38 | Partial | AI-07, AI-11 |
| 481 | map_error stored on failure with enough detail to debug. | G38 | Partial | AI-07, AI-11 |
| 519 | Parser exists for the specified grammar (OR/AND/NOT, parentheses, comparisons). | G39 | Not Implemented | AI-08, AI-11 |
| 520 | Relative path evaluation uses JSONPath subset (must not start with `$`). | G39 | Not Implemented | AI-08, AI-11 |
| 521 | Missing path semantics implemented: | G39 | Not Implemented | AI-08, AI-11 |
| 522 | comparisons false except special `!= null` semantics (as defined) | G39 | Not Implemented | AI-08, AI-11 |
| 523 | Wildcard semantics: | G39 | Not Implemented | AI-08, AI-11 |
| 524 | existential: any match satisfies | G39 | Not Implemented | AI-08, AI-11 |
| 525 | bounded by max wildcard expansion | G39 | Not Implemented | AI-08, AI-11 |
| 526 | Type semantics implemented exactly: | G39 | Not Implemented | AI-08, AI-11 |
| 527 | numeric comparisons require numeric operands | G39 | Not Implemented | AI-08, AI-11 |
| 528 | string comparisons lexicographic by codepoint | G39 | Not Implemented | AI-08, AI-11 |
| 529 | boolean only supports = and != | G39 | Not Implemented | AI-08, AI-11 |
| 530 | Compute accounting exists and is deterministic: | G39 | Not Implemented | AI-08, AI-11 |
| 531 | increments per path segment and expansions and comparison op | G39 | Not Implemented | AI-08, AI-11 |
| 532 | deterministic short-circuiting | G39 | Not Implemented | AI-08, AI-11 |
| 538 | Touch policy implemented exactly: | G40 | Verified | None |
| 539 | creation touches `artifacts.last_referenced_at` | G40 | Verified | None |
| 540 | retrieval/describe touches if not deleted | G40 | Verified | None |
| 541 | search does not touch last_referenced_at | G40 | Verified | None |
| 542 | Soft delete job exists: | G41 | Partial | AI-09, AI-11 |
| 543 | selects with SKIP LOCKED | G41 | Partial | AI-09, AI-11 |
| 544 | predicate rechecked on update | G41 | Partial | AI-09, AI-11 |
| 545 | sets deleted_at and increments generation | G41 | Partial | AI-09, AI-11 |
| 546 | does not remove payloads yet | G41 | Partial | AI-09, AI-11 |
| 547 | Hard delete job exists: | G41 | Partial | AI-09, AI-11 |
| 548 | deletes eligible artifacts | G41 | Partial | AI-09, AI-11 |
| 549 | cascades remove `artifact_roots`, `artifact_refs`, `artifact_samples` | G41 | Partial | AI-09, AI-11 |
| 550 | deletes unreferenced `payload_blobs` | G41 | Partial | AI-09, AI-11 |
| 551 | cascades remove `payload_binary_refs` | G41 | Partial | AI-09, AI-11 |
| 552 | deletes `binary_blobs` unreferenced by payload_binary_refs | G41 | Partial | AI-09, AI-11 |
| 553 | removes corresponding filesystem blob files | G41 | Partial | AI-09, AI-11 |
| 554 | optional reconciliation: detects orphan files on disk and can report/remove | G41 | Partial | AI-09, AI-11 |
| 562 | Code supports `index_status` lifecycle: | G42 | Partial | AI-05, AI-03 |
| 563 | off \| pending \| ready \| partial \| failed | G42 | Partial | AI-05, AI-03 |
| 564 | `artifact.find` respects sample-only unless index enabled rule. | G42 | Partial | AI-05, AI-03 |
| 565 | If indexing is truly out of project scope for now, code still must: | G42 | Partial | AI-05, AI-03 |
| 566 | store `index_status` fields | G42 | Partial | AI-05, AI-03 |
| 567 | return consistent behavior when off | G42 | Partial | AI-05, AI-03 |
| 573 | Structured logging exists (structlog or equivalent) for: | G43 | Partial | AI-10, AI-11 |
| 574 | startup discovery per upstream | G43 | Partial | AI-10, AI-11 |
| 575 | request_key computation (hashes only, no secrets) | G43 | Partial | AI-10, AI-11 |
| 576 | reuse decision: hit/miss and why | G43 | Partial | AI-10, AI-11 |
| 577 | artifact creation path including: | G43 | Partial | AI-10, AI-11 |
| 578 | envelope sizes | G43 | Partial | AI-10, AI-11 |
| 579 | oversized JSON offload events | G43 | Partial | AI-10, AI-11 |
| 580 | binary blob writes and dedupe hits | G43 | Partial | AI-10, AI-11 |
| 581 | mapping runs (full/partial), budgets, stop_reason, counts | G43 | Partial | AI-10, AI-11 |
| 582 | cursor validation failures categorized (invalid/expired/stale) | G43 | Partial | AI-10, AI-11 |
| 583 | pruning operations and bytes reclaimed | G43 | Partial | AI-10, AI-11 |
| 584 | Metrics counters exist (can be simple internal counters): | G43 | Partial | AI-10, AI-11 |
| 585 | advisory lock timeouts | G43 | Partial | AI-10, AI-11 |
| 586 | upstream call latency and error types | G43 | Partial | AI-10, AI-11 |
| 587 | mapping latency and stop reasons | G43 | Partial | AI-10, AI-11 |
| 588 | prune deletions and disk bytes reclaimed | G43 | Partial | AI-10, AI-11 |
| 596 | RFC 8785 canonicalization vectors + numeric edge cases. | G44 | Verified | None |
| 597 | Compression roundtrip integrity: compressed canonical bytes decompress to same bytes and hash matches. | G44 | Verified | None |
| 598 | Reserved arg stripping removes only `_gateway_*` keys and explicit reserved names. | G44 | Verified | None |
| 599 | Oversized JSON ingest becomes byte-backed binary_ref and is used for streaming mapping. | G44 | Verified | None |
| 600 | Partial mapping determinism: | G44 | Verified | None |
| 601 | same payload + same budgets => same sample_indices + same root inventory | G44 | Verified | None |
| 602 | map_budget_fingerprint mismatch => stale behavior | G44 | Verified | None |
| 603 | Prefix coverage semantics: | G44 | Verified | None |
| 604 | stop_reason != none => count_estimate null, prefix_coverage true, sampled_prefix_len correct | G44 | Verified | None |
| 605 | Sampling bias invariant: | G44 | Verified | None |
| 606 | oversize records skipped and counted; sample_indices exclude them | G44 | Verified | None |
| 607 | Cursor determinism: | G44 | Verified | None |
| 608 | same request and position => same cursor payload (before HMAC) and valid verification | G44 | Verified | None |
| 609 | CURSOR_STALE conditions: | G44 | Verified | None |
| 610 | sample_set mismatch | G44 | Verified | None |
| 611 | where_canonicalization_mode mismatch | G44 | Verified | None |
| 612 | traversal_contract_version mismatch | G44 | Verified | None |
| 613 | artifact_generation mismatch | G44 | Verified | None |
| 614 | Session discovery correctness: | G44 | Verified | None |
| 615 | artifact.search only returns artifacts in artifact_refs for that session | G45 | Partial | AI-11 |
| 616 | new artifact appears immediately | G45 | Partial | AI-11 |
| 617 | Cleanup correctness: | G45 | Partial | AI-11 |
| 618 | payload_binary_refs prevents orphaning | G45 | Partial | AI-11 |
| 619 | hard delete removes filesystem blobs only when unreferenced | G45 | Partial | AI-11 |
| 680 | `src/mcp_artifact_gateway/config/settings.py` | G46 | Verified | None |
| 681 | Loads config from (in precedence): env vars -> `DATA_DIR/state/config.json` -> defaults | G46 | Verified | None |
| 682 | Validates all caps/budgets exist (retrieval, mapping, JSON oversize caps, storage caps) | G46 | Verified | None |
| 683 | Exposes: | G46 | Verified | None |
| 684 | `DATA_DIR` and derived paths (`tmp/`, `logs/`, `blobs/`, `resources/`, `state/`) | G46 | Verified | None |
| 685 | `envelope_jsonb_mode`, `envelope_jsonb_minimize_threshold_bytes` | G46 | Verified | None |
| 686 | `max_json_part_parse_bytes` (oversized JSON becomes byte-backed binary ref) | G46 | Verified | None |
| 687 | partial-map budgets (the full set used in `map_budget_fingerprint`) | G46 | Verified | None |
| 689 | `src/mcp_artifact_gateway/constants.py` | G47 | Verified | None |
| 690 | `WORKSPACE_ID = "local"` | G47 | Verified | None |
| 691 | `traversal_contract_version` constant | G47 | Verified | None |
| 692 | `canonicalizer_version` constant | G47 | Verified | None |
| 693 | `mapper_version` constant | G47 | Verified | None |
| 694 | `prng_version` constant | G47 | Verified | None |
| 695 | `cursor_version` constant | G47 | Verified | None |
| 696 | Reserved key prefix: `_gateway_` and explicit reserved names | G47 | Verified | None |
| 700 | `src/mcp_artifact_gateway/lifecycle.py` | G48 | Verified | None |
| 701 | Ensures directories exist, permissions ok, temp dir writable | G48 | Verified | None |
| 718 | `src/mcp_artifact_gateway/db/migrate.py` | G49 | Partial | AI-05, AI-11 |
| 719 | Applies SQL migrations in order | G49 | Partial | AI-05, AI-11 |
| 720 | Records applied migrations (table `schema_migrations`) | G49 | Partial | AI-05, AI-11 |
| 721 | Fails hard if migrations missing | G49 | Partial | AI-05, AI-11 |
| 725 | `src/mcp_artifact_gateway/db/migrations/001_init.sql` | G50 | Partial | AI-05 |
| 726 | Creates tables exactly per spec: `sessions`, `binary_blobs`, `payload_blobs`, `payload_hash_aliases`, `payload_binary_refs`, `artifacts`, `artifact_refs`, `artifact_roots` | G50 | Partial | AI-05 |
| 727 | All PKs include `workspace_id` | G50 | Partial | AI-05 |
| 728 | All constraints and indexes exist (especially `created_seq` identity and ordering indexes) | G50 | Partial | AI-05 |
| 729 | `src/mcp_artifact_gateway/db/migrations/002_indexes.sql` (optional if you split) | G50 | Partial | AI-05 |
| 730 | Adds the heavier indexes (request_key, created_seq, last_seen) | G50 | Partial | AI-05 |
| 734 | `src/mcp_artifact_gateway/db/conn.py` | G51 | Partial | AI-05 |
| 735 | psycopg3 connection pool | G51 | Partial | AI-05 |
| 737 | `src/mcp_artifact_gateway/db/repos/*.py` (split by concern) | G52 | Verified | None |
| 738 | `sessions_repo.py` | G52 | Verified | None |
| 739 | `payloads_repo.py` | G52 | Verified | None |
| 740 | `artifacts_repo.py` | G52 | Verified | None |
| 741 | `mapping_repo.py` | G52 | Verified | None |
| 742 | `prune_repo.py` | G52 | Verified | None |
| 755 | `src/mcp_artifact_gateway/fs/blob_store.py` | G53 | Verified | None |
| 756 | `put_bytes(raw_bytes, mime) -> BinaryRef`: | G53 | Verified | None |
| 757 | `binary_hash = sha256(raw_bytes)` | G53 | Verified | None |
| 758 | path = `DATA_DIR/blobs/bin/ab/cd/<binary_hash>` | G53 | Verified | None |
| 759 | atomic write: temp in same dir -> fsync -> rename | G53 | Verified | None |
| 760 | if exists: verify size, optional probe head/tail hashes | G53 | Verified | None |
| 761 | `open_stream(binary_hash) -> IO[bytes]` for partial mapping byte-backed reads | G53 | Verified | None |
| 762 | MIME normalization: lowercase, strip params, alias map | G53 | Verified | None |
| 766 | `src/mcp_artifact_gateway/fs/resource_store.py` | G54 | Verified | None |
| 767 | Supports `resource_ref` durability rules (`internal` copies under `DATA_DIR/resources`) | G54 | Verified | None |
| 780 | `src/mcp_artifact_gateway/canon/rfc8785.py` | G55 | Verified | None |
| 781 | `canonical_bytes(obj) -> bytes` implementing RFC 8785 | G55 | Verified | None |
| 782 | Deterministic key ordering, UTF-8, number formatting | G55 | Verified | None |
| 783 | `src/mcp_artifact_gateway/canon/decimal_json.py` | G55 | Verified | None |
| 784 | JSON loader that parses floats as `Decimal`, rejects NaN/Infinity | G55 | Verified | None |
| 785 | Ensures canonicalization never sees Python float | G55 | Verified | None |
| 789 | `src/mcp_artifact_gateway/util/hashing.py` | G56 | Verified | None |
| 790 | `sha256_hex(bytes)`, `sha256_trunc(bytes, n)` | G56 | Verified | None |
| 791 | `payload_hash_full = sha256(envelope_canonical_bytes_uncompressed)` | G56 | Verified | None |
| 804 | `src/mcp_artifact_gateway/envelope/model.py` | G57 | Verified | None |
| 805 | Typed dataclasses or pydantic models for: | G57 | Verified | None |
| 806 | `Envelope`, `ContentPartJson`, `ContentPartText`, `ContentPartResourceRef`, `ContentPartBinaryRef`, `ErrorBlock` | G57 | Verified | None |
| 807 | `src/mcp_artifact_gateway/envelope/normalize.py` | G57 | Verified | None |
| 808 | Converts upstream MCP response into canonical envelope shape | G57 | Verified | None |
| 809 | Ensures: ok implies no error, error implies error present | G57 | Verified | None |
| 810 | Never stores raw binary bytes in envelope | G57 | Verified | None |
| 814 | `src/mcp_artifact_gateway/envelope/oversize.py` | G58 | Partial | AI-02, AI-11 |
| 815 | If any JSON part exceeds `max_json_part_parse_bytes`: | G58 | Partial | AI-02, AI-11 |
| 816 | do not parse | G58 | Partial | AI-02, AI-11 |
| 817 | store raw bytes as `binary_ref` with `mime = application/json` (optionally `+encoding`) | G58 | Partial | AI-02, AI-11 |
| 818 | replace that part with a `binary_ref` descriptor | G58 | Partial | AI-02, AI-11 |
| 819 | add a warning in `meta.warnings` with original part index and encoding | G58 | Partial | AI-02, AI-11 |
| 832 | `src/mcp_artifact_gateway/storage/payload_store.py` | G59 | Verified | None |
| 833 | `compress(bytes) -> (encoding, compressed, uncompressed_len)` | G59 | Verified | None |
| 834 | Supports `zstd\|gzip\|none` | G59 | Verified | None |
| 835 | Writes `payload_blobs` row with: | G59 | Verified | None |
| 836 | `envelope_canonical_bytes` compressed | G59 | Verified | None |
| 837 | `envelope_canonical_bytes_len` | G59 | Verified | None |
| 838 | `payload_json_bytes`, `payload_binary_bytes_total`, `payload_total_bytes` | G59 | Verified | None |
| 839 | `contains_binary_refs` | G59 | Verified | None |
| 840 | `canonicalizer_version` | G59 | Verified | None |
| 841 | Enforces integrity: | G59 | Verified | None |
| 842 | `payload_hash_full == sha256(uncompressed(envelope_canonical_bytes))` | G59 | Verified | None |
| 843 | JSONB storage mode implemented: | G59 | Verified | None |
| 844 | `full` | G59 | Verified | None |
| 845 | `minimal_for_large` projection | G59 | Verified | None |
| 846 | `none` projection | G59 | Verified | None |
| 850 | Payload retrieval can reconstruct envelope from canonical bytes even if jsonb is minimal/none | G60 | Verified | None |
| 858 | `src/mcp_artifact_gateway/mcp/upstream.py` | G61 | Not Implemented | AI-04, AI-11 |
| 859 | Connects to each upstream MCP (stdio/http) | G61 | Not Implemented | AI-04, AI-11 |
| 860 | Fetches tool list at startup | G61 | Not Implemented | AI-04, AI-11 |
| 861 | `src/mcp_artifact_gateway/mcp/mirror.py` | G61 | Not Implemented | AI-04, AI-11 |
| 862 | Exposes mirrored tools as `{prefix}.{tool}` with identical schema/docs, no injected fields | G62 | Verified | None |
| 863 | Strips reserved keys before schema validation and forwarding: | G62 | Verified | None |
| 864 | exact keys: `_gateway_context`, `_gateway_parent_artifact_id`, `_gateway_chain_seq` | G62 | Verified | None |
| 865 | any key starting with `_gateway_` | G62 | Verified | None |
| 866 | nothing else | G62 | Verified | None |
| 870 | `src/mcp_artifact_gateway/request_identity.py` | G63 | Partial | AI-02, AI-06 |
| 871 | Computes `upstream_instance_id` (semantic identity excluding secrets) | G63 | Partial | AI-02, AI-06 |
| 872 | Computes `canonical_args_bytes` via RFC 8785 after reserved stripping and schema validation | G63 | Partial | AI-02, AI-06 |
| 873 | `request_key = sha256(upstream_instance_id\|prefix\|tool\|canonical_args_bytes)` | G63 | Partial | AI-02, AI-06 |
| 874 | Persists `request_args_hash` and capped `request_args_prefix` | G63 | Partial | AI-02, AI-06 |
| 878 | `src/mcp_artifact_gateway/cache/reuse.py` | G64 | Partial | AI-02, AI-06, AI-11 |
| 879 | Advisory lock: derive two 32-bit keys from `sha256(request_key)` and `pg_advisory_lock` with timeout | G64 | Partial | AI-02, AI-06, AI-11 |
| 880 | If `cache_mode != fresh`, tries reuse by `request_key` latest (`created_seq desc`) | G64 | Partial | AI-02, AI-06, AI-11 |
| 881 | Strict gating by schema hash unless configured otherwise | G64 | Partial | AI-02, AI-06, AI-11 |
| 882 | Optional dedupe alias reuse (`payload_hash_aliases`) constrained to same upstream_instance_id + tool | G64 | Partial | AI-02, AI-06, AI-11 |
| 886 | `src/mcp_artifact_gateway/artifacts/create.py` | G65 | Not Implemented | AI-02, AI-11 |
| 887 | Implements the full step sequence in Section 9.1 | G65 | Not Implemented | AI-02, AI-11 |
| 888 | Always stores an artifact even on upstream error/timeout (error envelope) | G65 | Not Implemented | AI-02, AI-11 |
| 889 | Inserts: | G65 | Not Implemented | AI-02, AI-11 |
| 890 | payload blob row | G65 | Not Implemented | AI-02, AI-11 |
| 891 | payload_binary_refs rows | G65 | Not Implemented | AI-02, AI-11 |
| 892 | artifact row with `map_status=pending`, `map_kind=none` initially | G65 | Not Implemented | AI-02, AI-11 |
| 893 | artifact_refs row and session last_seen update | G65 | Not Implemented | AI-02, AI-11 |
| 906 | `src/mcp_artifact_gateway/mapping/runner.py` | G66 | Partial | AI-07, AI-11 |
| 907 | Picks JSON part to map deterministically with tie-break by part index | G66 | Partial | AI-07, AI-11 |
| 908 | Decides full vs partial: | G66 | Partial | AI-07, AI-11 |
| 909 | full if size <= `max_full_map_bytes` | G66 | Partial | AI-07, AI-11 |
| 910 | partial if too large or stored as `binary_ref application/json(+encoding)` | G66 | Partial | AI-07, AI-11 |
| 911 | Stores results in `artifact_roots`, updates artifact mapping columns | G66 | Partial | AI-07, AI-11 |
| 915 | `src/mcp_artifact_gateway/mapping/full.py` | G67 | Partial | AI-07 |
| 916 | Parses fully, discovers up to K roots (K=3), builds deterministic inventory, writes `artifact_roots` | G67 | Partial | AI-07 |
| 920 | `src/mcp_artifact_gateway/mapping/partial.py` | G68 | Partial | AI-07, AI-11 |
| 921 | Consumes byte stream only (binary_ref stream preferred) | G68 | Partial | AI-07, AI-11 |
| 922 | Enforces budgets and emits `stop_reason` | G68 | Partial | AI-07, AI-11 |
| 923 | Computes and stores: | G68 | Partial | AI-07, AI-11 |
| 924 | `map_backend_id` derived from python + ijson backend+version | G68 | Partial | AI-07, AI-11 |
| 925 | `prng_version` constant | G68 | Partial | AI-07, AI-11 |
| 926 | `map_budget_fingerprint` hash over budgets + versions | G68 | Partial | AI-07, AI-11 |
| 927 | Root path normalization rules and no wildcards in root_path | G68 | Partial | AI-07, AI-11 |
| 928 | Streaming skip contract: can discard subtrees; compute steps count all events | G68 | Partial | AI-07, AI-11 |
| 929 | Deterministic reservoir sampling: | G68 | Partial | AI-07, AI-11 |
| 930 | seed = sha256(payload_hash_full\|root_path\|map_budget_fingerprint) | G68 | Partial | AI-07, AI-11 |
| 931 | reservoir algorithm exactly as specified | G68 | Partial | AI-07, AI-11 |
| 932 | oversize sampled elements are skipped and counted (bias invariant) | G68 | Partial | AI-07, AI-11 |
| 933 | sampled_prefix_len semantics | G68 | Partial | AI-07, AI-11 |
| 934 | `sample_indices` stored sorted ascending and includes only materialized indices | G68 | Partial | AI-07, AI-11 |
| 935 | count_estimate only when stop_reason none and closing array observed | G68 | Partial | AI-07, AI-11 |
| 936 | Inventory derivation from sampled records with caps | G68 | Partial | AI-07, AI-11 |
| 937 | If stop_reason != none: | G68 | Partial | AI-07, AI-11 |
| 938 | prefix coverage true | G68 | Partial | AI-07, AI-11 |
| 939 | count_estimate null | G68 | Partial | AI-07, AI-11 |
| 943 | `src/mcp_artifact_gateway/mapping/worker.py` | G69 | Partial | AI-07, AI-11 |
| 944 | Async/hybrid/sync modes supported | G69 | Partial | AI-07, AI-11 |
| 945 | Conditional update safety: | G69 | Partial | AI-07, AI-11 |
| 946 | deleted_at null | G69 | Partial | AI-07, AI-11 |
| 947 | map_status in (pending, stale) | G69 | Partial | AI-07, AI-11 |
| 948 | generation matches snapshot | G69 | Partial | AI-07, AI-11 |
| 949 | else discard results | G69 | Partial | AI-07, AI-11 |
| 962 | `src/mcp_artifact_gateway/query/jsonpath.py` | G70 | Partial | AI-03, AI-08, AI-11 |
| 963 | Parser for allowed grammar only: `$`, `.name`, `['..']`, `[n]`, `[*]` | G70 | Partial | AI-03, AI-08, AI-11 |
| 964 | Caps: length, segments, wildcard expansion total | G70 | Partial | AI-03, AI-08, AI-11 |
| 965 | `src/mcp_artifact_gateway/query/select_paths.py` | G70 | Partial | AI-03, AI-08, AI-11 |
| 966 | Normalizes each path and rejects absolute `$` for select_paths | G70 | Partial | AI-03, AI-08, AI-11 |
| 967 | Sorts lexicographically, dedupes, computes `select_paths_hash` | G70 | Partial | AI-03, AI-08, AI-11 |
| 968 | `src/mcp_artifact_gateway/query/where_hash.py` | G70 | Partial | AI-03, AI-08, AI-11 |
| 969 | Implements `where_canonicalization_mode`: | G70 | Partial | AI-03, AI-08, AI-11 |
| 970 | raw_string hash mode | G70 | Partial | AI-03, AI-08, AI-11 |
| 972 | Exposes mode via `gateway.status()` | G71 | Verified | None |
| 976 | `src/mcp_artifact_gateway/retrieval/traversal.py` | G72 | Partial | AI-03, AI-07 |
| 977 | Arrays index ascending, objects keys lex asc | G72 | Partial | AI-03, AI-07 |
| 978 | Wildcard expansions obey same ordering | G72 | Partial | AI-03, AI-07 |
| 979 | Partial mode enumerates sampled indices ascending | G72 | Partial | AI-03, AI-07 |
| 991 | `src/mcp_artifact_gateway/cursor/secrets.py` | G73 | Verified | None |
| 992 | Loads secret set from `DATA_DIR/state/secrets.json` | G73 | Verified | None |
| 993 | Tracks active secret versions: newest signs, all active verify | G73 | Verified | None |
| 994 | `src/mcp_artifact_gateway/cursor/hmac.py` | G73 | Verified | None |
| 995 | Format: `base64url(payload_json) + "." + base64url(hmac)` | G73 | Verified | None |
| 996 | Enforces TTL and expires_at | G73 | Verified | None |
| 1000 | `src/mcp_artifact_gateway/cursor/payload.py` | G74 | Verified | None |
| 1001 | Includes all required fields in Section 14.2 | G74 | Verified | None |
| 1002 | Verifies server `where_canonicalization_mode` matches cursor else CURSOR_STALE | G74 | Verified | None |
| 1006 | `src/mcp_artifact_gateway/cursor/sample_set_hash.py` | G75 | Verified | None |
| 1007 | Computes `sample_set_hash` from root_path + stored sample_indices + map_budget_fingerprint + mapper_version | G75 | Verified | None |
| 1008 | Verification recomputes from DB and mismatch => CURSOR_STALE | G75 | Verified | None |
| 1021 | `src/mcp_artifact_gateway/mcp/server.py` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1022 | Registers gateway tools: | G76 | Partial | AI-01, AI-04, AI-11 |
| 1023 | `gateway.status` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1024 | `artifact.search` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1025 | `artifact.get` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1026 | `artifact.select` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1027 | `artifact.describe` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1028 | `artifact.find` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1029 | `artifact.chain_pages` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1030 | Also registers mirrored upstream tools at `{prefix}.{tool}` | G76 | Partial | AI-01, AI-04, AI-11 |
| 1034 | `src/mcp_artifact_gateway/tools/status.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1035 | Returns: upstream connectivity, DB ok, FS ok, versions, traversal_contract_version, where mode, map_backend_id/prng_version, budgets, cursor TTL, secret versions | G77 | Not Implemented | AI-03, AI-11 |
| 1036 | `src/mcp_artifact_gateway/tools/artifact_search.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1037 | Lists artifacts using `artifact_refs` only | G77 | Not Implemented | AI-03, AI-11 |
| 1038 | Touch policy: updates session/artifact_refs last_seen, does not touch artifact last_referenced | G77 | Not Implemented | AI-03, AI-11 |
| 1039 | `src/mcp_artifact_gateway/tools/artifact_get.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1040 | target `envelope` applies jsonpath on envelope root, reconstruct from canonical bytes if needed | G77 | Not Implemented | AI-03, AI-11 |
| 1041 | target `mapped` only if map_status ready and map_kind full/partial | G77 | Not Implemented | AI-03, AI-11 |
| 1042 | Touch semantics: touch last_referenced_at if not deleted, always update session/artifact_refs, else GONE | G77 | Not Implemented | AI-03, AI-11 |
| 1043 | `src/mcp_artifact_gateway/tools/artifact_select.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1044 | Full mapping: bounded deterministic scan | G77 | Not Implemented | AI-03, AI-11 |
| 1045 | Partial mapping: sampled-only enumeration and response includes sampled_only, sample_indices_used, sampled_prefix_len | G77 | Not Implemented | AI-03, AI-11 |
| 1046 | `src/mcp_artifact_gateway/tools/artifact_describe.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1047 | Includes partial mapping disclosures: sampled-only constraints, prefix coverage, stop_reason, counts | G77 | Not Implemented | AI-03, AI-11 |
| 1048 | `src/mcp_artifact_gateway/tools/artifact_find.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1049 | Sample-only unless index enabled | G77 | Not Implemented | AI-03, AI-11 |
| 1050 | `src/mcp_artifact_gateway/tools/artifact_chain_pages.py` | G77 | Not Implemented | AI-03, AI-11 |
| 1051 | Orders by chain_seq asc then created_seq asc, allocates chain_seq with retry | G77 | Not Implemented | AI-03, AI-11 |
| 1055 | `src/mcp_artifact_gateway/retrieval/response.py` | G78 | Verified | None |
| 1056 | Always returns `{items, truncated, cursor, omitted, stats}` | G78 | Verified | None |
| 1069 | `src/mcp_artifact_gateway/sessions.py` | G79 | Verified | None |
| 1070 | Creates or updates session row with last_seen_at | G79 | Verified | None |
| 1071 | Upserts artifact_refs (first_seen_at, last_seen_at) | G79 | Verified | None |
| 1075 | Implemented exactly: | G80 | Verified | None |
| 1076 | creation touches artifacts.last_referenced_at | G80 | Verified | None |
| 1077 | retrieval/describe touches if not deleted | G80 | Verified | None |
| 1078 | search does not touch | G80 | Verified | None |
| 1090 | `src/mcp_artifact_gateway/jobs/soft_delete.py` | G81 | Partial | AI-09, AI-11 |
| 1091 | Uses SKIP LOCKED, rechecks predicates on update, sets deleted_at and generation++ | G81 | Partial | AI-09, AI-11 |
| 1095 | `src/mcp_artifact_gateway/jobs/hard_delete.py` | G82 | Partial | AI-09, AI-11 |
| 1096 | Deletes artifacts, cascades remove artifact_roots and artifact_refs | G82 | Partial | AI-09, AI-11 |
| 1097 | Deletes unreferenced payload_blobs | G82 | Partial | AI-09, AI-11 |
| 1098 | Deletes unreferenced binary_blobs via payload_binary_refs | G82 | Partial | AI-09, AI-11 |
| 1099 | Removes filesystem blobs for removed binary_blobs | G82 | Partial | AI-09, AI-11 |
| 1103 | `src/mcp_artifact_gateway/jobs/reconcile_fs.py` | G83 | Verified | None |
| 1104 | Finds orphan files not referenced in DB and optionally removes them | G83 | Verified | None |
| 1114 | `src/mcp_artifact_gateway/obs/logging.py` | G84 | Partial | AI-10, AI-11 |
| 1115 | structlog configuration, JSON logs | G84 | Partial | AI-10, AI-11 |
| 1116 | Correlation fields: session_id, artifact_id, request_key, payload_hash_full | G84 | Partial | AI-10, AI-11 |
| 1117 | `src/mcp_artifact_gateway/obs/metrics.py` (optional) | G84 | Partial | AI-10, AI-11 |
| 1118 | Counters: | G84 | Partial | AI-10, AI-11 |
| 1119 | cache hits, alias hits, upstream calls | G84 | Partial | AI-10, AI-11 |
| 1120 | oversize JSON count | G84 | Partial | AI-10, AI-11 |
| 1121 | partial map stop_reason distribution | G84 | Partial | AI-10, AI-11 |
| 1122 | cursor stale reasons | G84 | Partial | AI-10, AI-11 |
| 1123 | advisory lock timeouts | G84 | Partial | AI-10, AI-11 |
| 1124 | Determinism debug logs: | G84 | Partial | AI-10, AI-11 |
| 1125 | map_budget_fingerprint | G84 | Partial | AI-10, AI-11 |
| 1126 | map_backend_id | G84 | Partial | AI-10, AI-11 |
| 1127 | prng_version | G84 | Partial | AI-10, AI-11 |
| 1128 | sample_set_hash on cursor issue/verify | G84 | Partial | AI-10, AI-11 |
| 1140 | `tests/test_reserved_arg_stripping.py` | G85 | Verified | None |
| 1141 | Only `_gateway_*` removed, nothing else | G85 | Verified | None |
| 1142 | `tests/test_rfc8785_vectors.py` | G85 | Verified | None |
| 1143 | `tests/test_decimal_json_no_float.py` | G85 | Verified | None |
| 1144 | `tests/test_payload_canonical_integrity.py` | G85 | Verified | None |
| 1145 | `tests/test_oversize_json_becomes_binary_ref.py` | G85 | Verified | None |
| 1146 | `tests/test_partial_mapping_determinism.py` | G85 | Verified | None |
| 1147 | same bytes + same budgets => identical sample_indices and fields_top | G85 | Verified | None |
| 1148 | `tests/test_prefix_coverage_semantics.py` | G85 | Verified | None |
| 1149 | stop_reason != none implies count_estimate null and prefix coverage true | G85 | Verified | None |
| 1150 | `tests/test_sampling_bias_invariant.py` | G85 | Verified | None |
| 1151 | oversize sampled elements are skipped and counted | G85 | Verified | None |
| 1152 | `tests/test_cursor_sample_set_hash_binding.py` | G85 | Verified | None |
| 1153 | `tests/test_cursor_where_mode_stale.py` | G85 | Verified | None |
| 1154 | `tests/test_touch_policy.py` | G85 | Verified | None |
| 1155 | Additional unit tests added beyond spec minimums (config loading, traversal, jsonpath, hashing, stores, bounded response, migrations) | G85 | Verified | None |

---

## Post-Audit Additions (PR #11, 2026-02-09)

The following groups were added retroactively to track features implemented after the original audit. These are NEW features not in the v1.9 spec, added to improve developer onboarding (standard config format, one-command init, Docker auto-provisioning).

### Group-Level Findings (Additions)
| Group | Lines | Done Tasks | Verdict | Feedback | Action Items | Evidence |
|---|---:|---:|---|---|---|---|
| G86 | 15b | 10 | Verified | Standard `mcpServers` dict format parser with transport inference, `_gateway` namespace, VS Code format support, and backward-compatible `upstreams` conversion. 892 unit tests pass. | None | `src/mcp_artifact_gateway/config/mcp_servers.py`, `src/mcp_artifact_gateway/config/settings.py`, `tests/unit/test_mcp_servers_config.py` |
| G87 | 15b | 14 | Verified | `mcp-gateway init --from <file>` migration command: reads/merges servers, backs up source, rewrites source, supports `--dry-run`/`--revert`, preserves VS Code format. CLI wiring with argparse subcommand verified. | None | `src/mcp_artifact_gateway/config/init.py`, `src/mcp_artifact_gateway/main.py`, `tests/unit/test_init_command.py`, `tests/unit/test_main.py` |
| G88 | 15b | 12 | Verified | Docker auto-provisioning of Postgres: container lifecycle (running/stopped/create), port scanning, health checks, credential extraction, DSN skip conditions (CLI flag / env var / existing config). All paths covered by ~30 unit tests. | None | `src/mcp_artifact_gateway/config/docker_postgres.py`, `tests/unit/test_docker_postgres.py` |
| G89 | 15b | 4 | Verified | README updated with mcpServers setup instructions, `mcp-gateway init` usage, manual config examples, `_gateway` namespace docs, and updated project layout. | None | `README.md` |

### Exhaustive Coverage Index (Additions)
| Plan Section | Task | Group | Verdict | Action Items |
|---|---|---|---|---|
| 15b | Standard mcpServers dict format parsing | G86 | Verified | None |
| 15b | Transport inference (command→stdio, url→http) | G86 | Verified | None |
| 15b | `_gateway` namespace for gateway-specific extensions | G86 | Verified | None |
| 15b | VS Code `mcp.servers` format support | G86 | Verified | None |
| 15b | Backward-compatible `upstreams` array support | G86 | Verified | None |
| 15b | `_resolve_mcp_servers_format()` in settings.py | G86 | Verified | None |
| 15b | `run_init()` migration function | G87 | Verified | None |
| 15b | Source file backup and rewrite | G87 | Verified | None |
| 15b | `run_revert()` backup restore | G87 | Verified | None |
| 15b | `--dry-run` mode | G87 | Verified | None |
| 15b | `--postgres-dsn` CLI flag | G87 | Verified | None |
| 15b | `init` argparse subcommand in main.py | G87 | Verified | None |
| 15b | `provision_postgres()` orchestrator | G88 | Verified | None |
| 15b | Container lifecycle (running/stopped/none) | G88 | Verified | None |
| 15b | Port scanning (5432-5442) | G88 | Verified | None |
| 15b | Health check polling | G88 | Verified | None |
| 15b | DSN skip conditions (3 sources) | G88 | Verified | None |
| 15b | Graceful fallback on DockerNotFoundError | G88 | Verified | None |
| 15b | README mcpServers setup section | G89 | Verified | None |
| 15b | README `mcp-gateway init` usage | G89 | Verified | None |

---

## Key Technical Notes (Highest Impact)
- `src/mcp_artifact_gateway/mcp/upstream.py` contains discovery/call stubs (`discover_tools` returns `[]`; `call_upstream_tool` raises `NotImplementedError`), which blocks multiple done claims in Phases 7, 11, and file-level checks.
- `src/mcp_artifact_gateway/mcp/server.py` handlers for artifact tools return `NOT_IMPLEMENTED` after basic validation; this blocks most retrieval/tool-surface done claims.
- `src/mcp_artifact_gateway/db/migrations/001_init.sql` `index_status` check currently allows `('off','ready','failed')`, while done checklist claims `off|pending|ready|partial|failed` lifecycle support.
- `src/mcp_artifact_gateway/db/conn.py` is single-connection helper (`psycopg.connect`), not a pool implementation as claimed in multiple done items.
- Test suite is broad at unit layer but lacks integration coverage for the highest-risk done claims (runtime DB/upstream/tool flow semantics).

## Recommended Sequencing for Follow-up
1. Execute `AI-04`, `AI-01`, `AI-02` together to unblock real mirrored-tool runtime behavior.
2. Execute `AI-03`, `AI-07`, `AI-08` to make retrieval/mapping/query semantics actually match done claims.
3. Execute `AI-05`, `AI-06`, `AI-09`, `AI-10`, then finalize with `AI-11` to close fidelity/operability/proof gaps.
