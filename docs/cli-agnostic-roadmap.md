# Sift Protocol-Agnostic Master Plan

## 1. Mission

Build Sift as a protocol-agnostic data workbench where MCP mode and CLI mode
share one execution core. The core must handle large outputs safely, preserve
queryability, and keep agent context small.

## 2. Product outcomes

1. Any large output can be captured once and queried incrementally.
2. MCP and CLI expose different interfaces but identical retrieval behavior.
   Pagination semantics stay aligned, while continuation invocation differs
   (`artifact(action="next_page")` in MCP vs `run --continue-from` in CLI).
3. OpenClaw users avoid context overflow by default through concise summaries.
4. Existing MCP users keep backward-compatible contracts during migration.

## 3. Constraints and design guardrails

1. Do not fork business logic by transport.
2. Keep cursor, pagination, filtering, and mapping semantics identical across interfaces.
3. Keep durable storage local-first and daemon-free.
4. Keep defaults safe: bounded outputs, TTL cleanup, deterministic traversal.
5. Ship incrementally with parity tests at each extraction step.

## 4. Target architecture

### 4.1 Layering

1. `sift_gateway/core/*`
   - Protocol-agnostic capture, artifact, query, cursor, lifecycle services.
   - Runtime protocols defining required host capabilities.
2. `sift_gateway/mcp/*`
   - MCP schemas, request validation, adapter implementations, thin handlers.
3. `sift_gateway/cli/*`
   - CLI command parsing, output formatting, adapter implementations.
4. `sift_gateway/db/*` and `sift_gateway/fs/*`
   - Shared persistence backends used by core services.

### 4.2 Core service boundaries

1. Capture service
   - Create artifacts from command output, stdin, or mirrored tool response.
2. Retrieval service
   - `describe|get|select|search|code` plus cursor continuation.
3. Pagination continuation service
   - Continue upstream pagination where upstream metadata exists.
   - MCP uses `artifact(action="next_page")`; CLI uses explicit follow-up
     `run --continue-from <artifact_id> -- <command>`.
4. Lifecycle service
   - TTL expiry, soft delete, hard delete, quota reconciliation.
5. Policy service
   - Output budgets, parsing limits, code-runtime allowlist.

### 4.3 Runtime protocols

1. Cursor runtime
   - Issue, verify, assert bindings, map errors.
2. Visibility runtime
   - Scope visibility and lineage resolution.
3. Touch runtime
   - Session/recency updates with mode-aware policy.
4. Host runtime
   - Structured `NOT_IMPLEMENTED` and capability checks.

## 5. Data model evolution plan

### 5.1 Problem

Current artifact identity fields are MCP-shaped:
`source_tool`, `upstream_instance_id`, `request_key`.
CLI mode needs equivalent provenance without fake MCP values.

### 5.2 Additive schema changes (backward-compatible first)

1. Add `capture_kind` with controlled values:
   - `mcp_tool`, `cli_command`, `stdin_pipe`, `file_ingest`, `derived_query`, `derived_codegen`.
2. Add `capture_origin` JSON for source-specific metadata:
   - MCP: `prefix`, `tool`, `upstream_instance_id`.
   - CLI: `command_argv`, `cwd`, `stdin_hash`, `env_fingerprint`.
3. Add `capture_key` as protocol-neutral identity hash.
4. Keep current MCP columns during transition and backfill from new fields.
5. Add indexes for `capture_kind`, `capture_key`, and time-based listing.

### 5.3 Migration phases

1. Phase A: additive columns + dual-write.
2. Phase B: dual-read with fallback to legacy fields.
3. Phase C: new API surfaces use neutral naming.
4. Phase D: legacy columns retained for compatibility until major release boundary.

## 6. Query contract decisions

### 6.1 Canonical filter model

1. Keep structured filter object as core canonical representation.
2. CLI `--where` DSL compiles into structured filter object.
3. MCP remains unchanged and passes structured object directly.

### 6.2 Path query model

1. Keep current JSONPath subset for deterministic behavior.
2. Reject unsupported JSONPath predicate syntax with explicit error guidance.
3. Optionally add separate `jmespath` mode later as distinct parameter, not silent overloading.

### 6.3 Cursor model

1. Keep a single cursor format and binding policy across MCP and CLI.
2. Add optional interface metadata in cursor payload only when required for UX.
3. Preserve stale/expired semantics and reasons.

## 7. Full implementation plan

### 7.1 Phase 0: Contract freeze and baseline (1-2 days)

1. Freeze current MCP behavior with explicit contract tests.
2. Snapshot golden outputs for `describe|get|select|search|code|next_page`.
3. Define acceptance criteria for parity per query kind.

Exit criteria:
1. Contract doc updated.
2. Golden tests in CI.

### 7.2 Phase 1: Core scaffolding (2-3 days)

1. Finalize `core` package structure and runtime protocol conventions.
2. Move generic row/response helpers into core utilities.
3. Standardize adapter naming (`Gateway*Runtime`, later `Cli*Runtime`).

Exit criteria:
1. Core package imports cleanly.
2. No behavior change in MCP integration tests.

### 7.3 Phase 2: Retrieval extraction by query kind (7-10 days)

1. `search` extraction (done).
2. Extract `get` into `core/artifact_get.py` (done).
3. Extract `describe` into `core/artifact_describe.py` (done).
4. Extract `select` into `core/artifact_select.py` (done).
5. Extract `code` into `core/artifact_code.py` (done).
6. Extract `next_page` into `core/artifact_next_page.py` (done).

Exit criteria:
1. MCP handlers are transport glue only.
2. Core services have direct unit tests.
3. Full unit + integration parity holds.

Status: complete as of February 19, 2026.

### 7.4 Phase 3: Capture-source neutralization (5-7 days)

1. Implement additive DB migration for `capture_kind`, `capture_origin`, `capture_key` (done).
2. Introduce protocol-neutral capture identity builder in core (done).
3. Dual-write in MCP capture path (done).
4. Update search filters to support neutral fields while preserving legacy fields (done).

Exit criteria:
1. New captures populate neutral fields.
2. Existing MCP behavior unchanged.
3. Migration tests cover backfill and dual-read.

Status: complete as of February 19, 2026.

### 7.5 Phase 4: CLI foundation (4-6 days)

1. Consolidate CLI and MCP modes under `sift-gateway` (done).
2. Implement core-driven commands (done):
   - `sift-gateway query`
   - `sift-gateway schema`
   - `sift-gateway get`
   - `sift-gateway list`
   - `sift-gateway code`
3. Provide `--json` machine mode for all commands (done).
4. Implement stable human-readable compact output defaults (done).

Exit criteria:
1. CLI retrieval commands work against existing artifacts (done).
2. Output format documented and snapshot-tested (done).

Status: complete as of February 19, 2026.

### 7.6 Phase 5: CLI capture workflows (5-8 days)

1. Implement `sift-gateway run -- <cmd>` command execution wrapper (done).
2. Implement `sift-gateway run --stdin` (done).
3. Implement tags and TTL options (done).
4. Always run commands fresh and persist each capture (done).
5. Add `sift-gateway diff` for artifact comparison (done).

Exit criteria:
1. End-to-end run/query loop tested (done; unit coverage added).
2. Run/TTL behavior deterministic (done).

Status: complete as of February 19, 2026.

### 7.7 Phase 6: OpenClaw integration pack (2-4 days)

1. Ship OpenClaw skill guidance for when to capture vs inline output (done).
2. Add examples tailored to common OpenClaw workflows (done).
3. Add troubleshooting docs for context overflow prevention (done).
4. Add concise response templates optimized for agent context budgets (done).

Exit criteria:
1. Skill docs are installable and tested manually (done; local checklist added).
2. OpenClaw-first quickstart published (done).

Status: complete as of February 19, 2026.

### 7.8 Phase 7: Packaging and naming transition (2-4 days)

1. Consolidate package and CLI handle under `sift-gateway` (done).
2. Remove separate `sift` command handle from packaging/docs (done).
3. Add optional extras:
   - `code` extra for pandas/numpy/jmespath (done).
4. Keep core install minimal without auto-install side effects (done).

Exit criteria:
1. Build and install flows validated in clean environments (done).
2. Upgrade path documented for current users (done).

Status: complete as of February 19, 2026.

### 7.9 Phase 8: Hardening and release (3-5 days)

1. Performance benchmarks on large payloads (done).
2. Long-run cleanup tests for TTL/quota/reconcile jobs (done).
3. Security pass on capture and code execution surfaces (done).
4. Changelog and migration guide finalization (done).

Exit criteria:
1. Release checklist and benchmark runner published (done).
2. No P1/P2 open defects in touched surfaces (done for Phase 8 scope).

Status: complete as of February 19, 2026.

## 8. Testing strategy

### 8.1 Test layers

1. Core unit tests
   - Behavior of each core service with fake runtimes.
2. Adapter tests
   - MCP and CLI adapters satisfy runtime contracts.
3. Contract tests
   - Golden response payloads for MCP tools and CLI commands.
4. Migration tests
   - Schema upgrades, dual-read/write correctness.
5. End-to-end tests
   - Run-capture-query flows with real sqlite/blob store.

### 8.2 Required parity gates per extracted service

1. Existing MCP tests unchanged and green.
2. New core tests cover error and boundary branches.
3. Lint and type checks pass for touched modules.

## 9. Observability and SLOs

1. Metrics
   - capture count, query count, cursor errors, stale reasons, retrieval truncation rate.
2. OpenClaw effectiveness metrics
   - average response bytes returned to agent per operation.
   - reduction in large inline output returns.
3. Reliability SLOs
   - zero data-loss on successful capture.
   - deterministic cursor resumption under stable inputs.

## 10. Risks and mitigations

1. Risk: abstraction leakage from MCP-specific assumptions.
   - Mitigation: runtime protocols + core tests without `GatewayServer`.
2. Risk: query behavior drift during extraction.
   - Mitigation: golden contract snapshots before each slice.
3. Risk: schema migration complexity.
   - Mitigation: additive-first migration and dual-read safety period.
4. Risk: CLI UX sprawl.
   - Mitigation: ship minimal stable command set first and freeze output format.
5. Risk: code-runtime dependency complexity.
   - Mitigation: optional extras and explicit install guidance.

## 11. Execution order and current status

1. Completed
   - Core search extraction.
   - Core get extraction.
   - Core describe extraction.
   - Core select extraction.
   - Core code extraction.
   - Core next_page extraction.
   - MCP adapter for search.
   - MCP adapter for get.
   - MCP adapter for describe.
   - MCP adapter for code.
   - MCP adapter for next_page.
   - MCP adapter for select.
   - Search/get/describe/select/code/next_page parity tests.
   - CLI retrieval + capture + diff + code surface.
   - OpenClaw integration pack docs and skill.
   - Packaging transition docs and extras policy (`code` extra).
   - Phase 8 hardening pack:
     - Large-payload benchmark runner + benchmark doc.
     - Long-run cleanup lifecycle tests.
     - Security hardening checklist for capture/code surfaces.
     - Migration guide and release checklist docs.
   - RC readiness tooling:
     - `scripts/run_rc_preflight.py` one-command local RC gate.
     - Published local benchmark baseline artifacts in `docs/benchmarks/`.
2. Next
   - Cut release candidate tag from current roadmap-complete state.
3. After that
   - Monitor CI release pipeline and publish package install validation notes.
4. Then
   - Optional post-RC enhancements.

## 12. Decision log needed before Phase 4

Resolved:
1. CLI executable is `sift-gateway` for both gateway and artifact modes.
2. `--where` uses structured JSON object in v1.
3. `sift-gateway code` is available in CLI; heavier runtime deps are optional via `code` extra.
4. Default CLI TTL behavior is implemented with explicit override support (`--ttl` / env).
