# Changelog

## [Unreleased]

## [0.3.3] - 2026-03-02

### Changed
- Relaxed secret redaction for signed file/media URLs so URL query signatures remain usable while preserving known token-pattern redaction

### Fixed
- Fixed CI lint compliance in URL-extension allowlist construction used by response redaction

## [0.3.2] - 2026-03-02

### Added
- Added `sift-gateway upstream login` command to run OAuth login for HTTP upstreams and persist auth headers in secret storage
- Added headless OAuth login mode (`--headless`) for CI/testing flows
- Added unit and integration coverage for OAuth login flow, including end-to-end headless lifecycle behavior

### Changed
- Updated quickstart and upstream registration docs to document OAuth login usage and behavior

### Fixed
- Fixed strict type-checking for OAuth transport selection by annotating shared transport assignment with `ClientTransport`

## [0.3.1] - 2026-02-27

### Added
- Registry-backed upstream management: `sift-gateway upstream add/remove/list/enable/disable/sync` admin CLI with SQLite-backed upstream registry and registry-first loading (#121)
- New DB migration `008_upstream_registry` for registry tables
- `codegen.validate` module exposing AST guard as a reusable utility (#114)
- `core.retrieval_helpers` expanded with reusable retrieval truncation utilities (#115)

### Changed
- Refactored CLI internals: extracted `cli/parse.py` and `cli/output.py` from monolithic `cli_main.py` (#97)
- Refactored MCP server: extracted `mcp/server_helpers.py` and `mcp/server_runtime.py` from `mcp/server.py` (#97)
- Refactored code-query core: split `artifact_code.py` into `artifact_code_collect.py`, `artifact_code_hints.py`, `artifact_code_internal.py`, and `artifact_code_parse.py` (#97)
- `upstream add` now delegates to shared registry when registry is enabled (#121)
- Config loading follows registry-first precedence when upstream registry is populated (#121)

### Fixed
- Stripped `_locator` metadata from codegen user data to prevent internal keys leaking into code-query context (#104)

## [0.3.0] - 2026-02-21

### Changed
- Removed compact schema payload/legend from `schema_ref` responses (`schemas_compact`, `schema_legend`)
- `schema_ref` now returns representative `sample_item` when item shape is consistent, with verbose `schemas` fallback
- Trimmed `sift-gateway run --json` model output to drop transport noise fields (`command_exit_code`, `payload_total_bytes`, `capture_kind`, `expires_at`, `status`, `tags`)
- Fixed usage hint root-path derivation to use sampled array paths (for example `$.items`) when schemas are absent
- Repositioned top-level docs and OpenClaw pack messaging around reliability controls (schema consistency, redaction posture, explicit pagination continuity, and artifact reproducibility)

## [0.2.8] - 2026-02-20

### Changed
- Normalized compact schema payload handling for `query_kind=code` so `schemas_compact` no longer collapses into null placeholders in multi-artifact code queries
- Aligned `sift-gateway code` CLI output normalization with the same compact schema handling used by tool responses

## [0.2.7] - 2026-02-20

### Changed
- **Breaking:** removed `sift-gateway code --expr`; CLI code queries now require `--code` or `--file`
- **Breaking:** removed `sift-gateway run --stdin`; CLI captures are command-backed via `run -- <command>`
- **Breaking:** removed legacy `zstd` envelope canonical encoding support (only `gzip` and `none` are accepted)
- `scope=all_related` lineage traversal now follows pagination chains only (excludes non-pagination lineage edges)

## [0.2.6] - 2026-02-20

### Added
- MIT LICENSE file
- CONTRIBUTING.md and CHANGELOG.md
- `prometheus_client` for metrics counters (replaces custom implementation)
- SQLite per-key advisory lock emulation via `threading.Lock`
- `artifact_ref` insertion on cache hit so reused artifacts are retrievable
- Code-query runtime tracebacks in error details (`details.traceback`, up to 2000 chars)
- Multi-artifact code queries via `artifact_ids`
- Sample-based schema enum metadata: `distinct_values` (max 10) and `cardinality`
- `sift-gateway code` CLI command with `--code`, `--file`, and `--expr` modes
- Phase 8 hardening docs: benchmark guide, security checklist, migration guide, and release checklist
- Large payload benchmark runner: `scripts/benchmark_large_payloads.py`
- Long-run cleanup lifecycle tests covering TTL soft-delete, quota prune, hard-delete, and reconcile
- RC preflight runner: `scripts/run_rc_preflight.py`
- Published benchmark baseline artifacts under `docs/benchmarks/`
- Packaged OpenClaw integration files in the PyPI distribution and added `sift-gateway-openclaw-skill` helper command

### Changed
- Rebranded from "MCP Artifact Gateway" to "Sift"
- Replaced silent `except Exception: pass` blocks with warning logs
- Structured logs are now opt-in via top-level `--logs`; default CLI output suppresses structured stderr logs
- **Breaking:** public artifact contract is now `query_kind=code` + `action=next_page`; legacy `describe|get|select|search` are removed from primary docs/workflows
- **Breaking:** artifact CLI contract is now `run`/`run --continue-from`/`code`; legacy `list|schema|get|query|diff` are no longer documented public workflow
- **Breaking:** `query_kind=code` now returns all results without pagination/cursor
- `query_kind=code` supports `scope` (`all_related` default, `single` anchor-only)
- Documented return normalization for code queries (non-list values auto-wrap to one-item lists)
- Mirrored/tool and CLI responses now use unified `full` vs `schema_ref` selection policy
- `docs/cli-agnostic-roadmap.md` now marks Phase 8 complete and tracks RC cut as next action

## [0.1.0] - 2025

### Added
- Make psycopg an optional dependency (`pip install .[postgres]`)
- SQLite as zero-dependency database backend (default)
- Passthrough mode: small results returned raw, larger results stored as artifacts
- Determinism artifacts in responses; cursor stale logging
- Lifecycle checks: migration validation, `--check` output, clean shutdown
- Standard `mcpServers` config format and `sift-gateway init` command
- Quota enforcement with LRU prune on storage cap breach
- E2E integration tests
- Sample corruption detection and expanded where DSL/hash test coverage
- Complete MCP runtime: server, mapping pipeline, jobs, tools, observability
- Core infrastructure: canonicalization, cursors, DB, envelope, FS, query, retrieval
- Project scaffold: config, constants, package layout, and v1.9 spec
