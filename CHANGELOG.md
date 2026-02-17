# Changelog

## [Unreleased]

### Added
- MIT LICENSE file
- CONTRIBUTING.md and CHANGELOG.md
- `prometheus_client` for metrics counters (replaces custom implementation)
- SQLite per-key advisory lock emulation via `threading.Lock`
- `artifact_ref` insertion on cache hit so reused artifacts are retrievable
- Code-query runtime tracebacks in error details (`details.traceback`, up to 2000 chars)
- Multi-artifact code queries via `artifact_ids`
- Sample-based schema enum metadata: `distinct_values` (max 10) and `cardinality`

### Changed
- Rebranded from "MCP Artifact Gateway" to "Sift"
- Replaced silent `except Exception: pass` blocks with warning logs
- **Breaking:** `query_kind=code` now returns all results without pagination/cursor
- `query_kind=code` ignores `scope` (always all-related semantics)
- Documented return normalization for code queries (non-list values auto-wrap to one-item lists)

## [0.1.0] - 2025

### Added
- Make psycopg an optional dependency (`pip install .[postgres]`)
- SQLite as zero-dependency database backend (default)
- Passthrough mode: small results returned raw, larger results stored as artifacts
- Determinism artifacts in responses; cursor stale logging
- Lifecycle checks: migration validation, `--check` output, clean shutdown
- Standard `mcpServers` config format and `sift-mcp init` command
- Quota enforcement with LRU prune on storage cap breach
- E2E integration tests
- Sample corruption detection and expanded where DSL/hash test coverage
- Complete MCP runtime: server, mapping pipeline, jobs, tools, observability
- Core infrastructure: canonicalization, cursors, DB, envelope, FS, query, retrieval
- Project scaffold: config, constants, package layout, and v1.9 spec
