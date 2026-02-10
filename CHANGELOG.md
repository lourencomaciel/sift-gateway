# Changelog

## [Unreleased]

### Added
- MIT LICENSE file
- CONTRIBUTING.md and CHANGELOG.md
- `prometheus_client` for metrics counters (replaces custom implementation)
- SQLite per-key advisory lock emulation via `threading.Lock`
- `artifact_ref` insertion on cache hit so reused artifacts are retrievable

### Changed
- Rebranded from "MCP Artifact Gateway" to "SidePouch"
- Replaced silent `except Exception: pass` blocks with warning logs
- Removed unused `orjson` dependency

## [0.1.0] - 2025

### Added
- Make psycopg an optional dependency (`pip install .[postgres]`)
- SQLite as zero-dependency database backend (default)
- Passthrough mode: small results returned raw, larger results stored as artifacts
- Determinism artifacts in responses; cursor stale logging
- Lifecycle checks: migration validation, `--check` output, clean shutdown
- Standard `mcpServers` config format and `sidepouch-mcp init` command
- Quota enforcement with LRU prune on storage cap breach
- E2E integration tests
- Sample corruption detection and expanded where DSL/hash test coverage
- Complete MCP runtime: server, mapping pipeline, jobs, tools, observability
- Core infrastructure: canonicalization, cursors, DB, envelope, FS, query, retrieval
- Project scaffold: config, constants, package layout, and v1.9 spec
