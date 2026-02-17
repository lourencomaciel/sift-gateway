# Sift

## Build & Test
- `python -m pytest tests/unit/ -q` — run unit tests
- `python -m ruff check src tests` — lint
- `python -m ruff format src tests` — auto-format
- `python -m mypy src` — strict type checking
- `PYTHONPATH=src python scripts/check_docs_consistency.py` — enforce docs/runtime contract
- Integration tests: `docker compose up -d` then `SIFT_MCP_TEST_POSTGRES_DSN=postgresql://sift:sift@localhost:5432/sift_test python -m pytest tests/integration/ -q`
- Build system: uv_build via pyproject.toml
- Always run full test suite after changes: tests must stay green

## CLI
- `sift-mcp --check` — validate config/DB/FS/upstreams and exit
- `sift-mcp init --from <file>` — import mcpServers config (SQLite by default; use `--db-backend postgres` for Postgres auto-provisioning)
- `sift-mcp upstream add <json>` — add upstream to existing config
- `sift-mcp instances list [--json]` — list managed instances
- `--dry-run` and `--revert` flags on init; `--data-dir` works globally
- Transport modes: `--transport stdio` (default), `sse`, `streamable-http`

## Project Structure
- Source: `src/sift_mcp/`
- `main.py` — CLI entrypoint (argparse, subcommands)
- `app.py` — Composition root (config -> db -> fs -> upstreams -> MCP server)
- `lifecycle.py` — Startup checks (`CheckResult`)
- `constants.py` — Version strings, `WORKSPACE_ID = "local"`, ID prefixes (`art_`, `bin_`), reserved keys
- `config/` — Pydantic settings (`GatewayConfig`, `UpstreamConfig`), init/sync/secrets
- `db/` — `DatabaseBackend` protocol, `Dialect` enum (Postgres/SQLite), repos, migrations
- `mcp/` — `GatewayServer`, upstream connections, tool mirroring, handlers
- `mcp/handlers/` — one handler per tool (artifact_get, artifact_select, artifact_code, etc.)
- `envelope/` — `Envelope` frozen dataclass, `ContentPart` union, `ErrorBlock`, normalize/serialize
- `artifacts/` — artifact creation pipeline (`persist_artifact`)
- `cache/` — request deduplication, advisory locks
- `mapping/` — full (in-memory) + partial (streaming) schema discovery
- `retrieval/` — bounded path traversal, where-filter evaluation
- `query/` — JSONPath, select paths, where DSL
- `pagination/` — auto-pagination loop, metadata extraction
- `cursor/` — HMAC-signed cursor tokens, key rotation
- `canon/` — RFC 8785 canonical JSON, zstandard compression
- `codegen/` — code query execution in subprocess with AST safety guards
- `obs/` — structlog setup (`LogEvents` constants), Prometheus metrics (`GatewayMetrics`)
- `jobs/` — quota enforcement, soft/hard delete, FS reconciliation
- Tests: `tests/unit/` (~97 files), `tests/integration/` (requires Postgres)
- Docs: `docs/` — README.md, quickstart.md, config.md, api_contracts.md, deployment.md, errors.md, observability.md, recipes.md, spec_v1_9.md
- Local dev: `local/` — gitignored; place ad-hoc validation scripts, scratch files, and test data here (not in `scripts/` which is tracked)

## Style Guide
- Follows the Google Python Style Guide; enforced by ruff (18 rule sets: B, C4, D, FLY, G, I, N, PERF, PIE, PT, RET, RSE, RUF, SIM, T20, TID, UP, W)
- Line length: 80 characters
- All public modules, classes, and functions have Google-style docstrings (Args/Returns/Raises)
- Tests are exempt from docstring rules (`D100-D107` suppressed under `tests/**/*.py`)
- No `@staticmethod`; prefer module-level functions
- No backslash line continuations in strings
- Exception classes must end with `Error` suffix (N818)
- Settings enums use `StrEnum` (not `str, Enum`)
- Use `contextlib.suppress()` over bare `try/except: pass` (SIM105)
- Use `pytest.raises(match=...)` over assert-in-except (PT017)
- Use `zip(..., strict=True)` for equal-length iterables (B905)
- `print()` only allowed in CLI modules (T201 per-file-ignored: main.py, config/init.py, config/upstream_add.py)

## Conventions
- Frozen dataclasses for domain models (BinaryRef, Envelope, etc.)
- Pydantic `BaseSettings` for configuration models (GatewayConfig, UpstreamConfig)
- All hashing via `util/hashing.py` — sha256_hex, binary_hash, blob_id, request_key
- Reserved key prefix `_gateway_*` — stripped before upstream forwarding and hashing
- Config precedence: env vars (SIFT_MCP_*) > state/config.json > defaults
- Config nested env var syntax: `SIFT_MCP_UPSTREAMS__0__PREFIX=github` (uses `__` delimiter)
- Env vars starting with `[` or `{` auto-parse as JSON for list/dict fields
- Tests monkeypatch module-level imports; when moving code between modules, update test patches too
- No shared pytest fixtures in root conftest — helpers are module-local
- Passthrough mode: results < passthrough_max_bytes (default 8 KB) returned raw; larger results return gateway handle payload (`artifact_id`, cache meta, inline describe, usage hint)
- Binary responses (with blob refs) never passthrough regardless of size

## Architecture
- Async throughout: handlers, upstream calls, mapping workers all use `asyncio`
- FastMCP framework: tools registered via `@app.tool()` decorator; `bootstrap_server()` async factory
- MCP tool handlers in `mcp/handlers/` — each is an async function taking typed args, returning `ToolResult`
- Upstream mirroring: `MirroredTool` dataclass wraps discovered upstream tools, registered as `prefix.tool_name`
- Database: `DatabaseBackend` protocol with SQLite and Postgres implementations; `Dialect` enum for SQL differences
- Repos: direct SQL with parameterization (no ORM); separate modules for artifacts, payloads, sessions, mapping, pruning
- Migrations: alembic-style with `schema_migrations` table; separate dirs for Postgres (`db/migrations/`) and SQLite (`db/migrations_sqlite/`)
- Error flow: upstream exceptions → `classify_upstream_exception()` → stable error codes → `ErrorBlock` in `Envelope`
- Error responses: `gateway_error(code, message)` and `gateway_tool_result()` in `envelope/responses.py`
- Logging: structlog with `LogEvents` constants for event names; JSON output to stderr
- Metrics: `GatewayMetrics` dataclass with Prometheus counters + custom thread-safe `Histogram`
- Codegen: user code runs in subprocess (`codegen/worker_main.py`) with AST validation (`ast_guard.py`) and import allowlist

## Docs Contract Guardrails
- Treat docs as contract surfaces, not optional prose.
- Any CLI/config/runtime behavior change must update docs in the same PR.
- `scripts/check_docs_consistency.py` must pass locally before opening a PR.
- GitHub Actions has a dedicated `Docs Contract` workflow; this check is expected to be required in branch protection.
- `.github/CODEOWNERS` and PR checklist enforce review attention on docs + CLI surfaces.
