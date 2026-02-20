# Sift

## Build & Test
- `uv run python -m pytest tests/unit/ -q` — run unit tests
- `uv run python -m ruff check src tests` — lint
- `uv run python -m ruff format src tests` — auto-format
- `uv run python -m mypy src` — strict type checking
- `PYTHONPATH=src uv run python scripts/check_docs_consistency.py` — enforce docs/runtime contract
- Integration tests: `uv run python -m pytest tests/integration/ -q`
- Build system: uv_build via pyproject.toml
- Always run full test suite after changes: tests must stay green

## CLI
- `sift-gateway --check` — validate config/DB/FS/upstreams and exit
- `sift-gateway init --from <file>` — import mcpServers config (SQLite backend)
- `sift-gateway upstream add <json>` — add upstream to existing config
- `sift-gateway install <packages...>` / `sift-gateway uninstall <packages...>` — manage code-query packages
- `--dry-run` and `--revert` flags on init; `--data-dir` works globally
- Transport modes: `--transport stdio` (default), `sse`, `streamable-http`

## Project Structure
- Source: `src/sift_gateway/`
- `main.py` — CLI entrypoint (argparse, subcommands)
- `app.py` — Composition root (config -> db -> fs -> upstreams -> MCP server)
- `lifecycle.py` — Startup checks (`CheckResult`)
- `constants.py` — Version strings, `WORKSPACE_ID = "local"`, ID prefixes (`art_`, `bin_`), reserved keys
- `config/` — Pydantic settings (`GatewayConfig`, `UpstreamConfig`), init/sync/secrets
- `db/` — SQLite backend, repos, migrations
- `mcp/` — `GatewayServer`, upstream connections, tool mirroring, handlers
- `mcp/handlers/` — consolidated artifact routing (`query_kind=code`, `next_page`) plus mirrored tool handling
- `envelope/` — `Envelope` frozen dataclass, `ContentPart` union, `ErrorBlock`, normalize/serialize
- `artifacts/` — artifact creation pipeline (`persist_artifact`)
- `mapping/` — full (in-memory) + partial (streaming) schema discovery
- `retrieval/` — output budget truncation for retrieval responses
- `query/` — JSONPath, select paths, structured filters, SQL compilation
- `pagination/` — auto-pagination loop, metadata extraction
- `cursor/` — Unsigned cursor tokens (`cur1.<payload_b64u>`), TTL enforcement
- `canon/` — RFC 8785 canonical JSON and compression helpers (`gzip`/`none`)
- `codegen/` — code query execution in subprocess with AST safety guards
- `obs/` — structlog setup (`LogEvents` constants), Prometheus metrics (`GatewayMetrics`)
- `jobs/` — quota enforcement, soft/hard delete, FS reconciliation
- Tests: `tests/unit/` (~97 files), `tests/integration/`
- Docs: `docs/` — README.md, quickstart.md, config.md, api_contracts.md, deployment.md, errors.md, observability.md, recipes.md, architecture.md
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
- Config precedence: env vars (SIFT_GATEWAY_*) > state/config.json > defaults
- Config nested env var syntax: `SIFT_GATEWAY_UPSTREAMS__0__PREFIX=github` (uses `__` delimiter)
- Env vars starting with `[` or `{` auto-parse as JSON for list/dict fields
- Tests monkeypatch module-level imports; when moving code between modules, update test patches too
- No shared pytest fixtures in root conftest — helpers are module-local
- Mirrored tool calls always persist artifacts and return `response_mode=full|schema_ref`
- Responses use `response_mode=full|schema_ref`; pagination always forces `schema_ref`
- Upstream pagination state on handle responses is surfaced in `pagination` metadata and continued via `artifact(action="next_page", artifact_id=...)`

## Architecture
- Async throughout: handlers, upstream calls, mapping workers all use `asyncio`
- FastMCP framework: tools registered programmatically in `build_fastmcp_app()`; `bootstrap_server()` async factory
- MCP tool handlers in `mcp/handlers/` are async gateway routines wired through `GatewayServer` dispatch methods
- Upstream mirroring: `MirroredTool` dataclass wraps discovered upstream tools, registered as `prefix.tool_name`
- Database: SQLite backend with WAL mode
- Repos: direct SQL with parameterization (no ORM); separate modules for artifacts, payloads, sessions, mapping, pruning
- Migrations: startup migration manager with `schema_migrations` table (implemented in `db/migrate.py`)
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
