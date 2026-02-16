# Contributing

## Development Setup

```bash
# Clone and install with dev dependencies
git clone https://github.com/zmaciel/sift-mcp.git
cd sift-mcp
uv sync --extra dev --extra postgres
```

## Running Tests

```bash
# Unit tests (~1000+ tests)
uv run python -m pytest tests/unit/ -q

# Integration tests (requires live Postgres)
SIFT_MCP_TEST_POSTGRES_DSN="postgresql://..." uv run python -m pytest tests/integration/ -q
```

## Linting & Formatting

```bash
uv run python -m ruff check src tests     # lint
uv run python -m ruff format src tests    # auto-format
uv run python -m mypy src                 # strict type checking
```

## Project Layout

```
src/sift_mcp/
  main.py                  # CLI entrypoint
  app.py                   # app composition root
  config/                  # settings, mcpServers parser, init, sync, secrets
  db/                      # pool, migrations, repositories
  fs/                      # content-addressed blob storage
  mcp/                     # upstream connections, mirroring, server wiring
  artifacts/               # envelope and artifact creation pipeline
  mapping/                 # full + partial mapping
  retrieval/               # bounded deterministic traversal responses
  cursor/                  # signed cursor payload + HMAC verification
  query/                   # JSONPath subset, select paths, where DSL
  tools/                   # gateway and artifact retrieval tool handlers
  jobs/                    # soft delete, hard delete, reconcile tasks
  obs/                     # structured logging + metrics
tests/
  unit/                    # ~1026 unit tests
  integration/             # integration tests (requires PostgreSQL)
docs/
  spec_v1_9.md            # Architecture and design specification
  config.md               # Configuration reference
  errors.md               # Error taxonomy
  observability.md        # Logging and metrics
  traversal_contract.md   # Traversal ordering rules
  cursor_contract.md      # Cursor format and staleness
  quickstart.md           # Getting started guide
  recipes.md              # Usage patterns and examples
  deployment.md           # Production deployment
  api_contracts.md        # API contracts and response formats
```

## Coding Conventions

- **Frozen dataclasses** for domain models (`BinaryRef`, `Envelope`, etc.)
- **All hashing** via `util/hashing.py` — `sha256_hex`, `binary_hash`, `blob_id`, `request_key`
- **Reserved key prefix** `_gateway_*` — stripped before upstream forwarding and hashing
- **Config precedence**: env vars (`SIFT_MCP_*`) > `state/config.json` > defaults
- **Metrics**: `prometheus_client.Counter` for counters, custom `Histogram` for latency (min/max tracking)
- **No shared pytest fixtures** in root conftest — helpers are module-local
- **Tests monkeypatch module-level imports** — when moving code between modules, update test patches too

## Pull Request Workflow

1. Branch from `main`
2. Make changes, keeping commits focused
3. Ensure all checks pass: `ruff check`, `ruff format --check`, `mypy`, `pytest`
4. Open a PR with a clear description of what changed and why

## Maintainer Release Workflow

1. Update `pyproject.toml` version and move release notes from
   `Unreleased` in `CHANGELOG.md`.
2. Merge release changes to `main`.
3. Create and push a tag that matches `v*` (for example `v0.1.1`):

```bash
git tag v0.1.1
git push origin v0.1.1
```

4. Push the tag and let GitHub Actions run
   `.github/workflows/release.yml`:
   - `verify_build` runs lint, type checks, unit tests, build, twine check,
     and wheel smoke commands.
   - `publish_testpypi` publishes to TestPyPI (`testpypi` environment).
5. After validating TestPyPI, manually run
   `.github/workflows/publish-pypi.yml` from Actions with input
   `tag=vX.Y.Z` (for example `v0.1.1`).
6. Confirm TestPyPI and PyPI install/upgrade paths:
   - `pipx install sift-mcp`
   - `uv tool install sift-mcp`

### Trusted Publisher Setup (One-Time)

Configure Trusted Publishers in both TestPyPI and PyPI with:

- GitHub repository: `zmaciel/sift-mcp`
- TestPyPI workflow: `.github/workflows/release.yml`
- TestPyPI environment: `testpypi`
- PyPI workflow: `.github/workflows/publish-pypi.yml`
- PyPI environment: `pypi`
