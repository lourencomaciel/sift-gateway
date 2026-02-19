# Contributing

## Development Setup

```bash
# Clone and install with dev dependencies
git clone https://github.com/lourencomaciel/sift-gateway.git
cd sift-gateway
uv sync --extra dev
```

## Running Tests

```bash
# Unit tests
uv run python -m pytest tests/unit/ -q

# Integration tests (SQLite, no external deps)
uv run python -m pytest tests/integration/ -q
```

## Linting & Formatting

```bash
uv run python -m ruff check src tests     # lint
uv run python -m ruff format src tests    # auto-format
uv run python -m mypy src                 # strict type checking
PYTHONPATH=src uv run python scripts/check_docs_consistency.py  # docs/runtime contract
```

## Docs and CLI Contract

Treat docs as part of the runtime contract.

- If CLI surface changes (flags, defaults, behavior), update docs in the same PR.
- If docs claim behavior, ensure that behavior exists in code/tests.
- `scripts/check_docs_consistency.py` is a required quality gate.
- Keep examples executable and aligned with current parser behavior.

Guardrails in this repo:

- `.github/workflows/docs-contract.yml` runs docs/runtime consistency checks.
- `.github/CODEOWNERS` routes docs/CLI/CI changes to maintainer review.
- `.github/pull_request_template.md` includes docs/CLI contract checklist items.

## Project Layout

```
src/sift_gateway/
  main.py                  # CLI entrypoint
  app.py                   # app composition root
  config/                  # settings, mcpServers parser, init, sync, secrets
  db/                      # pool, migrations, repositories
  fs/                      # content-addressed blob storage
  mcp/                     # upstream connections, mirroring, server wiring
  artifacts/               # envelope and artifact creation pipeline
  mapping/                 # full + partial mapping
  retrieval/               # bounded deterministic traversal responses
  cursor/                  # cursor token encoding, payload construction, TTL
  query/                   # JSONPath, select paths, structured filters, SQL
  tools/                   # gateway and artifact retrieval tool handlers
  jobs/                    # soft delete, hard delete, reconcile tasks
  obs/                     # structured logging + metrics
tests/
  unit/                    # unit tests
  integration/             # integration tests
docs/
  README.md              # Documentation map and reading paths
  architecture.md         # Architecture and design specification
  config.md               # Configuration reference
  errors.md               # Error taxonomy
  observability.md        # Logging and metrics
  quickstart.md           # Getting started guide
  recipes.md              # Usage patterns and examples
  deployment.md           # Production deployment
  api_contracts.md        # API contracts and response formats
```

## Coding Conventions

- **Frozen dataclasses** for domain models (`BinaryRef`, `Envelope`, etc.)
- **All hashing** via `util/hashing.py` — `sha256_hex`, `binary_hash`, `blob_id`, `request_key`
- **Reserved key prefix** `_gateway_*` — stripped before upstream forwarding and hashing
- **Config precedence**: env vars (`SIFT_GATEWAY_*`) > `state/config.json` > defaults
- **Metrics**: `prometheus_client.Counter` for counters, custom `Histogram` for latency (min/max tracking)
- **No shared pytest fixtures** in root conftest — helpers are module-local
- **Tests monkeypatch module-level imports** — when moving code between modules, update test patches too

## Pull Request Workflow

1. Branch from `main`
2. Make changes, keeping commits focused
3. Ensure all checks pass: `ruff check`, `ruff format --check`, `mypy`, `pytest`, `check_docs_consistency`
4. Open a PR with a clear description of what changed and why

## Maintainer GitHub Settings (Required)

Use repository rulesets/branch protection on `main` with:

- Require pull requests before merge
- Require at least one approval
- Require code owner review
- Require status checks to pass:
  - `CI / quality`
  - `Docs Contract / docs-contract`
- Require conversation resolution before merge
- Disable force pushes and branch deletion

Recommended setup path (rulesets):

1. Open repository **Settings**.
2. Go to **Rules** -> **Rulesets**.
3. Click **New ruleset** -> **Import a ruleset**.
4. Upload `.github/rulesets/main-protection.json`.
5. Confirm required checks include:
   - `CI / quality`
   - `Docs Contract / docs-contract`
6. Save and enable the ruleset.

## Maintainer Release Workflow

1. Run local preflight checks:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/run_rc_preflight.py
```

2. Update `pyproject.toml` version and move release notes from
   `Unreleased` in `CHANGELOG.md`.
3. Merge release changes to `main`.
4. Create and push a tag that matches `v*` (for example `v0.1.1`):

```bash
git tag v0.1.1
git push origin v0.1.1
```

5. Push the tag and let GitHub Actions run
   `.github/workflows/release.yml`:
   - `verify_build` runs lint, type checks, unit tests, build, twine check,
     and wheel smoke commands.
   - `publish_testpypi` publishes to TestPyPI (`testpypi` environment).
   - `publish_pypi` publishes to PyPI (`pypi` environment) after
     `publish_testpypi` succeeds.
6. Validate `publish_testpypi` output before approving the `pypi`
   environment deployment.
7. Confirm TestPyPI and PyPI install/upgrade paths:
   - `pipx install sift-gateway`
   - `uv tool install sift-gateway`

### Trusted Publisher Setup (One-Time)

Configure Trusted Publishers in both TestPyPI and PyPI with:

- GitHub repository: `lourencomaciel/sift-gateway`
- TestPyPI workflow: `.github/workflows/release.yml`
- TestPyPI environment: `testpypi`
- PyPI workflow: `.github/workflows/release.yml`
- PyPI environment: `pypi`
