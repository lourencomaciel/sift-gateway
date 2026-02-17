# Sift

## Build & Test
- `python -m pytest tests/unit/ -q` — run unit tests
- `python -m ruff check src tests` — lint
- `python -m ruff format src tests` — auto-format
- `python -m mypy src` — strict type checking
- `PYTHONPATH=src python scripts/check_docs_consistency.py` — enforce docs/runtime contract
- Integration tests require live Postgres: `SIFT_MCP_TEST_POSTGRES_DSN`
- Build system: uv_build via pyproject.toml

## CLI
- `sift-mcp --check` — validate config/DB/FS/upstreams and exit
- `sift-mcp init --from <file>` — import mcpServers config (SQLite by default; use `--db-backend postgres` for Postgres auto-provisioning)
- `--dry-run` and `--revert` flags on init; `--data-dir` works globally

## Project Structure
- Source: `src/sift_mcp/`
- `main.py` — CLI entrypoint (argparse, subcommands)
- `app.py` — Composition root (config -> db -> fs -> upstreams -> MCP server)
- `lifecycle.py` — Startup checks, exports `CheckResult` (not `StartupReport`)
- `constants.py` — Version strings, WORKSPACE_ID, reserved keys
- Tests: `tests/unit/`, fixtures in `conftest.py`
- Docs: `docs/` — README.md, quickstart.md, config.md, api_contracts.md, deployment.md, errors.md, observability.md, recipes.md, spec_v1_9.md
- Local dev: `local/` — gitignored; place ad-hoc validation scripts, scratch files, and test data here (not in `scripts/` which is tracked)

## Task Plan Workflow
- `task_plan.md` contains a section-numbered completion checklist (sections 0-15b)
- "evaluate N" means: verify section N items against source code, update checkboxes, create missing pieces
- Always run full test suite after changes: tests must stay green

## Style Guide
- Follows the Google Python Style Guide; enforced by ruff with `pydocstyle convention = "google"`
- Line length: 80 characters
- All public modules, classes, and functions have Google-style docstrings (Args/Returns/Raises)
- Tests are exempt from docstring rules (`D100-D107` suppressed under `tests/**/*.py`)
- No `@staticmethod`; prefer module-level functions
- No backslash line continuations in strings

## Conventions
- Frozen dataclasses for domain models (BinaryRef, Envelope, etc.)
- All hashing via `util/hashing.py` — sha256_hex, binary_hash, blob_id, request_key
- Reserved key prefix `_gateway_*` — stripped before upstream forwarding and hashing
- Config precedence: env vars (SIFT_MCP_*) > state/config.json > defaults
- Config nested env var syntax: `SIFT_MCP_UPSTREAMS__0__PREFIX=github` (uses `__` delimiter)
- Env vars starting with `[` or `{` auto-parse as JSON for list/dict fields
- Tests monkeypatch module-level imports; when moving code between modules, update test patches too
- No shared pytest fixtures in root conftest — helpers are module-local
- Passthrough mode: results < passthrough_max_bytes (default 8 KB) returned raw; larger results return gateway handle payload (`artifact_id`, cache meta, inline describe, usage hint)
- Binary responses (with blob refs) never passthrough regardless of size

## Docs Contract Guardrails
- Treat docs as contract surfaces, not optional prose.
- Any CLI/config/runtime behavior change must update docs in the same PR.
- `scripts/check_docs_consistency.py` must pass locally before opening a PR.
- GitHub Actions has a dedicated `Docs Contract` workflow; this check is expected to be required in branch protection.
- `.github/CODEOWNERS` and PR checklist enforce review attention on docs + CLI surfaces.
