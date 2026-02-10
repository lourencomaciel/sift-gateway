# Contributing

## Development Setup

```bash
# Clone and install with dev dependencies
git clone https://github.com/zmaciel/sidepouch-mcp.git
cd sidepouch-mcp
uv sync --all-extras --group dev
```

## Running Tests

```bash
# Unit tests (~1000+ tests)
python -m pytest tests/unit/ -q

# Integration tests (requires live Postgres)
SIDEPOUCH_MCP_TEST_POSTGRES_DSN="postgresql://..." python -m pytest tests/integration/ -q
```

## Linting & Formatting

```bash
python -m ruff check src tests     # lint
python -m ruff format src tests    # auto-format
python -m mypy src                 # strict type checking
```

## Coding Conventions

- **Frozen dataclasses** for domain models (`BinaryRef`, `Envelope`, etc.)
- **All hashing** via `util/hashing.py` — `sha256_hex`, `binary_hash`, `blob_id`, `request_key`
- **Reserved key prefix** `_gateway_*` — stripped before upstream forwarding and hashing
- **Config precedence**: env vars (`SIDEPOUCH_MCP_*`) > `state/config.json` > defaults
- **Metrics**: `prometheus_client.Counter` for counters, custom `Histogram` for latency (min/max tracking)
- **No shared pytest fixtures** in root conftest — helpers are module-local
- **Tests monkeypatch module-level imports** — when moving code between modules, update test patches too

## Pull Request Workflow

1. Branch from `main`
2. Make changes, keeping commits focused
3. Ensure all checks pass: `ruff check`, `ruff format --check`, `mypy`, `pytest`
4. Open a PR with a clear description of what changed and why
