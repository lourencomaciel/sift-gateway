# Sift

Sift is a local, single-tenant gateway for AI agent tool work.
It reduces context bloat by persisting tool output as artifacts and returning
only what the agent needs inline.

Use it with agents over MCP, or run it directly in CLI workflows.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## One Command, Two Modes

Sift uses one command handle: `sift-gateway`.

| Mode | How you run it | What it does |
|---|---|---|
| MCP gateway mode | `sift-gateway` | Mirrors upstream MCP tools and persists outputs as artifacts |
| CLI mode | `sift-gateway run` / `sift-gateway code` | Captures command output and runs Python over persisted artifacts |

These modes are independent. You can run only MCP mode, only CLI mode, or both.

## MCP Gateway Mode

Use this when an agent/client talks to tools over MCP.

### Quick start

```bash
pipx install sift-gateway
sift-gateway init --from claude
sift-gateway --check
```

Then restart your MCP client.

`--from` shortcuts:
`claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`
(or pass an explicit config path).

### Common gateway commands

```bash
sift-gateway --help
sift-gateway upstream add '<json>' --from claude
sift-gateway install pandas
sift-gateway uninstall pandas
sift-gateway --transport sse --host 127.0.0.1 --port 8080
```

### Runtime behavior

- Mirrors upstream MCP tools with original schemas.
- Persists mirrored outputs as artifacts.
- Returns either:
  - `response_mode="full"` (inline payload), or
  - `response_mode="schema_ref"` (`artifact_id` + compact schema).
- For paginated upstream results, always returns `schema_ref` and
  `pagination.next_action` with `artifact(action="next_page", ...)`.

## CLI Mode

Use this when you want artifact workflows directly in terminal automation.

### Quick start

```bash
pipx install sift-gateway
sift-gateway run -- echo '{"items":[{"id":1,"name":"a"}]}'
```

### Capture sources

```bash
# Capture command output
sift-gateway run -- git status --porcelain

# Capture stdin
cat payload.json | sift-gateway run --stdin
```

### Continuing paginated captures

If `sift-gateway run` reports `pagination.has_next_page=true`, continue with:

```bash
# first capture
sift-gateway run -- gh api repos/org/repo/pulls --limit 100 --after CUR_1

# follow-up capture linked to previous page
sift-gateway run --continue-from art_123 -- gh api repos/org/repo/pulls --limit 100 --after CUR_2
```

In MCP mode, continuation is `artifact(action="next_page", artifact_id=...)`.
In CLI mode, continuation is manual via `run --continue-from`.

### Analyze artifacts with Python

```bash
# single-artifact expression
sift-gateway code art_123 '$.items' --expr 'len(df)'

# single-artifact full function
sift-gateway code art_123 '$.items' --code 'def run(data, schema, params): return {"rows": len(data)}'

# multi-artifact expression
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.items' --expr 'len(df)'

# multi-artifact file mode
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.users' --root-path '$.orders' --file ./join.py
```

`--expr` always receives a pandas `df` DataFrame. In multi-artifact mode,
`df` is the concatenation of requested artifact rows, and
`artifact_frames` is available as a `{artifact_id: DataFrame}` mapping.

CLI mode uses local state in `.sift-gateway` by default.
Use `--data-dir` to target a different instance.

## MCP Artifact Tool Contract

MCP retrieval is through the `artifact` tool:

- `action="query"` with `query_kind="code"` only.
- `action="next_page"` to fetch the next upstream page for an artifact chain.

Example `query_kind="code"`:

```python
artifact(
    action="query",
    query_kind="code",
    artifact_id="art_123",
    root_path="$.items",
    code="def run(data, schema, params): return {'rows': len(data)}",
)
```

Example `next_page`:

```python
artifact(
    action="next_page",
    artifact_id="art_123",
)
```

## Response Modes

Sift chooses between two response modes:

- `full`: inline payload
- `schema_ref`: `artifact_id` + `schemas_compact` + `schema_legend`

Selection rules:

1. If pagination exists: always `schema_ref`.
2. Else if full response exceeds `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES`: `schema_ref`.
3. Else return `schema_ref` only when schema payload is at least 50% smaller.
4. Otherwise return `full`.

## Configuration Highlights

| Env var | Default | Description |
|---|---|---|
| `SIFT_GATEWAY_DATA_DIR` | `.sift-gateway` | Instance root directory |
| `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` | `8192` | Inline response cap used by full/schema_ref mode selection |
| `SIFT_GATEWAY_CODE_QUERY_ENABLED` | `true` | Enable code queries |
| `SIFT_GATEWAY_SECRET_REDACTION_ENABLED` | `true` | Redact likely outbound secrets |
| `SIFT_GATEWAY_AUTH_TOKEN` | unset | Required for non-local HTTP binds |

Full reference: [`docs/config.md`](docs/config.md)

## Security Notes

- Code queries use AST/import/time/memory guardrails, not full OS sandboxing.
- Outbound secret redaction is enabled by default.

Disable code queries if needed:

```bash
export SIFT_GATEWAY_CODE_QUERY_ENABLED=false
```

More: [`SECURITY.md`](SECURITY.md)

## Documentation

- [`docs/README.md`](docs/README.md) - full map
- [`docs/quickstart.md`](docs/quickstart.md)
- [`docs/recipes.md`](docs/recipes.md)
- [`docs/api_contracts.md`](docs/api_contracts.md)
- [`docs/config.md`](docs/config.md)
- [`docs/deployment.md`](docs/deployment.md)
- [`docs/errors.md`](docs/errors.md)
- [`docs/observability.md`](docs/observability.md)
- [`docs/architecture.md`](docs/architecture.md)

## Development

```bash
git clone https://github.com/lourencomaciel/sift-gateway.git
cd sift-gateway
uv sync --extra dev

UV_CACHE_DIR=/tmp/uv-cache uv run python -m ruff check src tests
UV_CACHE_DIR=/tmp/uv-cache uv run python -m mypy src
UV_CACHE_DIR=/tmp/uv-cache uv run python -m pytest tests/unit/ -q
```

## License

MIT - see [`LICENSE`](LICENSE).
