# Sift

Sift is a local, single-tenant gateway for AI agent tool work.
It fixes context bloat by storing large tool outputs as artifacts and returning
only the slices an agent needs.

Use it with agents that talk through MCP, or run it directly in CLI workflows.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## One Command, Two Modes

Sift uses one command handle: `sift-gateway`.

| Mode | How you run it | What it does |
|---|---|---|
| MCP gateway mode | `sift-gateway` (default server behavior) | Proxies MCP traffic and persists tool outputs as artifacts |
| CLI mode | `sift-gateway <artifact-command>` | Captures, queries, and computes over artifacts directly |

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
- Returns small responses inline and larger responses as handles.

## CLI Mode

Use this when you want artifact workflows directly in terminal automation.

### Quick start

```bash
pipx install sift-gateway
sift-gateway run -- echo '{"items":[{"id":1,"name":"a"}]}'
sift-gateway list --limit 10
```

### Capture sources

```bash
# Capture command output
sift-gateway run -- git status --porcelain

# Capture stdin
cat payload.json | sift-gateway run --stdin
```

### Continuing paginated CLI captures

If `sift-gateway run` detects upstream pagination, it returns:

- `pagination.has_next_page`
- `pagination.next_params`
- `pagination.next_action.command_line`

Follow up by running the next command explicitly and linking lineage:

```bash
# first capture
sift-gateway run -- gh api repos/org/repo/pulls --limit 100 --after CUR_1

# follow-up capture (example: apply next_params from prior response)
sift-gateway run --continue-from art_123 -- gh api repos/org/repo/pulls --limit 100 --after CUR_2
```

In MCP mode, continuation is `artifact(action="next_page", artifact_id=...)`.
In CLI mode, continuation is manual via `run --continue-from`.

### Query and inspect artifacts

```bash
sift-gateway schema art_123
sift-gateway get art_123
sift-gateway query art_123 '$.items' --select id,name --limit 50
sift-gateway code art_123 '$.items' --expr 'len(df)'
sift-gateway diff art_left art_right
```

CLI mode uses local state in `.sift-gateway` by default.
Use `--data-dir` to target a different instance.

## MCP Artifact Query Model

In MCP workflows, retrieval happens through the `artifact` tool with
`action="query"`.

Supported `query_kind` values:

| query_kind | Purpose |
|---|---|
| `describe` | Schema and metadata |
| `get` | Full payload retrieval |
| `select` | Projection/filtering from a root path |
| `search` | Search and list session artifacts in the current workspace |
| `code` | Execute constrained Python over artifact data |

Example `query_kind="search"`:

```python
artifact(
    action="query",
    query_kind="search",
    query="github issues",
    limit=25,
)
```

Example `query_kind="select"`:

```python
artifact(
    action="query",
    query_kind="select",
    artifact_id="art_123",
    root_path="$.items",
    select_paths=["id", "name", "status"],
    limit=50,
)
```

## Context-Bloat Controls

- `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` controls inline-vs-handle threshold.
- Default: `8192` bytes.
- Set to `0` to force handle-first behavior.

## Configuration Highlights

| Env var | Default | Description |
|---|---|---|
| `SIFT_GATEWAY_DATA_DIR` | `.sift-gateway` | Instance root directory |
| `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` | `8192` | Inline response threshold |
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

- [`docs/quickstart.md`](docs/quickstart.md)
- [`docs/config.md`](docs/config.md)
- [`docs/api_contracts.md`](docs/api_contracts.md)
- [`docs/recipes.md`](docs/recipes.md)
- [`docs/deployment.md`](docs/deployment.md)
- [`docs/errors.md`](docs/errors.md)
- [`docs/observability.md`](docs/observability.md)
- [`docs/architecture.md`](docs/architecture.md)
- [`docs/openclaw/README.md`](docs/openclaw/README.md)

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
