# Sift

**Artifact gateway** - Structured memory for AI agents. Keeps context usable in multi-step workflows.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

AI agents break when their tools return too much data. A single MCP call or CLI command can return 30-100 KB of JSON. That is roughly 8,000-25,000 tokens spent before the agent can do the next step. After a few calls, the model starts dropping details or making bad calls. See [Why Sift exists](docs/why.md) for research and open issues behind this pattern.

Sift stores tool output as artifacts, infers a schema, and returns a compact reference with field types and sample values. The agent can see the data shape without carrying full payloads in context. When it needs details, it runs focused Python queries against stored artifacts.

Sift works with MCP clients (Claude Desktop, Claude Code, Cursor, VS Code, Windsurf, Zed) and CLI agents (OpenClaw, terminal automation). Same artifact store, same query interface, two entry points.

```
                           ┌─────────────────────┐
  MCP tool call ──────────▶│                     │──────────▶ Upstream MCP Server
  CLI command   ──────────▶│        Sift         │──────────▶ Shell command
                           │                     │
                           │   ┌─────────────┐   │
                           │   │  Artifacts  │   │
                           │   │  (SQLite)   │   │
                           │   └─────────────┘   │
                           └─────────────────────┘
                                     │
                                     ▼
                           Small output? return inline
                           Large output? return schema reference
                           Agent queries what it needs via code
```

## Quick start

### MCP agents

```bash
pipx install sift-gateway
sift-gateway init --from claude
```

Restart your MCP client. Sift mirrors upstream tools, persists outputs as artifacts, and returns either the full payload (for small responses) or a schema reference (for large responses). The agent can query stored artifacts with `artifact(action="query", query_kind="code", ...)`.

`--from` shortcuts: `claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`, or an explicit path.

### CLI agents (OpenClaw, terminal automation)

```bash
pipx install sift-gateway
sift-gateway run -- kubectl get pods -A -o json
```

Large output is stored and returned as an artifact ID plus compact schema. Example:

```bash
sift-gateway code <artifact_id> '$.items' --expr "df.groupby('status')['name'].count().to_dict()"
```

Pipe mode:

```bash
curl -s api.example.com/events | sift-gateway run --stdin
```

For OpenClaw, see the [OpenClaw Integration Pack](docs/openclaw/README.md).

## Example workflow

You ask an agent to check what is failing in prod:

```
datadog.list_monitors(tag="service:payments")
```

Without Sift, 70 KB of monitor configs and metadata can go straight into context. That is about 18,000 tokens before the next tool call.

With Sift, the agent gets a schema reference:

```json
{
  "response_mode": "schema_ref",
  "artifact_id": "art_9b2c...",
  "schemas_compact": [{"rp": "$.monitors", "f": [
    {"p": "$.name", "t": ["string"]},
    {"p": "$.status", "t": ["string"], "examples": ["Alert", "OK", "Warn"]},
    {"p": "$.type", "t": ["string"]},
    {"p": "$.last_triggered", "t": ["datetime"]}
  ]}],
  "schema_legend": {"schema": {"rp": "root_path"}, "field": {"p": "path", "t": "types"}}
}
```

The agent can then run a focused query:

```python
artifact(
    action="query",
    query_kind="code",
    artifact_id="art_9b2c...",
    root_path="$.monitors",
    code="def run(data, schema, params): return [m for m in data if m.get('status') == 'Alert']",
)
```

In this example, two calls use about 400 tokens and still leave room for follow-up steps.

## How it works

Sift runs one processing pipeline for MCP and CLI:

1. Execute the tool call or command.
2. Parse JSON output.
3. Detect pagination from the raw response.
4. Redact sensitive values (enabled by default).
5. Persist the artifact to SQLite.
6. Map the schema (field types, sample values, cardinality).
7. Choose response mode: `full` (inline) or `schema_ref` (compact reference).
8. Return the artifact-centric response.

### Response mode selection

Sift chooses between inline and reference automatically:

- If the response has upstream pagination: always `schema_ref`.
- If the full response exceeds the configured cap (default 8 KB): `schema_ref`.
- If the schema reference is at least 50% smaller than full: `schema_ref`.
- Otherwise: `full` (inline payload).

## Pagination

When upstream tools or APIs paginate, Sift handles continuation explicitly.

MCP:
```python
artifact(action="next_page", artifact_id="art_9b2c...")
```

CLI:
```bash
sift-gateway run --continue-from art_9b2c... -- gh api repos/org/repo/pulls --after NEXT_CURSOR
```

Each page creates a new artifact linked to the previous one through lineage metadata. The agent can run code queries across the full chain.

## Code queries

Both MCP and CLI agents can analyze stored artifacts with Python.

MCP:
```python
artifact(
    action="query",
    query_kind="code",
    artifact_id="art_123",
    root_path="$.items",
    code="def run(data, schema, params): return {'count': len(data)}",
)
```

CLI:
```bash
# Expression mode (receives a pandas DataFrame as df)
sift-gateway code art_123 '$.items' --expr "df['status'].value_counts().to_dict()"

# Function mode
sift-gateway code art_123 '$.items' --code "def run(data, schema, params): return {'count': len(data)}"

# File mode
sift-gateway code art_123 '$.items' --file ./analysis.py
```

Multi-artifact query example:

```python
artifact(
    action="query",
    query_kind="code",
    artifact_ids=["art_users", "art_orders"],
    root_paths={"art_users": "$.users", "art_orders": "$.orders"},
    code="""
def run(artifacts, schemas, params):
    users = {u["id"]: u["name"] for u in artifacts["art_users"]}
    return [{"user": users.get(o["user_id"]), "amount": o["amount"]}
            for o in artifacts["art_orders"]]
""",
)
```

### Import allowlist

Code queries run with a configurable import allowlist. Default modules include `math`, `json`, `re`, `collections`, `statistics`, `heapq`, `numpy`, `pandas`, `jmespath`, `datetime`, `itertools`, `functools`, `operator`, `decimal`, `csv`, `io`, `string`, `textwrap`, `copy`, `typing`, `dataclasses`, `enum`, `fractions`, `bisect`, `random`, `base64`, and `urllib.parse`.

Install additional packages:

```bash
sift-gateway install scipy matplotlib
```

## Security

Code queries use AST validation, an import allowlist, timeout enforcement, and memory limits. This is not a full OS-level sandbox.

Outbound secret redaction is enabled by default to reduce accidental leakage of API keys from upstream tool responses.

See [SECURITY.md](SECURITY.md) for the full security policy.

## Configuration

| Env var | Default | Description |
|---|---|---|
| `SIFT_GATEWAY_DATA_DIR` | `.sift-gateway` | Root data directory |
| `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` | `8192` | Inline response cap |
| `SIFT_GATEWAY_SECRET_REDACTION_ENABLED` | `true` | Redact secrets from tool output |
| `SIFT_GATEWAY_AUTH_TOKEN` | unset | Required for non-local HTTP binds |

Full reference: [docs/config.md](docs/config.md)

## Documentation

| Doc | Covers |
|---|---|
| [Why Sift Exists](docs/why.md) | Research and ecosystem context |
| [Quick Start](docs/quickstart.md) | Install, init, first artifact |
| [Recipes](docs/recipes.md) | Practical usage patterns |
| [OpenClaw Pack](docs/openclaw/README.md) | OpenClaw skill, quickstart, templates |
| [API Contracts](docs/api_contracts.md) | MCP + CLI public contract |
| [Configuration](docs/config.md) | All settings and env vars |
| [Deployment](docs/deployment.md) | Transport modes, auth, ops |
| [Errors](docs/errors.md) | Error codes and troubleshooting |
| [Observability](docs/observability.md) | Structured logging and metrics |
| [Architecture](docs/architecture.md) | Design and invariants |

## Development

```bash
git clone https://github.com/lourencomaciel/sift-gateway.git
cd sift-gateway
uv sync --extra dev

uv run python -m pytest tests/unit/ -q
uv run python -m ruff check src tests
uv run python -m mypy src
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

## License

MIT - see [LICENSE](LICENSE).
