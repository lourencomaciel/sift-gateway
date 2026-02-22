# Sift

**Reliability gateway** - Schema-stable, secret-safe, pagination-complete JSON for AI agents.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

Sift is built for workflows where data correctness matters as much as model quality: enterprise automation, research pipelines, and long-running agent sessions.

For one-off CLI tasks, plain `jq` or Python can be enough. Sift adds value when you need guarantees: consistent schema handling, secret redaction before data re-enters model context, explicit pagination continuation, and an auditable artifact history.

Sift stores tool output as artifacts, infers schema metadata, and returns either inline payload (`full`) or an artifact reference (`schema_ref`). In `schema_ref`, Sift returns either a representative `sample_item` preview or verbose `schemas` fallback.

Keeping large payloads out of prompt context is still a core benefit, but it is one outcome of these guarantees rather than the only goal. See [Why Sift exists](docs/why.md) for research and ecosystem context.

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

Use this flow when you need reproducibility and policy controls on command output (not just ad-hoc extraction). Large output is stored and returned as an artifact ID plus `schema_ref` metadata. Example:

```bash
sift-gateway code <artifact_id> '$.items' --code "def run(data, schema, params): return {'rows': len(data)}"
```

Another capture example:

```bash
sift-gateway run -- curl -s api.example.com/events
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
  "sample_item": {
    "name": "Payments monitor",
    "status": "Alert",
    "type": "query alert"
  },
  "sample_item_source_index": 0,
  "sample_item_count": 120
}
```

If a representative sample is not valid for the result set, `schema_ref` falls back to `schemas`.

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
7. Choose response mode: `full` (inline) or `schema_ref` (sample preview or schema fallback).
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

Code queries run with a configurable import allowlist. Default allowed import roots include `math`, `json`, `re`, `collections`, `statistics`, `heapq`, `numpy`, `pandas`, `jmespath`, `datetime`, `itertools`, `functools`, `operator`, `decimal`, `csv`, `io`, `string`, `textwrap`, `copy`, `typing`, `dataclasses`, `enum`, `fractions`, `bisect`, `random`, `base64`, and `urllib.parse`. Third-party modules are usable only when installed in Sift's runtime environment.

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

## Benchmarks

The Tier 1 benchmark compares Sift's schema_ref + codegen approach against naive full-JSON context stuffing across 8 real-world datasets and 43 factual questions.

Results with `claude-sonnet-4-6`:

| Condition | Accuracy | Input Tokens | Token Reduction |
|---|---|---|---|
| Baseline (context-stuffed) | 11/43 (25.6%) | 2,053,966 | — |
| **Sift (schema_ref + code)** | **33/43 (76.7%)** | **158,620** | **92.3%** |

Sift answers 3x more questions correctly while using ~13x fewer tokens. The baseline fails entirely on large datasets (earthquakes, laureates, photos) where payloads exceed context limits, while Sift handles them through artifact queries.

```bash
python benchmarks/tier1/fetch_data.py
python benchmarks/tier1/harness.py --model claude-sonnet-4-6
```

See `benchmarks/tier1/` for the full suite and per-dataset breakdown.

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
