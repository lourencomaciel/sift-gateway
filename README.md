# Sift

**MCP Artifact Gateway** — Keep large MCP responses out of your context window. Query them.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-mcp.svg)](https://pypi.org/project/sift-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

AI agents connect to real-world tools through MCP. When those tools return large responses, the data floods the context window, degrading model performance, inflating cost, and blocking multi-step workflows. Sift sits invisibly between your MCP client and upstream servers: small responses pass through untouched, large payloads get stored and replaced with lightweight handles. You get retrieval tools to query, page, chain, and compute over the stored data without loading it into context.

Nothing about your existing MCP servers needs to change.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────────────┐
│  MCP Client  │────────▶│     Sift     │────────▶│  Upstream MCP Servers│
│  (Claude,    │◀────────│              │◀────────│  (GitHub, Slack,     │
│   Cursor,    │ handle  │   ┌──────┐   │  large  │   Meta Ads, ...)     │
│   VS Code)   │ or raw  │   │Store │   │  resp.  │                      │
└──────────────┘         │   └──────┘   │         └──────────────────────┘
                         └──────────────┘
                          < 8 KB ─▶ passthrough (raw response)
                          ≥ 8 KB ─▶ store + return handle (artifact_id + schema)
```

## Why This Matters

A typical MCP tool call returning GitHub issues, database query results, or Slack threads can easily be 30-100 KB. Without Sift, that's 8,000-25,000 tokens consumed every time the model references the data. With Sift, the handle is roughly 200-400 tokens, and selective retrieval pulls only what's needed.

Research confirms this problem doesn't go away with larger context windows. Models show measurable performance degradation well before hitting their advertised limits, effective capacity is 60-70% of the stated maximum, and cost scales linearly with tokens regardless.

Sift is the first tool in the MCP ecosystem to treat tool responses as first-class, persistent, queryable, computable artifacts. It's not a proxy (it stores and indexes), not a cache (it infers schemas and supports compute), and not a database connector (it intercepts outputs from any tool automatically). It's a new category: the **artifact gateway**.

## Quick Start

**1. Install**

```bash
pipx install sift-mcp
```

**2. Import your MCP config**

```bash
sift-mcp init --from claude
```

This reads your existing MCP client config, moves your servers behind Sift, and writes everything back. One command. Shortcuts: `claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`, or pass an explicit file path.

**3. Restart your MCP client**

That's it. Sift is now proxying your upstream servers. Responses over 8 KB are automatically stored as queryable artifacts. Everything under 8 KB flows through transparently, as if Sift isn't there.

> **Note:** Your MCP client (Claude Desktop, Claude Code, Cursor, VS Code, Windsurf, Zed) launches Sift automatically via the config. Use `sift-mcp --check` to verify health without starting the server.

## How It Works

1. Sift connects to your configured upstream MCP servers (stdio or HTTP).
2. Each upstream tool is mirrored as `{prefix}.{tool_name}` with the original schema preserved. No injected fields.
3. When a tool returns a response under 8 KB (configurable), the raw response passes through to your client unchanged.
4. When a response exceeds the threshold, Sift stores it durably (SQLite by default, or PostgreSQL), infers the schema, and returns a lightweight **artifact handle** containing the `artifact_id`, discovered schemas, and a usage hint.
5. You query the stored artifact using `artifact(action="query", query_kind=...)` with bounded, paginated responses.

## What You Can Do With Artifacts

### Selective retrieval

Pull exactly the fields you need from a stored response without loading the full payload:

```python
artifact(
    action="query",
    query_kind="select",
    artifact_id="art_7f3a...",
    root_path="$.items",
    select_paths=["name", "status", "assignee"],
    limit=50,
)
```

Results come back paginated. Continue with the returned cursor until `pagination.retrieval_status == "COMPLETE"`.

### Code queries

Run Python against stored data without loading it into context. Install libraries with `sift-mcp install pandas` (or `pipx install "sift-mcp[data-science]"` for the bundle):

```python
artifact(
    action="query",
    query_kind="code",
    artifact_id="art_7f3a...",
    root_path="$.issues",
    code="""
def run(data, schema, params):
    import pandas as pd
    df = pd.DataFrame(data)
    return df.groupby("assignee")["story_points"].sum().to_dict()
""",
)
```

Multi-artifact code queries let you join data across tools:

```python
artifact(
    action="query",
    query_kind="code",
    artifact_ids=["art_users...", "art_orders..."],
    root_paths={"art_users...": "$.users", "art_orders...": "$.orders"},
    code="""
def run(artifacts, schemas, params):
    users = {u["id"]: u["name"] for u in artifacts["art_users..."]}
    return [
        {"user": users.get(o["user_id"]), "amount": o["amount"]}
        for o in artifacts["art_orders..."]
    ]
""",
)
```

### Tool chaining with artifact references

Pass data between tools without the model ever loading the intermediate payload:

```python
# tool_a returns an artifact handle with artifact_id
# Pass the artifact_id (or a JSONPath into it) to another tool
tool_b(input="art_7f3a...")                     # full payload resolved server-side
tool_b(email="art_7f3a...:$.users[0].email")    # specific field
tool_b(ids="art_7f3a...:$.items[*].id")         # wildcard expansion
```

Sift resolves references server-side before forwarding to the upstream tool. The model sends a reference string, not the data. Two tool calls, zero intermediate tokens.

### Other query kinds

| Query kind | Purpose |
|---|---|
| `describe` | Schema and metadata for a stored artifact |
| `get` | Full original response (paginated if large) |
| `select` | JSONPath extraction with pagination |
| `code` | Python execution against stored data |
| `search` | List session artifacts visible in the current workspace |

## Security Note: Code Queries

Code queries execute model-generated Python in a subprocess with AST-level validation, an import allowlist, timeout enforcement, and memory limits. **This is not a full OS-level sandbox.** The guardrails prevent common abuse patterns but do not provide the isolation guarantees of a container or VM.

For production environments where untrusted models generate code, consider running Sift inside a container or disabling code queries entirely:

```bash
export SIFT_MCP_CODE_QUERY_ENABLED=false
```

See [SECURITY.md](SECURITY.md) for reporting vulnerabilities.

## Documentation

| Doc | What it covers |
|---|---|
| **[Quick Start Guide](docs/quickstart.md)** | Detailed setup: install, init, PostgreSQL, first artifact |
| **[Configuration Reference](docs/config.md)** | Every setting, env var, and default |
| **[Recipes & Examples](docs/recipes.md)** | Pagination loops, tool chaining, code queries, search |
| **[API Contracts](docs/api_contracts.md)** | Response shapes, pagination layers, handle format |
| **[Deployment Guide](docs/deployment.md)** | Transport modes, PostgreSQL, multi-process, monitoring |
| **[Error Reference](docs/errors.md)** | Error codes and troubleshooting |
| **[Observability](docs/observability.md)** | Structured logging events and metrics |
| **[Architecture](docs/architecture.md)** | Design specification and invariants |

## Development

```bash
git clone https://github.com/lourencomaciel/sift-mcp.git
cd sift-mcp
uv sync --extra dev

# Tests
uv run pytest tests/unit/ -q

# Lint and type check
uv run ruff check src tests
uv run mypy src
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide, release workflow, and coding conventions.

## Requirements

- Python >= 3.11
- [pipx](https://pipx.pypa.io/) or [uv](https://docs.astral.sh/uv/)
- Docker (optional, for PostgreSQL backend)

## License

MIT — See [LICENSE](LICENSE)
