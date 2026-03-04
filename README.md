# Sift

Reliability gateway for AI tool output: schema-stable, secret-safe, pagination-complete JSON.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Sift is a drop-in reliability layer for MCP and CLI tool output. It persists full payloads as artifacts, returns either inline payload (`full`) or compact references (`schema_ref`), and lets agents query what they need with Python code over stored data.

Benchmark summary: on 103 factual questions across 12 real JSON datasets, Sift improved accuracy from 33.0% to 99.0% while cutting input tokens by 95.4% (10,757,230 -> 489,655). Full details: [benchmarks/README.md](benchmarks/README.md).

## How it works

```
                           ┌─────────────────────┐
  MCP tool call ──────────▶│                     │──────────▶ Upstream MCP server
  CLI command   ──────────▶│        Sift         │──────────▶ Shell/API command
                           │                     │
                           │   ┌─────────────┐   │
                           │   │  Artifacts  │   │
                           │   │  (SQLite)   │   │
                           │   └─────────────┘   │
                           └─────────────────────┘
                                     │
                                     ▼
                         Small output -> `full` inline
                         Large output -> `schema_ref`
                         Agent queries artifacts with code
```

Flow:

1. Execute upstream tool/command and capture JSON.
2. Persist full output as an artifact in SQLite and deterministically map schema/root hints.
3. Return `full` (small) or `schema_ref` (large/paginated).
4. Continue pages explicitly until `pagination.retrieval_status == COMPLETE`.
5. Run focused Python queries on one artifact or the full pagination chain.

## Main MCP pain points

These are recurring across MCP client issue trackers and protocol usage in production:

- Large tool definitions and large tool results consume context quickly.
- Upstream API pagination often sits outside MCP list-cursor flows, so agents can stop early and answer on partial data.
- Tool output shape differs across servers, which makes follow-up parsing brittle.
- Tool output is untrusted input and can contain sensitive values that should not re-enter model context.
- Raw outputs scroll away in chat history, so provenance and reproducibility degrade across multi-step runs.

Background and references: [docs/why.md](docs/why.md).
Protocol references: [MCP pagination utility](https://modelcontextprotocol.io/specification/2025-06-18/basic/utilities/pagination), [MCP tools and security considerations](https://modelcontextprotocol.io/specification/2025-06-18/server/tools).

## What Sift adds (without changing upstream servers)

- Artifact-backed outputs: keep full data out of prompt context while preserving it losslessly.
- Schema-aware references: `schema_ref` returns query guidance for stable follow-up analysis.
- Exact structured retrieval: run Python against stored artifacts instead of relying on prompt-sized payloads.
- Explicit pagination contract: continue with `artifact(action="next_page")` or `run --continue-from`.
- Completion signaling: do not stop until `pagination.retrieval_status == COMPLETE`.
- Pagination-chain analysis: query one artifact or all related pages (`scope="all_related"`; CLI default).
- Outbound secret redaction enabled by default before output returns to the model.

## MCP vs CLI positioning

- MCP: Sift is a reliability gateway for mirrored tool calls and artifact-based follow-up queries.
- CLI/OpenClaw: same artifact contract for command output (`sift-gateway run` + `sift-gateway code`).
- CLI pitfall: ad-hoc extraction can silently scope analysis to partial data (for example, inspecting only one row).
- CLI note: for one-off local extraction, plain `jq` can be enough. Sift is for repeatable, pagination-complete, policy-controlled workflows.

## 60-second quickstart

### MCP clients

```bash
pipx install sift-gateway
sift-gateway init --from claude
```

Restart your MCP client, then use mirrored tools normally.

Supported `--from` shortcuts: `claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`, or an explicit config path.

### CLI flow

```bash
# 1) Capture JSON output as an artifact
sift-gateway run --json -- kubectl get pods -A -o json

# 2) Query artifact data with Python
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return {'rows': len(data)}"
```

Use `$` when rows are at root. If nested, use `metadata.usage.root_path` from `run --json` (or `metadata.queryable_roots` in MCP `schema_ref`).

### Pagination continuation

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next-params-applied>
```

Do not claim completion until `pagination.retrieval_status == COMPLETE`.

### Python codegen over all pages

For complex questions, generate Python once and run it over the entire pagination chain:

```bash
sift-gateway code --json --scope all_related <artifact_id> '$' --file ./analysis.py
```

CLI default is `--scope all_related`. Use `--scope single` for anchor-only analysis.

## Benchmarks

Tier 1 result (`claude-sonnet-4-6`):

| Condition | Accuracy | Input Tokens |
|---|---|---|
| Baseline (context-stuffed) | 34/103 (33.0%) | 10,757,230 |
| Sift | 102/103 (99.0%) | 489,655 |

That is +66.0 points accuracy with 95.4% fewer input tokens on the same question set.

Methodology, scripts, and Tier 2 autonomous-agent results: [benchmarks/README.md](benchmarks/README.md).

## Documentation library

Start here: [docs/README.md](docs/README.md)

### Getting started

- [Quick Start](docs/quickstart.md)
- [Installation](docs/quickstart.md#installation)
- [Your first artifact (CLI)](docs/quickstart.md#your-first-artifact-cli)
- [Your first artifact (MCP)](docs/quickstart.md#your-first-artifact-mcp)
- [Adding MCP servers after initial setup](docs/quickstart.md#adding-mcp-servers-after-initial-setup)
- [Troubleshooting](docs/quickstart.md#troubleshooting)

### Core contracts

- [API Contracts](docs/api_contracts.md)
- [Mirrored Response Contract (`full` vs `schema_ref`)](docs/api_contracts.md#mirrored-response-contract)
- [Response Mode Selection](docs/api_contracts.md#response-mode-selection)
- [Pagination Metadata](docs/api_contracts.md#pagination-metadata)
- [Code Query Contract](docs/api_contracts.md#artifactactionquery-query_kindcode)
- [CLI output contract](docs/api_contracts.md#cli-output-contract)
- [CLI default scope (`all_related`)](docs/api_contracts.md#sift-gateway-code-scope-default)

### Operations and security

- [Deployment Guide](docs/deployment.md)
- [Authentication tokens](docs/deployment.md#authentication-tokens)
- [Outbound secret redaction](docs/deployment.md#outbound-secret-redaction)
- [Configuration Reference](docs/config.md)
- [Code query runtime](docs/config.md#code-query-runtime)
- [Error Contract](docs/errors.md)
- [Security policy](SECURITY.md)

### Patterns and deep dives

- [Recipes](docs/recipes.md)
- [Pagination chain (CLI)](docs/recipes.md#pattern-2-upstream-pagination-chain-cli)
- [Pagination chain (MCP)](docs/recipes.md#pattern-3-upstream-pagination-chain-mcp)
- [Architecture](docs/architecture.md)
- [Pagination model](docs/architecture.md#5-pagination-model)
- [Observability](docs/observability.md)
- [Why Sift exists](docs/why.md)
- [OpenClaw integration pack](docs/openclaw/README.md)
- [Upstream registration design](docs/upstream_registration.md)

## Security

See [SECURITY.md](SECURITY.md) for threat model and hardening guidance.

## Development

```bash
git clone https://github.com/lourencomaciel/sift-gateway.git
cd sift-gateway
uv sync --extra dev
uv run python -m pytest tests/unit/ -q
```

Full contributor workflow: [CONTRIBUTING.md](CONTRIBUTING.md)

## License

MIT - see [LICENSE](LICENSE).
