# Sift

Reliability gateway for AI tool output: schema-stable, secret-safe, pagination-complete JSON.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Sift sits between agents and upstream tools, stores full outputs as artifacts, and returns either inline payload (`full`) or artifact references (`schema_ref`) with query guidance.

## Why it exists

Agent sessions fail on large JSON for the same reasons:

- hidden truncation and incomplete pagination
- unstable follow-up logic when payload shape shifts
- secret leakage back into model context
- no reproducible lineage across pages and derived queries

Sift addresses those with artifact-backed queries, redaction, and explicit continuation semantics.

## 60-second quickstart

### MCP clients

```bash
pipx install sift-gateway
sift-gateway init --from claude
```

Restart your MCP client, then use mirrored tools normally. Sift will persist responses and surface queryable artifacts.

### CLI flow

```bash
# 1) capture
sift-gateway run --json -- kubectl get pods -A -o json

# 2) query
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return {'rows': len(data)}"
```

Use `$` when rows are at the root. If data is nested, use `metadata.usage.root_path` from `run --json` (or `metadata.queryable_roots` in MCP `schema_ref`).

### Pagination continuation

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next-params-applied>
```

Do not claim completion until `pagination.retrieval_status == COMPLETE`.

## Benchmarks

On the Tier 1 suite (103 factual questions across real datasets), Sift improved answer reliability while reducing context load:

| Model | Condition | Accuracy | Input Tokens |
|---|---|---|---|
| claude-sonnet-4-6 | Baseline (context-stuffed) | 34/103 (33.0%) | 10,757,230 |
| claude-sonnet-4-6 | Sift | 102/103 (99.0%) | 489,655 |

Full details: [benchmarks/README.md](benchmarks/README.md)

## Docs for launch

Start here: [docs/README.md](docs/README.md)

- [Quick Start](docs/quickstart.md) for install + first success path
- [API Contracts](docs/api_contracts.md) for canonical MCP/CLI contract
- [Deployment](docs/deployment.md) + [Configuration](docs/config.md) for operators
- [Errors](docs/errors.md) for troubleshooting

Advanced/optional:

- [Recipes](docs/recipes.md)
- [Architecture](docs/architecture.md)
- [Observability](docs/observability.md)
- [OpenClaw Pack](docs/openclaw/README.md)
- [Upstream Registration](docs/upstream_registration.md)
- [Why Sift Exists](docs/why.md)

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
