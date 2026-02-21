---
name: context-query-guard
description: Capture large or paginated command output (gh api, curl, kubectl, logs) as artifacts and query them with Python instead of flooding context
metadata: {"openclaw":{"requires":{"bins":["sift-gateway"]},"install":[{"id":"uv","kind":"uv","package":"sift-gateway","bins":["sift-gateway"],"label":"Install Sift Gateway (uv)"}]}}
---

# Context Query Guard

Capture large command output as artifacts, keep it out of the context window, then query it for compact answers.

## Activation rules

- Use for: API list calls (`gh api`, `curl`, `kubectl ... -o json`), large logs, long tables, repeated analysis.
- Skip for: one-off output that is clearly small and does not need follow-up querying.

## Default playbook

- Start with a single-artifact query and an explicit `root_path`.
- Use `run(data, schema, params)` first; only switch to multi-artifact when you must combine artifacts.
- Use pure Python first; do not assume `pandas` is installed.
- Keep outputs compact (aggregates or top <= 20 rows).
- Prefer `--scope single`; use pagination-chain expansion (`scope=all_related`) only when cross-artifact analysis is required.

## Core flow

1. Capture data:

```bash
sift-gateway run --json -- <command>
```

2. Keep only `artifact_id` and a short summary in model context.
3. If pagination exists (`pagination.next.kind=="command"`), continue explicitly:

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Query with narrow code outputs:

```bash
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return [{'id': row.get('id'), 'status': row.get('status')} for row in data[:20]]"
```

## Output shape

`sift-gateway run --json` returns a JSON object with:

- `artifact_id`: use this for follow-up queries.
- `response_mode`: `"full"` or `"schema_ref"`.
- `records`: captured item estimate.
- `pagination.next.kind`: if `"command"`, run a continuation capture.
- `payload`: present in `"full"` mode.
- `schemas`: present in `"schema_ref"` fallback mode when sample preview is not available.
- `sample_item`: present in `"schema_ref"` when the first item is representative.

`sift-gateway code --json` returns:

- `response_mode: "full"`: query output in `payload` and normalized rows in `items`.
- `response_mode: "schema_ref"`: either representative sample preview (`sample_item`) or verbose `schemas`.

## Query methods

`sift-gateway code` accepts:

- `--code "<python_source>"` for inline `run(data, schema, params)` (single artifact) or `run(artifacts, schemas, params)` (multi-artifact).
- `--file <path.py>` for file-based `run(...)`.
- `--params '<json_object>'` to pass runtime parameters.
- `--scope single` to disable pagination-chain expansion.
- multi-artifact mode with repeated `--artifact-id` and required `--root-path` (one shared root path or one per artifact).

```bash
# inline function mode (single artifact signature)
sift-gateway code --json <artifact_id> '$.items' \
  --code "def run(data, schema, params): return {'rows': len(data), 'tag': params.get('tag')}" \
  --params '{"tag":"daily"}'

# multi-artifact inline mode (multi-artifact signature)
sift-gateway code --json --artifact-id art_users --artifact-id art_orders --root-path '$.items' \
  --code "def run(artifacts, schemas, params): return {k: len(v) for k, v in artifacts.items()}"
```

## Command reference

| Command | Purpose |
| --- | --- |
| `sift-gateway run --json -- <cmd>` | Capture command output as artifact and return machine-readable metadata |
| `sift-gateway run --json --continue-from <id> -- <cmd>` | Capture next upstream page and link lineage |
| `sift-gateway code --json ...` | Run sandboxed Python over one or multiple artifact roots |

## High-signal capture patterns

```bash
# GitHub PRs
sift-gateway run --json -- gh api repos/org/repo/pulls

# Kubernetes inventory
sift-gateway run --json --tag k8s -- kubectl get pods -A -o json

# API dump with retention control
sift-gateway run --json --ttl 8h --tag events -- curl -s https://api.example.com/events
```

## Guardrails

- Never paste raw captured payloads back into context; keep `artifact_id` plus compact findings.
- Use explicit continuation commands for partial pagination; do not assume auto-follow.
- When `response_mode` is `"schema_ref"`, use `sample_item` first; if absent, use `schemas` field paths/types/examples before writing code queries.
- Return focused results (counts, grouped stats, selected columns), not full records.
- Treat each `run` capture as a fresh artifact; do not assume implicit dedupe.
