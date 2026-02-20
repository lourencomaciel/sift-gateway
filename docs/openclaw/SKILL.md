---
name: sift-gateway
description: Store and analyze large command outputs without context bloat
requirements:
  - sift-gateway
---

# Sift - Large Output Handler

Use Sift when output may exceed ~4KB or when data will be reused.

## When to use Sift

- API list calls: `gh api`, `curl`, `kubectl ... -o json`
- large logs or long tabular output
- multi-step tasks where the same dataset is reused

## Core workflow

1. Capture output:

```bash
sift-gateway run -- <command>
```

2. Keep `artifact_id` and short summary in context.
3. If pagination exists (`pagination.next.kind=="command"`), continue explicitly:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Run focused analysis with code:

```bash
sift-gateway code <artifact_id> '$' --expr "df.head(5).to_dict('records')"
```

## Code methods

`sift-gateway code` accepts:

- `--expr "<python_expr>"` for quick DataFrame expressions.
- `--code "<python_source>"` for inline `run(data, schema, params)`.
- `--file <path.py>` for file-based `run(...)`.
- `--params '<json_object>'` to pass runtime parameters.
- `--scope single` to disable lineage expansion.
- multi-artifact mode with repeated `--artifact-id` and optional `--root-path` mapping.

```bash
# quick expression
sift-gateway code <artifact_id> '$.items' --expr "df['status'].value_counts().to_dict()"

# inline function mode
sift-gateway code <artifact_id> '$.items' \
  --code "def run(data, schema, params): return {'rows': len(data), 'tag': params.get('tag')}" \
  --params '{"tag":"daily"}'

# file mode
sift-gateway code <artifact_id> '$.items' --file ./analysis.py --params '{"owner":"alice"}'

# multi-artifact mode
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.items' --expr "len(df)"
```

## Commands

| Command | Purpose |
| --- | --- |
| `sift-gateway run -- <cmd>` | Capture command output as artifact |
| `sift-gateway run --continue-from <id> -- <cmd>` | Capture next upstream page and link lineage |
| `sift-gateway run --stdin` | Capture piped stdin |
| `sift-gateway code ...` | Run sandboxed Python over one or multiple artifact roots |

## High-signal patterns

```bash
# GitHub PRs
sift-gateway run -- gh api repos/org/repo/pulls

# Kubernetes inventory
kubectl get pods -A -o json | sift-gateway run --stdin --tag k8s

# API dump with retention control
curl -s https://api.example.com/events | sift-gateway run --stdin --ttl 8h --tag events
```

## Context budget rules

- keep raw output out of context; keep `artifact_id` plus compact summary
- for paginated APIs, keep `next_params` and issue explicit continuation
- prefer narrow code outputs (counts, projections, aggregates)
- each run capture is fresh by design; do not assume implicit dedupe
