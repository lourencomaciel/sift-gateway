---
name: sift
description: Store and query large command outputs without context bloat
requirements:
  - sift-gateway
---

# Sift - Large Output Handler

Use Sift when output might exceed about 4KB, or when the same data will be referenced more than once.

## When to Use Sift

- API list calls: `gh api`, `curl`, `kubectl ... -o json`
- Large query results or long logs
- Multi-step workflows that reuse the same dataset

## Core Workflow

1. Capture output:

```bash
sift run -- <command>
```

2. Read summary and keep only the artifact id in context.
3. Query only needed slices:

```bash
sift query <artifact_id> '$' --limit 20
```

4. Narrow fields/rows:

```bash
sift query <artifact_id> '$.items' --where '{"path":"$.state","op":"eq","value":"open"}' --select "id,title"
```

5. Run Python analysis when selection logic is not enough:

```bash
sift code <artifact_id> '$.items' --expr "df.groupby('owner').size().to_dict()"
```

### Code Methods (`sift code`)

`sift code` accepts three code input modes:

- `--expr "<python_expr>"` for quick DataFrame expressions (`df` is preloaded).
- `--code "<python_source>"` for inline source defining `run(data, schema, params)`.
- `--file <path.py>` to load source from a file defining `run(data, schema, params)`.

Pass runtime parameters with `--params '<json_object>'`; the object is available as
`params` inside `run`.

```bash
# Quick expression mode
sift code <artifact_id> '$.items' --expr "df['status'].value_counts().to_dict()"

# Inline function mode
sift code <artifact_id> '$.items' \
  --code "def run(data, schema, params): return {'rows': len(data), 'tag': params.get('tag')}" \
  --params '{"tag":"daily"}'

# File mode
sift code <artifact_id> '$.items' --file ./analysis.py --params '{"owner":"alice"}'
```

## Commands

| Command | Purpose |
| --- | --- |
| `sift run -- <cmd>` | Capture command output as an artifact |
| `sift run --stdin` | Capture piped stdin |
| `sift query <id> <root_path>` | Filter/project rows from mapped roots |
| `sift code <id> <root_path>` | Run sandboxed Python over root data |
| `sift schema <id>` | Inspect structure before querying |
| `sift list` | List recent artifacts |
| `sift get <id>` | Retrieve envelope or mapped payload |
| `sift diff <id1> <id2>` | Compare artifacts |

## High-Signal Patterns

```bash
# GitHub PRs
sift run -- gh api repos/org/repo/pulls

# Kubernetes inventory
kubectl get pods -A -o json | sift run --stdin --tag k8s

# API dump with retention control
curl -s https://api.example.com/events | sift run --stdin --ttl 8h --tag events

# Python aggregation with DataFrame convenience
sift code art_abc123 '$.items' --expr "df['status'].value_counts().to_dict()"
```

## Context Budget Rules

- Keep raw output out of context; keep artifact IDs and small summaries.
- Query in pages (`--limit`) and only required fields (`--select`).
- Use `sift code` for complex aggregation/join logic.
- Repeated commands always produce fresh captures; compare runs with `sift diff`.
