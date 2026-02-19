---
name: sift-gateway
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
sift-gateway run -- <command>
```

2. Read summary and keep only the artifact id in context.
3. If pagination is available (`pagination.has_next_page=true`), continue manually:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

Use `pagination.next_params` from the previous result to set continuation args
(`after`, `cursor`, `page_token`, etc.) on `<next-command-with-next_params-applied>`.

4. Query only needed slices:

```bash
sift-gateway query <artifact_id> '$' --limit 20
```

5. Narrow fields/rows:

```bash
sift-gateway query <artifact_id> '$.items' --where '{"path":"$.state","op":"eq","value":"open"}' --select "id,title"
```

6. Run Python analysis when selection logic is not enough:

```bash
sift-gateway code <artifact_id> '$.items' --expr "df.groupby('owner').size().to_dict()"
```

### Code Methods (`sift-gateway code`)

`sift-gateway code` accepts three code input modes:

- `--expr "<python_expr>"` for quick DataFrame expressions (`df` is preloaded).
- `--code "<python_source>"` for inline source defining `run(data, schema, params)`.
- `--file <path.py>` to load source from a file defining `run(data, schema, params)`.
- `--scope single` to restrict processing to the anchor artifact(s) only.
- Multi-artifact input:
  - shared root path: repeat `--artifact-id` and provide one `--root-path`.
  - per-artifact root paths: repeat both `--artifact-id` and `--root-path` in matching order.

Pass runtime parameters with `--params '<json_object>'`; the object is available as
`params` inside `run`.

```bash
# Quick expression mode
sift-gateway code <artifact_id> '$.items' --expr "df['status'].value_counts().to_dict()"
sift-gateway code <artifact_id> '$.items' --scope single --expr "df['status'].value_counts().to_dict()"

# Inline function mode
sift-gateway code <artifact_id> '$.items' \
  --code "def run(data, schema, params): return {'rows': len(data), 'tag': params.get('tag')}" \
  --params '{"tag":"daily"}'

# File mode
sift-gateway code <artifact_id> '$.items' --file ./analysis.py --params '{"owner":"alice"}'

# Multi-artifact mode (shared root path)
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.items' --expr "len(df)"

# Multi-artifact mode (per-artifact root paths)
sift-gateway code --artifact-id art_users --artifact-id art_orders --root-path '$.users' --root-path '$.orders' --file ./join.py
```

## Commands

| Command | Purpose |
| --- | --- |
| `sift-gateway run -- <cmd>` | Capture command output as an artifact |
| `sift-gateway run --continue-from <id> -- <cmd>` | Capture next upstream page and link lineage |
| `sift-gateway run --stdin` | Capture piped stdin |
| `sift-gateway query <id> <root_path>` | Filter/project rows from mapped roots |
| `sift-gateway code <id> <root_path>` or `sift-gateway code --artifact-id ... --root-path ...` | Run sandboxed Python over one or multiple artifact roots |
| `sift-gateway schema <id>` | Inspect structure before querying |
| `sift-gateway list` | List recent artifacts |
| `sift-gateway get <id>` | Retrieve envelope or mapped payload |
| `sift-gateway diff <id1> <id2>` | Compare artifacts |

## High-Signal Patterns

```bash
# GitHub PRs
sift-gateway run -- gh api repos/org/repo/pulls

# GitHub PRs next page (example)
sift-gateway run --continue-from art_page_1 -- gh api repos/org/repo/pulls --after CUR_2 --limit 100

# Kubernetes inventory
kubectl get pods -A -o json | sift-gateway run --stdin --tag k8s

# API dump with retention control
curl -s https://api.example.com/events | sift-gateway run --stdin --ttl 8h --tag events

# Python aggregation with DataFrame convenience
sift-gateway code art_abc123 '$.items' --expr "df['status'].value_counts().to_dict()"
```

## Context Budget Rules

- Keep raw output out of context; keep artifact IDs and small summaries.
- For upstream pagination, keep `next_params` and issue explicit `run --continue-from` calls.
- Query in pages (`--limit`) and only required fields (`--select`).
- Use `sift-gateway code` for complex aggregation/join logic.
- Repeated commands always produce fresh captures; compare runs with `sift-gateway diff`.
