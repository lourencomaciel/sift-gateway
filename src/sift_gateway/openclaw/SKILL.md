---
name: context-query-guard
description: Capture large or paginated command output as artifacts and analyze it with compact reproducible code queries.
homepage: https://github.com/lourencomaciel/sift-gateway/tree/main/docs/openclaw
metadata: {"openclaw":{"skillKey":"sift-gateway-context-query-guard","homepage":"https://github.com/lourencomaciel/sift-gateway/tree/main/docs/openclaw","requires":{"bins":["sift-gateway"]},"install":[{"id":"uv","kind":"uv","package":"sift-gateway","bins":["sift-gateway"],"label":"Install Sift Gateway (uv)"}]}}
---

# Context Query Guard

Use this skill when command output is large, paginated, or likely to be reused.
Capture once, query by schema, and return compact answers without pasting raw
payloads into context.

## When to use

- API list calls (`gh api`, `curl`, `kubectl ... -o json`)
- Large logs or tables
- Follow-up analysis across multiple turns
- Workflows that need reproducibility, redaction discipline, or auditability

Skip for trivial one-off output that is clearly small.

## Core workflow

1. Capture output as an artifact:

```bash
sift-gateway run --json -- <command>
```

2. Keep only `artifact_id` plus a short summary in prompt context.
3. If pagination is partial (`pagination.next.kind=="command"`), continue
   explicitly:

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Query with explicit root path and compact output:

```bash
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return [{'id': row.get('id'), 'status': row.get('status')} for row in data[:20]]"
```

Use `metadata.usage.root_path` from `run --json` if `$` is not correct.

## Guardrails

- Prefer `--scope single` for initial queries (`sift-gateway code` defaults to
  `--scope all_related`).
- Start with `run(data, schema, params)`; move to multi-artifact only when
  combining artifacts is required.
- Use pure Python first; do not assume optional packages are installed.
- If `response_mode` is `schema_ref`, inspect `sample_item` first, then
  `schemas`.
- Return aggregates or top <= 20 rows; avoid full record dumps.
- Never paste raw captured payloads back into context.
