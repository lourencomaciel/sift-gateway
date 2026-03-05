---
name: context-query-guard
description: Capture large or paginated command output as artifacts and analyze it with compact reproducible code queries.
homepage: https://github.com/lourencomaciel/sift-gateway/tree/main/docs/openclaw
metadata: {"openclaw":{"skillKey":"sift-gateway-context-query-guard","homepage":"https://github.com/lourencomaciel/sift-gateway/tree/main/docs/openclaw","requires":{"bins":["sift-gateway"]},"install":[{"id":"uv","kind":"uv","package":"sift-gateway","bins":["sift-gateway"],"label":"Install Sift Gateway (uv)"}]}}
---

# Context Query Guard

Use this skill when output can overflow context or requires reproducible
analysis. Capture once, then query artifacts with explicit schema/root handling.

## Trigger

Use for:

- API list calls (`gh api`, `curl`, `kubectl ... -o json`)
- large logs or tables
- multi-step analysis across turns
- workflows needing reproducibility, pagination continuity, or auditability

Skip for clearly small one-off output with no follow-up.

## Workflow

1. Capture output:

```bash
sift-gateway run --json -- <command>
```

2. Read the capture envelope before querying:
   - `response_mode`
   - `metadata.usage.root_path`
   - `sample_item` (when present)
   - `schemas` (when present)
   - `pagination.next.kind`
3. Keep only `artifact_id` plus a short summary in prompt context.
4. If pagination is partial (`pagination.next.kind=="command"`), continue
   explicitly:

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

5. Query with explicit root path and compact output:

```bash
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return [{'id': row.get('id'), 'status': row.get('status')} for row in data[:20]]"
```

Use `metadata.usage.root_path` from `run --json` when `$` is not correct.

## Schema Discovery Protocol

- Treat `metadata.usage.root_path` as the default root-path source of truth.
- If `response_mode=="schema_ref"`, use `sample_item` first, then `schemas`.
- If shape is uncertain, run a compact probe query before analysis.

```bash
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): keys=set(); [keys.update(r.keys()) for r in data[:20] if isinstance(r, dict)]; return {'rows': len(data), 'sampled': min(len(data), 20), 'keys': sorted(keys)[:50]}"
```

Never infer schema from first-item `jq` heuristics (for example `.[0]`):
first rows are often sparse or non-representative, which causes wrong key/root
assumptions and bad downstream queries.

## Guardrails

- Prefer `--scope single` first (`sift-gateway code` defaults to
  `--scope all_related`).
- Start with `run(data, schema, params)`; move to multi-artifact only when
  combining artifacts is required.
- Use pure Python first; do not assume optional packages are installed.
- Return aggregates or top <= 20 rows; avoid full-record dumps.
- Never paste raw captured payloads back into context.

## Completion Check

- Do not claim a paginated result is complete while
  `pagination.next.kind=="command"` remains.
- If only the anchor artifact was queried, state that scope explicitly.
