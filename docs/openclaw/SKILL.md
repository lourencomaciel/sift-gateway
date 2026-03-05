---
name: context-query-guard
description: Capture large or paginated command output as artifacts and analyze it with compact reproducible code queries.
homepage: https://github.com/lourencomaciel/sift-gateway/tree/main/docs/openclaw
metadata: {"openclaw":{"skillKey":"sift-gateway-context-query-guard","homepage":"https://github.com/lourencomaciel/sift-gateway/tree/main/docs/openclaw","requires":{"bins":["sift-gateway"]},"install":[{"id":"uv","kind":"uv","package":"sift-gateway","bins":["sift-gateway"],"label":"Install Sift Gateway (uv)"}]}}
---

# Context Query Guard

Use this skill whenever command output will be analyzed by the model and
correctness matters. Capture once, query from artifacts, and return compact
answers without copying raw payloads into model context.

## Trigger

- API list calls (`gh api`, `curl`, `kubectl ... -o json`)
- Large logs or tables
- JSON payloads with uncertain shape/root path or heterogeneous rows (even when
  small)
- Follow-up analysis across multiple turns
- Workflows that need reproducibility, redaction discipline, or auditability

Skip only for trivial one-off, human-eyeball checks with clearly small output
and no follow-up model reasoning.

## Required workflow

1. Capture output as an artifact:

```bash
sift-gateway run --json -- <command>
```

2. Keep only `artifact_id` plus a short summary in prompt context.
3. Handle pagination explicitly only when present. If
   `pagination.next.kind=="command"`, continue with:

```bash
sift-gateway run --json --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Query artifacts with explicit root path and compact output:

```bash
sift-gateway code --json <artifact_id> '$' --code "def run(data, schema, params): return [{'id': row.get('id'), 'status': row.get('status')} for row in data[:20]]"
```

5. Resolve root path from response hints, not guesswork:
- Current `run` behavior uses canonical root path `$`; use `$` for follow-up
  code queries.
- If `response_mode=="schema_ref"` and `schemas` are present, use schema
  `root_path` as the source of truth.
- Treat `sample_item` as a preview row only.

## Schema discovery protocol

- Do not use `jq '.[0]'` (or equivalent "first-item" shortcuts) to infer schema
  or root path. Many payloads are object-wrapped, have multiple candidate roots,
  or include heterogeneous rows where first-item heuristics are misleading.
- In Sift responses, `sample_item` is emitted only when Sift can verify
  consistent item shape across the resolved list. If `sample_item` is absent,
  inspect `schemas`; for current `run` captures, schema `root_path` should be
  `$`.
- If `sample_item_text_truncated` is true, treat long text fields as truncated
  previews and confirm details with a focused code query.

## Guardrails

- `sift-gateway code` defaults to `--scope all_related`; start with
  `--scope single` for anchor-only analysis and widen scope only when needed.
- Start with `run(data, schema, params)`; move to `run(artifacts, schemas,
  params)` only when cross-artifact joins are required.
- Use pure Python first; do not assume optional packages are available.
- Return aggregates or top <= 20 rows; avoid full-record dumps.
- Never paste raw captured payloads back into context.
- Do not claim completeness until `pagination.retrieval_status == COMPLETE`.
