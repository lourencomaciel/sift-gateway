# Contract V1: Run, Next Page, Code

This document defines the hard-cut agent contract for Sift with only:

- `run`
- `next_page`
- `code`

All behavior here applies to both MCP and CLI paths unless explicitly noted.

## 1. Scope

### MCP surface

- Mirrored upstream tool calls (run semantic).
- `artifact(action="next_page", artifact_id=...)`.
- `artifact(action="query", query_kind="code", ...)`.

### CLI surface

- `sift-gateway run -- <command>`.
- `sift-gateway run --continue-from <artifact_id> -- <next-command>` (next-page semantic).
- `sift-gateway code ...`.

## 2. Processing Pipeline

The gateway MUST run this pipeline for run/code/page continuations:

1. Execute upstream tool/command.
2. Parse JSON result.
3. Detect pagination from the raw parsed result.
4. Redact sensitive values.
5. Persist artifact.
6. Map artifact.
7. Build compact schema and legend.
8. Compute response sizes.
9. Choose response mode.
10. Return response.

Important:

- Pagination detection happens before redaction to avoid losing cursor signals.
- Persisted artifacts store redacted content.

## 3. Response Modes

Two response modes are allowed:

- `full`: inline payload is returned.
- `schema_ref`: artifact reference and schema are returned.

All machine-readable JSON responses MUST be minified when serialized
(no pretty-print whitespace between keys/values).

### Mode payloads

`full` includes inline payload data.

`schema_ref` includes:

- `artifact_id`
- `schemas_compact`
- `schema_legend`

No verbose `schemas` object is part of this contract.

## 4. Mode Selection Rules

Let:

- `has_pagination`: pagination exists for this response.
- `full_bytes`: serialized byte size of full-mode response.
- `schema_ref_bytes`: serialized byte size of schema-ref response.
- `max_bytes`: hard inline response cap.

Decision function:

```text
if has_pagination:
  mode = "schema_ref"
else:
  if full_bytes > max_bytes:
    mode = "schema_ref"
  elif schema_ref_bytes * 2 <= full_bytes:
    mode = "schema_ref"
  else:
    mode = "full"
```

Interpretation:

- Pagination always forces `schema_ref`.
- Non-paginated responses still respect hard cap.
- Under cap, choose `schema_ref` only if it is at least 50% smaller.

## 5. Pagination Semantics

- If pagination exists, mode is always `schema_ref`.
- Do not switch mode mid-chain.
- Each continued page is linked via:
  - `parent_artifact_id`
  - `chain_seq`

### Continuation APIs

- MCP continuation: `artifact(action="next_page", artifact_id=...)`.
- CLI continuation: `run --continue-from <artifact_id> -- <next-command>`.

## 6. Required Metadata in Every Response

The response MUST always include:

- `response_mode`: `"full"` or `"schema_ref"`
- `artifact_id`
- `lineage` (at minimum `scope`, and artifact linkage info)
- `pagination` (when present)

## 7. Code Behavior

- Code uses the same mode-selection policy as run responses.
- Code executes against persisted redacted artifacts.
- Scope rules:
  - `scope=all_related`: execute against lineage set.
  - `scope=single`: execute against anchor artifact only.

Code outputs are persisted as derived artifacts regardless of returned mode.

## 8. Size Measurement Guidance

To avoid large serialization overhead:

- Use a capped byte estimator.
- Stop counting once `max_bytes + 1` is reached when only cap decision is required.
- Use exact serialization only when needed for `schema_ref_bytes * 2 <= full_bytes` decision.

## 9. Examples

### 9.1 Schema-ref response

```json
{
  "response_mode": "schema_ref",
  "artifact_id": "art_123",
  "schemas_compact": [
    {
      "rp": "$",
      "f": [
        {
          "p": "$.id",
          "t": [
            "number"
          ]
        }
      ]
    }
  ],
  "schema_legend": {
    "schema": {
      "rp": "root_path"
    },
    "field": {
      "p": "path",
      "t": "types"
    }
  },
  "lineage": {
    "scope": "single",
    "artifact_ids": [
      "art_123"
    ]
  },
  "pagination": {
    "has_more": true
  }
}
```

### 9.2 Full response (non-paginated)

```json
{
  "response_mode": "full",
  "artifact_id": "art_456",
  "payload": {
    "status": "ok",
    "count": 2
  },
  "lineage": {
    "scope": "single",
    "artifact_ids": [
      "art_456"
    ]
  }
}
```

### 9.3 CLI run example

```bash
sift-gateway run --json -- curl -sS 'https://api.example.com/items?page=1'
```

If paginated, response mode is `schema_ref` and continuation uses:

```bash
sift-gateway run --continue-from art_123 --json -- curl -sS 'https://api.example.com/items?page=2'
```

### 9.4 MCP next-page example

```json
{
  "tool": "artifact",
  "arguments": {
    "action": "next_page",
    "artifact_id": "art_123"
  }
}
```

## 10. Non-Goals for V1

- Backward compatibility fields for older response formats.
- Auto-pagination loops.
- Passthrough mode that bypasses artifact-centric responses.
- Verbose schema payloads in public run/code responses.
