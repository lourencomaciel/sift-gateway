# API Contracts

Canonical behavior for the consolidated `artifact` tool and related pagination,
cache, and error surfaces.

## Artifact Query Entrypoint

Use the consolidated `artifact` tool:

- `action="query"` for retrieval/search operations
- `action="next_page"` for upstream pagination continuation

For `action="query"`, `query_kind` is required and must be one of:
`describe`, `get`, `select`, `search`, `code`.

Argument routing rules:

- `query_kind="describe"|"get"|"select"` requires `artifact_id`
- `query_kind="code"` requires `artifact_id` (single) or `artifact_ids` (multi)
- `query_kind="search"` rejects `artifact_id` and `scope`
- `scope` applies to `describe|get|select`; code queries always run all-related

## Pagination Contract

Sift uses layer-explicit pagination metadata.

### Layers

- `pagination.layer = "upstream"`
  Used by mirrored upstream tool responses. Continue with
  `artifact(action="next_page", artifact_id=...)`.
- `pagination.layer = "artifact_retrieval"`
  Used by retrieval/search responses. Continue with
  `artifact(action="query", query_kind=..., cursor=...)`.

### Retrieval Pagination Fields

For `pagination.layer = "artifact_retrieval"`:

- `retrieval_status`: `PARTIAL` or `COMPLETE`
- `partial_reason`: `CURSOR_AVAILABLE` or `null`
- `has_more`: boolean
- `next_cursor`: opaque cursor string when `has_more=true`, else `null`
- `hint`: human-readable continuation guidance

Compatibility field:

- `cursor` may also be present at top level in select/get/search responses.

Completion rule:

- Do not claim completeness until
  `pagination.retrieval_status == "COMPLETE"`.

### Upstream Pagination Fields

For `pagination.layer = "upstream"`:

- `retrieval_status`: `PARTIAL` or `COMPLETE`
- `partial_reason`: one of
  `MORE_PAGES_AVAILABLE`, `SIGNAL_INCONCLUSIVE`, `CONFIG_MISSING`,
  `NEXT_TOKEN_MISSING`, or `null`
- `has_more`: boolean
- `next_action`: follow-up call shape for `artifact(action="next_page")`
- Back-compat: `has_next_page`, `hint`

## Gateway Context Controls

Mirrored tool calls must include `_gateway_context.session_id`.

Sift always creates a fresh artifact for mirrored upstream calls.

Handle responses include cache metadata under `meta.cache`:

- `request_key`
- `reason` (`fresh`)
- `artifact_id_origin` (`fresh`)

Session visibility guarantee:

- Created artifacts are attached to the caller session before returning the
  handle, so follow-up retrieval in the same session succeeds.

## Handle Response Contract

When mirrored responses exceed passthrough budget, Sift returns an artifact
handle.

Common fields:

- `artifact_id` (required)
- `schemas` (required)
- `usage_hint` (required)
- `meta.cache` (required)
- `mapping` (optional)
- `pagination` (optional; upstream-layer metadata)

## Query Response Shapes

`artifact(action="query")` has query-kind-specific response shapes.

### `query_kind="describe"`

Required:

- `artifact_id`, `scope`, `lineage`, `artifacts`, `roots`

Optional:

- `schema_legend`
- `schemas` (single-scope responses with schema data)

Not present by contract:

- `items`, `cursor`, retrieval `pagination`

### `query_kind="get"`

`target="envelope"` required fields:

- `artifact_id`, `scope`, `target`, `items`, `truncated`, `lineage`, `pagination`

Common optional fields:

- `cursor`, `omitted`, `stats`, `warnings`

`target="mapped"` required fields:

- `artifact_id`, `scope`, `target`, `roots`, `lineage`, `pagination`

Optional:

- `warnings`

### `query_kind="select"`

Required request args:

- `artifact_id`, `root_path`, `select_paths`

Optional request args:

- `where` — structured filter object (see [Filter syntax](#filter-syntax) below)
- `order_by`, `distinct`, `count_only`, `limit`, `cursor`, `scope`

Required response fields:

- `items`, `truncated`, `total_matched`, `scope`, `lineage`, `pagination`

Optional response fields:

- `cursor`, `omitted`, `stats`, `determinism`, `warnings`
- `sampled_only`, `sample_indices_used`, `sampled_prefix_len`

Special case:

- `count_only=true` returns a compact shape: `count`, `truncated=false`,
  and `pagination` (no `items`, `scope`, or `lineage` fields).

### `query_kind="code"`

Code responses are select-like and intentionally unpaginated.

Required:

- `items`, `truncated`, `total_matched`, `scope`, `lineage`, `pagination`,
  `determinism`

Optional:

- `stats`, `warnings`, `sampled_only`

Not present by contract:

- `cursor` (code queries are unpaginated)

Notes:

- Scalar/dict return values are normalized to a one-item list.
- Output is bounded by `max_bytes_out`.
- `limit`/`cursor` are not pagination controls for code queries.

### `query_kind="search"`

Required:

- `items`, `truncated`, `omitted`, `pagination`

Optional:

- `cursor`

## Example (`query_kind="select"`)

```json
{
  "items": [
    {
      "_locator": {"artifact_id": "art_1", "index": 0},
      "projection": {"$.name": "Alice", "$.spend": "42"}
    }
  ],
  "truncated": true,
  "cursor": "cur1....",
  "pagination": {
    "layer": "artifact_retrieval",
    "retrieval_status": "PARTIAL",
    "partial_reason": "CURSOR_AVAILABLE",
    "has_more": true,
    "next_cursor": "cur1....",
    "hint": "More results available. Resume with the cursor returned in this response."
  },
  "total_matched": 150,
  "scope": "all_related",
  "lineage": {
    "scope": "all_related",
    "anchor_artifact_id": "art_1",
    "artifact_count": 3,
    "artifact_ids": ["art_1", "art_2", "art_3"]
  }
}
```

## Filter Syntax

The `where` parameter for `query_kind="select"` accepts a structured filter
object. Filters compile to parameterized SQLite `json_extract` queries — no
custom DSL parsing required.

### Single predicate

```json
{"path": "$.status", "op": "eq", "value": "active"}
```

### Logical group

```json
{
  "logic": "and",
  "filters": [
    {"path": "$.spend", "op": "gte", "value": 100},
    {"path": "$.region", "op": "in", "value": ["US", "EU"]}
  ]
}
```

### Supported operators

| Operator | Description | Value |
|----------|-------------|-------|
| `eq` | Equal | scalar |
| `ne` | Not equal | scalar |
| `gt` | Greater than | scalar |
| `gte` | Greater than or equal | scalar |
| `lt` | Less than | scalar |
| `lte` | Less than or equal | scalar |
| `in` | Value in list | list |
| `contains` | Substring match (cast to text) | string |
| `array_contains` | JSON array element membership | scalar |
| `exists` | Field is present | _(ignored)_ |
| `not_exists` | Field is absent | _(ignored)_ |

### Negation

Wrap any filter or group with `"not"` to negate it:

```json
{"not": {"path": "$.status", "op": "eq", "value": "deleted"}}
```

Negation works with any filter type, including groups:

```json
{"not": {"logic": "or", "filters": [
  {"path": "$.status", "op": "eq", "value": "archived"},
  {"path": "$.status", "op": "eq", "value": "deleted"}
]}}
```

Negation can also appear inside a group:

```json
{
  "logic": "and",
  "filters": [
    {"path": "$.active", "op": "eq", "value": true},
    {"not": {"path": "$.name", "op": "in", "value": ["test", "demo"]}}
  ]
}
```

### Nesting

Groups and negations can be nested arbitrarily:

```json
{
  "logic": "or",
  "filters": [
    {"path": "$.status", "op": "eq", "value": "active"},
    {
      "logic": "and",
      "filters": [
        {"path": "$.status", "op": "eq", "value": "pending"},
        {"path": "$.priority", "op": "gte", "value": 5}
      ]
    }
  ]
}
```

## Error Contract

Gateway errors are returned in this envelope:

```json
{
  "error": {
    "code": "RESOURCE_EXHAUSTED",
    "message": "lineage query exceeds related artifact limit",
    "details": {
      "artifact_count": 300,
      "max_artifacts": 256
    }
  }
}
```

See `errors.md` for the complete taxonomy.

## Reserved Keys

Keys with `_gateway_` prefix are gateway-reserved.

- Stripped before upstream forwarding
- Excluded from request-key hashing
- Used for gateway control and metadata
