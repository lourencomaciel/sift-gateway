# API Contracts

Technical specifications for Sift MCP's API contracts and response formats.

## Artifact Query Entrypoint

Use the consolidated `artifact` tool for retrieval queries:

- `action="query"` selects retrieval mode.
- `query_kind="describe|get|select|search|code"` selects behavior.

`search` is for session artifact discovery; the other `query_kind` values require an `artifact_id`.

## Pagination Contract v1

Sift uses **layer-explicit pagination metadata** to distinguish between upstream pagination and gateway retrieval pagination.

### Pagination Layers

**Two distinct layers:**

1. **`pagination.layer = "upstream"`** — Mirrored upstream tool responses
   - Indicates pagination controlled by the upstream MCP server
   - Use `artifact(action="next_page")` to fetch subsequent pages

2. **`pagination.layer = "artifact_retrieval"`** — Artifact retrieval responses
   - Indicates pagination controlled by Sift's retrieval tools
   - Use `cursor` parameter to continue pagination

### Key Fields

All pagination responses include these fields:

- **`retrieval_status`**: `PARTIAL` or `COMPLETE`
  - `PARTIAL` — More data exists, pagination required
  - `COMPLETE` — No more data available

- **`partial_reason`**: machine-readable partial reason or `null`
  - `"budget_items"` — Hit `max_items` limit
  - `"budget_bytes"` — Hit `max_bytes_out` limit
  - `"wildcard_cap"` — Hit wildcard expansion limit
  - `null` — Complete retrieval

- **`has_more`**: boolean — Whether more data is available

### Upstream Pagination (Compatibility Fields)

For `pagination.layer = "upstream"`, these additional fields remain for compatibility:

- **`has_next_page`**: boolean — Whether upstream has more pages
- **`hint`**: string — Upstream pagination hint (if provided)

### Completion Rule

**CRITICAL:** Do not claim full completeness until:

```python
pagination["retrieval_status"] == "COMPLETE"
```

Even if `has_more` is `false`, check `retrieval_status` to ensure completeness.

### Examples

#### Upstream Pagination Response

```json
{
  "artifact_id": "art_123...",
  "pagination": {
    "layer": "upstream",
    "retrieval_status": "PARTIAL",
    "partial_reason": null,
    "has_more": true,
    "has_next_page": true,
    "hint": "Use artifact(action='next_page') to fetch next page"
  }
}
```

#### Artifact Retrieval Pagination Response

```json
{
  "items": [...],
  "pagination": {
    "layer": "artifact_retrieval",
    "retrieval_status": "PARTIAL",
    "partial_reason": "budget_items",
    "has_more": true,
    "cursor": "eyJ..."
  },
  "total_matched": 1523
}
```

## Gateway Context Controls

Control artifact creation and caching behavior via `_gateway_context` in mirrored tool calls.

### `allow_reuse` Parameter

Mirrored tool calls may include `_gateway_context.allow_reuse`:

- **`false` (default)** — Always create a fresh artifact, even if an identical one exists
- **`true`** — Allow request-key deduplication and reuse compatible artifacts

**Example:**

```json
{
  "tool": "github.search_repositories",
  "arguments": {
    "query": "MCP server",
    "_gateway_context": {
      "allow_reuse": true
    }
  }
}
```

### Cache Metadata in Responses

All handle responses include consistent cache metadata:

- **`reused`**: boolean — Whether this artifact was reused from cache
- **`request_key`**: string — SHA-256 hash of normalized request
- **`reason`**: string — Why this artifact was created/reused
  - `"fresh"` — Newly created artifact
  - `"request_key_match"` — Reused based on request key
- **`artifact_id_origin`**: `"cache"` or `"fresh"`
- **`allow_reuse`**: boolean — The effective `allow_reuse` setting

**Example (cache hit):**

```json
{
  "artifact_id": "art_abc123...",
  "meta": {
    "cache": {
      "reused": true,
      "request_key": "sha256:def456...",
      "reason": "request_key_match",
      "artifact_id_origin": "cache",
      "allow_reuse": true
    }
  }
}
```

**Example (fresh artifact):**

```json
{
  "artifact_id": "art_xyz789...",
  "meta": {
    "cache": {
      "reused": false,
      "request_key": "sha256:ghi789...",
      "reason": "fresh",
      "artifact_id_origin": "fresh",
      "allow_reuse": false
    }
  }
}
```

### Session Visibility on Cache Reuse

**Important guarantee:** When a mirrored call returns a reused `artifact_id`, Sift **first attaches that artifact to the caller's session** (`artifact_refs`) before returning the handle.

This guarantees the returned handle is immediately retrievable by:
- `artifact(action="query", query_kind="get")`
- `artifact(action="query", query_kind="describe")`
- `artifact(action="query", query_kind="select")`
- `artifact(action="query", query_kind="code")`

**You never receive an artifact handle that you can't immediately query.**

## Handle Response Contract (Schema-First)

When a mirrored tool call response exceeds the passthrough threshold (`passthrough_max_bytes`, default 8 KB), Sift returns an **artifact handle** instead of the raw response.

### Handle Response Fields

**Always present:**

- **`artifact_id`**: string — Unique identifier for artifact retrieval
- **`schemas`**: array — Inferred data structures (schema-first approach)
- **`usage_hint`**: string — Human-readable guidance for querying the artifact
- **`meta.cache`**: object — Cache metadata (see Gateway Context Controls above)

**Optional:**

- **`mapping`**: object — Data structure mapping metadata
- **`pagination`**: object — Pagination metadata (if upstream provides it)

### Schema-First Design

The `schemas` field is **canonical** — there's no duplicated `roots[].schema` embedding. To save space:

- **When one root has unique highest coverage:** Only that schema is returned
- **When multiple roots tie for highest coverage:** All tied schemas are returned

**Example (single primary schema):**

```json
{
  "artifact_id": "art_abc123...",
  "schemas": [
    {
      "root_path": "$.items",
      "coverage": {
        "observed_records": 150
      },
      "fields": {
        "id": {"type": "string", "nullable": false},
        "name": {"type": "string", "nullable": false},
        "created_at": {"type": "string", "nullable": true}
      }
    }
  ],
  "usage_hint": "Use artifact(action='query', query_kind='select', root_path='$.items') to retrieve data"
}
```

**Example (multiple tied schemas):**

```json
{
  "artifact_id": "art_def456...",
  "schemas": [
    {
      "root_path": "$.users",
      "coverage": {"observed_records": 50}
    },
    {
      "root_path": "$.groups",
      "coverage": {"observed_records": 50}
    }
  ],
  "usage_hint": "Multiple data roots detected. Choose root_path: '$.users' or '$.groups'"
}
```

### Schema Inference

Sift automatically infers schemas from response data by:

1. **Traversing the JSON structure** — Walks all paths to identify roots and fields
2. **Sampling records** — Observes up to 50 records per root (configurable)
3. **Type detection** — Infers JSON types (string, number, boolean, array, object, null)
4. **Nullability tracking** — Marks fields as nullable if any sampled record has null

See [spec_v1_9.md](spec_v1_9.md) for complete schema inference algorithm.

## Artifact Query Response Shapes

`artifact(action="query")` does **not** have one universal response shape. Fields depend on `query_kind`.

### `query_kind="describe"`

Metadata response (no retrieval items):

- **Required**: `artifact_id`, `scope`, `lineage`, `artifacts`, `roots`
- **Optional**: `schema_legend`, `schemas` (present for `scope="single"` when schema data exists)
- **Not present by contract**: `items`, `total_matched`, `cursor`, retrieval `pagination`

### `query_kind="get"`

`target="envelope"` returns paginated envelope/jsonpath values:

- **Required**: `artifact_id`, `scope`, `target`, `items`, `truncated`, `omitted`, `stats`, `lineage`, `pagination`
- **Optional**: `cursor`, `warnings`

`target="mapped"` returns mapped root catalog:

- **Required**: `artifact_id`, `scope`, `target`, `roots`, `lineage`, `pagination`
- **Optional**: `warnings`

### `query_kind="select"`

Select-style retrieval response:

- **Required**: `items`, `truncated`, `pagination`, `total_matched`, `scope`, `lineage`
- **Optional**: `cursor`, `omitted`, `stats`, `determinism`, `warnings`

Special case: `count_only=true` returns `count` plus retrieval `pagination` (no `items` payload).

### `query_kind="code"`

Code-query response is also select-like:

- **Required**: `items`, `truncated`, `pagination`, `total_matched`, `scope`, `lineage`, `determinism`
- **Optional**: `cursor`, `omitted`, `stats`, `warnings`

### `query_kind="search"`

Session artifact listing response:

- **Required**: `items`, `truncated`, `omitted`, `pagination`
- **Optional**: `cursor`

### Example (`query_kind="select"`)

```json
{
  "items": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ],
  "pagination": {
    "layer": "artifact_retrieval",
    "retrieval_status": "PARTIAL",
    "partial_reason": "budget_items",
    "has_more": true,
    "cursor": "eyJvZmZzZXQiOjUwfQ=="
  },
  "total_matched": 150,
  "truncated": true,
  "scope": "all_related",
  "lineage": ["art_abc123...", "art_def456..."]
}
```

## Error Contract

All errors follow a consistent envelope format:

```json
{
  "error": {
    "code": "RESOURCE_EXHAUSTED",
    "message": "lineage query exceeds related artifact limit",
    "details": {
      "artifact_count": 3500,
      "max_artifacts": 2000
    }
  }
}
```

See [errors.md](errors.md) for complete error taxonomy.

## Reserved Keys

Sift reserves keys with the `_gateway_*` prefix for internal use. These keys:

- Are **stripped before upstream forwarding** — Upstreams never see them
- Are **excluded from hashing** — Don't affect request_key computation
- Control gateway behavior (caching, dedupe, passthrough, etc.)

**Example:**

```json
{
  "query": "search term",
  "_gateway_context": {
    "allow_reuse": true
  },
  "_gateway_trace_id": "abc123"
}
```

The upstream receives only `{"query": "search term"}`.

## Next Steps

- **[Recipes & Examples](recipes.md)** — See these contracts in action
- **[Architecture & Spec](spec_v1_9.md)** — Deep dive into technical design
- **[Configuration Reference](config.md)** — Configure pagination budgets and thresholds
- **[Error Reference](errors.md)** — Complete error code catalog
