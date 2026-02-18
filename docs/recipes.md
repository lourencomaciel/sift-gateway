# Recipes & Examples

Practical patterns for retrieving and reusing Sift artifacts.

## Pattern 1: Large Response -> Deterministic Retrieval

When an upstream tool returns a large payload, Sift returns an artifact handle.
Use `select` to page through the data.

```python
# 1) Mirrored upstream call
result = github.search_repositories(query="mcp", limit=100)
artifact_id = result["artifact_id"]

# 2) First retrieval page
page = artifact(
    action="query",
    query_kind="select",
    artifact_id=artifact_id,
    root_path="$.items",
    select_paths=["name", "stargazers_count"],
    limit=50,
)

# 3) Continue while retrieval is partial
while page["pagination"]["retrieval_status"] == "PARTIAL":
    page = artifact(
        action="query",
        query_kind="select",
        artifact_id=artifact_id,
        cursor=page["cursor"],
        limit=50,
    )
```

Notes:

- `cursor` is passed back as a top-level field.
- `pagination.next_cursor` contains the same continuation token.
- Completeness is based on `pagination.retrieval_status == "COMPLETE"`.

## Pattern 1b: Filtered Retrieval

Use `where` with structured filter objects to push predicates down to SQL.

```python
page = artifact(
    action="query",
    query_kind="select",
    artifact_id=artifact_id,
    root_path="$.items",
    select_paths=["name", "stargazers_count"],
    where={
        "logic": "and",
        "filters": [
            {"path": "$.stargazers_count", "op": "gte", "value": 100},
            {"path": "$.language", "op": "in", "value": ["Python", "TypeScript"]},
        ],
    },
    limit=50,
)
```

Supported operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`,
`array_contains`, `exists`, `not_exists`. See `api_contracts.md` for full
syntax.

## Pattern 2: Upstream Pagination Chain

Use `next_page` only for upstream-layer pagination captured from mirrored tools.

```python
page = github.list_issues(repo="owner/repo", per_page=100)

while page.get("pagination", {}).get("has_next_page"):
    page = artifact(
        action="next_page",
        artifact_id=page["artifact_id"],
    )
```

## Pattern 3: Tool Chaining with Artifact References

Pass artifact references directly to other mirrored tools.

```python
# Full payload
tool_b(input="art_7f3a...")

# Specific field
tool_b(input="art_7f3a...:$.items[0].name")

# Wildcard expansion
tool_b(emails="art_7f3a...:$.users[*].email")
```

Constraint:

- Only top-level string arguments are resolved. Nested dict/list values are not.

## Pattern 4: Code Query (Single Artifact)

Code queries are root-scoped and unpaginated.

```python
summary = artifact(
    action="query",
    query_kind="code",
    artifact_id="art_123...",
    root_path="$.result.rows",
    code="""
def run(data, schema, params):
    floor = float(params.get('min_spend', 0))
    return [
        {"campaign_id": row.get("campaign_id"), "spend": float(row.get("spend", 0) or 0)}
        for row in data
        if float(row.get("spend", 0) or 0) >= floor
    ]
""",
    params={"min_spend": 10},
)
```

Contract highlights:

- Required: `artifact_id`, `root_path`, `code`
- Optional: `params`
- Response has no `cursor`; code query is one-shot
- Output must fit `max_bytes_out`

## Pattern 5: Code Query (Multi-Artifact)

Use `artifact_ids` and a multi-artifact runtime entrypoint.

```python
joined = artifact(
    action="query",
    query_kind="code",
    artifact_ids=["art_users...", "art_orders..."],
    root_paths={
        "art_users...": "$.users",
        "art_orders...": "$.orders",
    },
    code="""
def run(artifacts, schemas, params):
    users = artifacts["art_users..."]
    orders = artifacts["art_orders..."]
    user_names = {u["id"]: u.get("name") for u in users}

    totals = {}
    for order in orders:
        uid = order.get("user_id")
        totals[uid] = totals.get(uid, 0) + float(order.get("amount", 0) or 0)

    return [
        {"user_id": uid, "name": user_names.get(uid), "total": total}
        for uid, total in totals.items()
    ]
""",
)
```

## Pattern 6: Session Artifact Search

List artifacts already visible to the current session.

```python
search_page = artifact(
    action="query",
    query_kind="search",
    filters={"status": "ok", "source_tool_prefix": "github"},
    order_by="created_seq_desc",
    limit=50,
)

if search_page["truncated"]:
    search_page_2 = artifact(
        action="query",
        query_kind="search",
        cursor=search_page["cursor"],
        limit=50,
    )
```

## Common Mistakes

- Using `action="next_page"` for retrieval cursors.
  Use `artifact(action="query", ..., cursor=...)` instead.
- Calling `select` without `select_paths` on a fresh query.
- Treating code queries as paginated (`limit`/`cursor` are not retrieval paging controls).
- Assuming `has_more=false` means complete without checking
  `pagination.retrieval_status`.
