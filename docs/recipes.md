# Recipes & Examples

Practical patterns for capturing, paginating, and analyzing artifacts.

## Pattern 1: Run -> Code (CLI)

```bash
# capture
sift-gateway run -- curl -s 'https://jsonplaceholder.typicode.com/comments?_page=1&_limit=200'

# analyze
sift-gateway code <artifact_id> '$' --expr "df['email'].str.contains('joana', case=False, na=False).sum()"
```

Use `--expr` for quick transforms and `--code` / `--file` for multi-step logic.

## Pattern 2: Upstream pagination chain (CLI)

When `run` returns `pagination.has_next_page=true`:

```bash
# page 1
sift-gateway run -- gh api repos/org/repo/pulls --limit 100 --after CUR_1

# page 2 (linked lineage)
sift-gateway run --continue-from art_page_1 -- gh api repos/org/repo/pulls --limit 100 --after CUR_2
```

Notes:

- Apply `pagination.next_params` from the previous response.
- Each continuation creates a new artifact linked by `parent_artifact_id` and
  `chain_seq`.

## Pattern 3: Upstream pagination chain (MCP)

```python
page = github.list_issues(repo="owner/repo", per_page=100)

while page.get("pagination", {}).get("retrieval_status") == "PARTIAL":
    page = artifact(
        action="next_page",
        artifact_id=page["artifact_id"],
    )
```

Do not claim completeness until
`pagination.retrieval_status == "COMPLETE"`.

## Pattern 4: Code query (MCP, single artifact)

```python
summary = artifact(
    action="query",
    query_kind="code",
    artifact_id="art_123",
    root_path="$.result.rows",
    code="""
def run(data, schema, params):
    floor = float(params.get('min_spend', 0))
    return [
        {
            "campaign_id": row.get("campaign_id"),
            "spend": float(row.get("spend", 0) or 0),
        }
        for row in data
        if float(row.get("spend", 0) or 0) >= floor
    ]
""",
    params={"min_spend": 10},
)
```

## Pattern 5: Code query (MCP, multi artifact)

```python
joined = artifact(
    action="query",
    query_kind="code",
    artifact_ids=["art_users", "art_orders"],
    root_paths={
        "art_users": "$.users",
        "art_orders": "$.orders",
    },
    code="""
def run(artifacts, schemas, params):
    users = artifacts["art_users"]
    orders = artifacts["art_orders"]
    names = {u["id"]: u.get("name") for u in users}

    totals = {}
    for order in orders:
        uid = order.get("user_id")
        totals[uid] = totals.get(uid, 0) + float(order.get("amount", 0) or 0)

    return [
        {"user_id": uid, "name": names.get(uid), "total": total}
        for uid, total in totals.items()
    ]
""",
)
```

## Pattern 6: Schema-first response handling

For both mirrored-tool and code responses:

1. Check `response_mode`.
2. If `full`, consume `payload` directly.
3. If `schema_ref`, inspect `schemas_compact` and run focused code queries.
4. If pagination is present and partial, continue with `next_page` (MCP) or
   `run --continue-from` (CLI).

## Common mistakes

- Trying to call `artifact(action="query")` without `query_kind="code"`.
- Treating `run --continue-from` as optional when `has_next_page=true`.
- Assuming completion without checking `pagination.retrieval_status`.
- Returning huge code outputs without narrowing logic.
