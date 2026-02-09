# Cursor Contract — `cursor_v1`

> Defines the cursor token format, payload fields, binding rules, staleness conditions,
> and secret rotation mechanics.

## Token format

```
cur.<version>.<payload_b64u>.<signature_b64u>
```

| Part | Description |
|------|-------------|
| `cur` | Fixed prefix (literal) |
| `<version>` | Secret version identifier (e.g., `v1`, `v2`) |
| `<payload_b64u>` | Base64url-encoded canonical payload bytes (no padding) |
| `<signature_b64u>` | Base64url-encoded HMAC-SHA256 signature (no padding) |

### Signing

1. Payload dict → canonical bytes via RFC 8785 (`canonical_bytes()`)
2. HMAC-SHA256 computed over canonical bytes using the signing secret
3. Signature compared on verification using `hmac.compare_digest()` (timing-safe)

## Payload fields

### Required (always present)

| Field | Type | Description |
|-------|------|-------------|
| `cursor_version` | string | `"cursor_v1"` |
| `traversal_contract_version` | string | `"traversal_v1"` |
| `workspace_id` | string | `"local"` |
| `artifact_id` | string | Artifact being paginated |
| `tool` | string | Tool performing traversal (e.g., `"artifact.get"`) |
| `where_canonicalization_mode` | string | `"raw_string"` (default) |
| `mapper_version` | string | `"mapper_v1"` |
| `position_state` | dict | Pagination position (e.g., `{"offset": 10}`) |
| `issued_at` | string | ISO 8601 UTC timestamp (`"2026-02-08T12:00:00Z"`) |
| `expires_at` | string | ISO 8601 UTC timestamp (`"2026-02-08T12:30:00Z"`) |

### Optional

| Field | Type | Description |
|-------|------|-------------|
| `sample_set_hash` | string | Hash binding for partial mapping cursors |

Additional custom fields may be included via the `extra` dict, but must not conflict with reserved field names.

## TTL and expiration

- `expires_at = issued_at + timedelta(minutes=cursor_ttl_minutes)` (default: 60 minutes)
- Microseconds are stripped from timestamps
- Verification checks `expires_at <= now(UTC)` — if true, raises `CursorExpiredError`
- Both aware and naive datetime objects supported (naive assumed UTC)

## Binding rules

On cursor resume, the following fields are verified:

| Field | Checked against | Error on mismatch |
|-------|-----------------|-------------------|
| `tool` | Expected tool name | `CursorStaleError("cursor tool mismatch")` |
| `artifact_id` | Expected artifact ID | `CursorStaleError("cursor artifact binding mismatch")` |
| `workspace_id` | Expected workspace ID | `CursorStaleError("cursor workspace binding mismatch")` |
| `traversal_contract_version` | `TRAVERSAL_CONTRACT_VERSION` | `CursorStaleError("cursor traversal_contract_version mismatch")` |
| `mapper_version` | `MAPPER_VERSION` | `CursorStaleError("cursor mapper_version mismatch")` |
| `where_canonicalization_mode` | Expected mode (if provided) | `CursorStaleError("cursor where_canonicalization_mode mismatch")` |

## Staleness triggers

A cursor becomes stale (raises `CursorStaleError`) when any of these conditions occur:

1. **Tool mismatch** — cursor created for `artifact.search` used with `artifact.get`
2. **Artifact mismatch** — cursor for artifact `art_1` used with `art_2`
3. **Workspace mismatch** — cursor from different workspace
4. **Version increments** — `traversal_contract_version` or `mapper_version` changed
5. **Where mode mismatch** — `where_canonicalization_mode` differs from request
6. **Sample set hash mismatch** — recomputed hash differs (partial mapping only)
7. **Expiry** — `expires_at <= now` (raises `CursorExpiredError`, distinct from binding errors)

## Sample set hash binding

For cursors that paginate over partial mapping results:

```
sample_set_hash = sha256_trunc(
    canonical_bytes({
        "root_path": root_path,
        "sample_indices": list(sample_indices),
        "map_budget_fingerprint": map_budget_fingerprint,
        "mapper_version": mapper_version,
    }),
    32   # 32 hex chars
)
```

**Mismatch scenarios** (cursor becomes stale):
- Root path changed
- Sample indices changed (remapping selected different samples)
- Map budget fingerprint changed (budgets recalculated)
- Mapper version incremented

## Secret management

### Storage

Secrets are stored in `DATA_DIR/state/secrets.json`:

```json
{
  "active": {
    "v1": "<base64url_secret>",
    "v2": "<base64url_secret>"
  },
  "signing_version": "v2"
}
```

File permissions: `0o600` (read/write for owner only). Writes are atomic (temp → fsync → rename).

### Rotation

1. Generate new secret (32 random bytes, base64url-encoded)
2. Add to `active` dict with new version key
3. Update `signing_version` to new version
4. Old tokens still verify via their version's secret in `active`
5. Remove old version from `active` after grace period to invalidate remaining tokens

### Initialization

On first run: creates `secrets.json` with a single secret (`v1`), sets `signing_version = "v1"`.

## Exception hierarchy

```
ValueError
├── CursorBindingError
│   └── CursorStaleError          # binding/version mismatches
└── CursorTokenError              # signing/format/expiry
    └── CursorExpiredError        # expires_at <= now

SampleSetHashBindingError         # sample hash mismatch
```

## Token validation failures

| Condition | Exception | Message |
|-----------|-----------|---------|
| Not 4 dot-separated parts or prefix != `cur` | `CursorTokenError` | `"invalid cursor token format"` |
| Unknown secret version | `CursorTokenError` | `"unknown cursor secret version: {version}"` |
| Invalid base64 | `CursorTokenError` | `"invalid base64 cursor token"` |
| Invalid UTF-8 payload | `CursorTokenError` | `"invalid cursor payload"` |
| Non-dict payload | `CursorTokenError` | `"cursor payload must be a JSON object"` |
| Wrong `cursor_version` | `CursorTokenError` | `"cursor_version mismatch"` |
| Missing `expires_at` | `CursorTokenError` | `"cursor missing expires_at"` |
| Invalid `expires_at` format | `CursorTokenError` | `"invalid expires_at timestamp"` |
| Expired | `CursorExpiredError` | `"cursor expired"` |
| Signature mismatch | `CursorTokenError` | `"cursor signature mismatch"` |
