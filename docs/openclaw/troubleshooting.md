# OpenClaw Troubleshooting

Common issues when using Sift to prevent context overflow in OpenClaw workflows.

## Symptom: Context Still Gets Flooded

Cause:
- Large commands are still run directly instead of through `sift run`.

Fix:
- Route large outputs through Sift:

```bash
sift run -- <large-command>
```

- Keep only `artifact_id` and short summaries in prompts.

## Symptom: `sift run` Returns Command Errors

Cause:
- Command exits non-zero, missing auth, or missing executable.

Fix:
- Check the command standalone first.
- For JSON APIs, inspect stderr captured in artifact payload:

```bash
sift get <artifact_id> --target envelope
```

## Symptom: Query Returns Too Much Data

Cause:
- Query root is too broad or missing projection.

Fix:
- Add `--limit` and `--select`, and narrow root path.

```bash
sift query <artifact_id> '$.items' --select "id,name,status" --limit 20
```

## Symptom: `sift code` Fails Immediately

Cause:
- Missing code input mode or invalid JSON in `--params`.

Fix:
- Provide exactly one code source: `--expr`, `--code`, or `--file`.
- Ensure `--params` decodes to a JSON object.

```bash
sift code <artifact_id> '$.items' --expr "df.shape[0]"
sift code <artifact_id> '$.items' --file ./analysis.py --params '{"team":"infra"}'
```

## Symptom: Artifact Not Found / Gone

Cause:
- Wrong artifact id or TTL expiration.

Fix:
- Locate valid IDs:

```bash
sift list --limit 50
```

- Increase retention for long tasks:

```bash
sift run --ttl 24h -- <command>
```

## Symptom: Too Many Repeated Captures

Cause:
- `sift run` always captures fresh output by design.

Fix:
- Filter or diff recent runs to compare only relevant changes:

```bash
sift list --capture-kind cli_command --limit 20
```

## Symptom: Need Quick Delta Between Two Runs

Fix:

```bash
sift diff <old_artifact_id> <new_artifact_id>
```

Use `--max-lines` if you need more unified diff context.
