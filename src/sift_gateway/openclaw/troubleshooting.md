# OpenClaw Troubleshooting

Common issues when using Sift to control context size in OpenClaw workflows.

## Symptom: context still gets flooded

Cause:

- large commands are still run directly instead of through `sift-gateway run`.

Fix:

```bash
sift-gateway run -- <large-command>
```

Keep only `artifact_id` and compact summaries in prompts.

## Symptom: `sift-gateway run` returns command errors

Cause:

- command exits non-zero, auth failure, or missing executable.

Fix:

- run the command standalone first
- inspect `status`, `command_exit_code`, and captured metadata from `run --json`

## Symptom: paginated API stops after first page

Cause:

- follow-up command was not issued with continuation parameters.

Fix:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

Use `pagination.next_params` from the prior result.

## Symptom: `sift-gateway code` fails immediately

Cause:

- missing code source or invalid JSON in `--params`.

Fix:

```bash
sift-gateway code <artifact_id> '$.items' --expr "df.shape[0]"
sift-gateway code <artifact_id> '$.items' --file ./analysis.py --params '{"team":"infra"}'
```

## Symptom: artifact not found

Cause:

- wrong artifact id, expired TTL, or wrong data dir.

Fix:

- reuse the exact `artifact_id` from previous command output
- verify the same `--data-dir` is used across commands
- increase retention when needed:

```bash
sift-gateway run --ttl 24h -- <command>
```

## Symptom: output from code is too large

Cause:

- code returns full records instead of a narrow projection.

Fix:

- return aggregates/selected columns only
- split work into smaller code steps if needed
