# OpenClaw Integration Pack

This pack makes Sift the default large-output path for OpenClaw agents.

## What you get

- installable skill file: `docs/openclaw/SKILL.md`
- OpenClaw-first quickstart
- troubleshooting guidance
- compact response templates

## OpenClaw-first quickstart

1. Install Sift:

```bash
pipx install sift-gateway
```

2. Install the packaged skill:

```bash
mkdir -p ~/.openclaw/skills/sift-gateway
sift-gateway-openclaw-skill --output ~/.openclaw/skills/sift-gateway/SKILL.md
```

3. Restart OpenClaw (or reload skills), then run:

```bash
sift-gateway run -- echo '[{"id":1,"state":"open"},{"id":2,"state":"closed"}]'
sift-gateway code <artifact_id> '$' --expr "df.shape[0]"
```

If `pagination.next.kind=="command"`, continue with:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

4. Add one short system instruction in your OpenClaw profile:

```text
When command output may exceed ~4KB, capture with `sift-gateway run` and analyze with `sift-gateway code`.
```

## Capture vs inline decision rule

- Inline: expected output < 4KB and used once.
- Capture: lists, logs, paginated APIs, JSON blobs, tabular data, or reused data.
- Always capture: `gh api`, `kubectl ... -o json`, `curl` returning arrays/objects.

## Manual validation checklist

- `sift-gateway run -- <cmd>` returns artifact summary.
- paginated captures continue via `run --continue-from`.
- `sift-gateway run --stdin` works from a pipe.
- `sift-gateway code` returns focused outputs in both `--expr` and `--file` modes.

## Troubleshooting

### Symptom: context still gets flooded

Cause:

- large commands are still run directly instead of through `sift-gateway run`.

Fix:

```bash
sift-gateway run -- <large-command>
```

Keep only `artifact_id` and compact summaries in prompts.

### Symptom: `sift-gateway run` returns command errors

Cause:

- command exits non-zero, auth failure, or missing executable.

Fix:

- run the command standalone first
- inspect `status`, `command_exit_code`, and captured metadata from `run --json`

### Symptom: paginated API stops after first page

Cause:

- follow-up command was not issued with continuation parameters.

Fix:

```bash
sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>
```

Use `pagination.next.params` from the prior result.

### Symptom: `sift-gateway code` fails immediately

Cause:

- missing code source or invalid JSON in `--params`.

Fix:

```bash
sift-gateway code <artifact_id> '$.items' --expr "df.shape[0]"
sift-gateway code <artifact_id> '$.items' --file ./analysis.py --params '{"team":"infra"}'
```

### Symptom: artifact not found

Cause:

- wrong artifact id, expired TTL, or wrong data dir.

Fix:

- reuse the exact `artifact_id` from previous command output
- verify the same `--data-dir` is used across commands
- increase retention when needed:

```bash
sift-gateway run --ttl 24h -- <command>
```

### Symptom: output from code is too large

Cause:

- code returns full records instead of a narrow projection.

Fix:

- return aggregates or selected columns only
- split work into smaller code steps if needed

## Response templates

### Capture summary

```text
Captured to <artifact_id> (<records> records, <bytes> bytes).
Next: run `sift-gateway code <artifact_id> '$' --expr "<narrow_expr>"`.
```

### Follow-up capture

```text
Captured page artifact <artifact_id> linked to <parent_artifact_id>.
Next: continue with `sift-gateway run --continue-from <artifact_id> -- <next-command>` if more pages remain.
```

### Code result

```text
Computed <summary> from <artifact_id>.
Returned <count> result rows.
```

### Pagination follow-up

```text
Upstream pagination is PARTIAL for <artifact_id>.
Use: `sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>`.
```

### Failure template

```text
Operation failed: <code> - <message>.
Next action: <single command to unblock>.
```

## Related docs

- `docs/openclaw/SKILL.md`
