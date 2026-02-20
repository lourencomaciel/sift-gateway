# Security Policy

## Supported Versions

Security fixes are provided for the latest release on `main`.

| Version | Supported |
| ------- | --------- |
| Latest  | Yes       |
| Older   | No        |

## Reporting a Vulnerability

Please do **not** report security vulnerabilities in public issues.

Preferred channel:

- Use GitHub Security Advisories ("Report a vulnerability") for this
  repository.

If that option is unavailable to you:

- Contact the maintainer directly on GitHub:
  https://github.com/lourencomaciel

Include:

- A clear description of the issue
- Reproduction steps or a proof of concept
- Impact assessment (what data/systems are affected)
- Suggested remediation (if known)

## Response Expectations

- Initial acknowledgement: within 3 business days
- Triage/update: within 7 business days
- Fix timeline: depends on severity and release complexity

We will coordinate disclosure timing with reporters when possible.

## Runtime Hardening Guidance

### Capture Surface (`sift-gateway run`)

Current controls:

1. Command execution uses `subprocess.run(command_argv, shell=False)`.
2. `--stdin` cannot be combined with command execution in the same invocation.
3. Command failures are persisted as structured error metadata.
4. Dedup identity uses hash keys, not raw shell strings.

Operational guidance:

1. Treat `sift-gateway run` as trusted-local execution.
2. Restrict who can invoke capture in shared environments.
3. Prefer `--stdin` ingestion for external data when practical.

### Code Surface (`sift-gateway code` / `query_kind="code"`)

Current controls:

1. AST guard enforces import/root restrictions.
2. Runtime executes in a subprocess with timeout and memory constraints.
3. Import allowlist is explicit and configurable.
4. Code runtime can be disabled:

```bash
export SIFT_GATEWAY_CODE_QUERY_ENABLED=false
```

Operational guidance:

1. Keep code query disabled unless required.
2. For untrusted model-authored code, run Sift inside a container boundary.
3. Keep optional code dependencies minimal and controlled.

### Residual Risk

Code-query isolation is process-level, not VM-level. Treat it as a guarded
execution path, not a hardened multi-tenant sandbox.
