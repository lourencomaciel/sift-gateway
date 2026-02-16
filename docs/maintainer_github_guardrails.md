# Maintainer GitHub Guardrails

Configure repository protection so docs/runtime contract checks are mandatory.

## Recommended: Rulesets (Modern GitHub)

Use the prebuilt import file in this repo:

- `.github/rulesets/main-protection.json`

1. Open repository **Settings**.
2. Go to **Rules** -> **Rulesets**.
3. Click **New ruleset** -> **Import a ruleset**.
4. Upload `.github/rulesets/main-protection.json`.
5. Confirm the imported values:
   - PR required (1 approval, Code Owner review, thread resolution)
   - Required checks:
     - `CI / quality`
     - `Docs Contract / docs-contract`
   - Force push and delete protections
6. Save and enable the ruleset.

## Legacy Alternative: Branch Protection Rule

If your repo still uses branch protection rules instead of rulesets:

1. Open **Settings** -> **Branches**.
2. Under **Branch protection rules**, click **Add rule**.
3. Set branch name pattern to `main`.
4. Enable:
   - **Require a pull request before merging**
   - **Require approvals** (at least 1)
   - **Require review from Code Owners**
   - **Require conversation resolution before merging**
   - **Require status checks to pass before merging**
   - **Do not allow force pushes**
   - **Do not allow deletions**
5. Add required checks:
   - `CI / quality`
   - `Docs Contract / docs-contract`
6. Save changes.

## Verification Checklist

After configuration:

1. Open any PR.
2. Confirm checks show both required entries:
   - `CI / quality`
   - `Docs Contract / docs-contract`
3. Confirm merge is blocked if either check fails.
4. Confirm merge is blocked without Code Owner approval.
