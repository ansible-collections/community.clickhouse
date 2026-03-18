---
name: pr-review
description: Reviews pull requests and code changes in the community.clickhouse Ansible collection against project standards and the Ansible Collection Review Checklist. Use when asked to review a PR, patch, diff, or set of code changes. Do not use for GitHub Issues or general Q&A.
---

# Skill: pr-reviewer

## Purpose

Review pull requests and code changes in the `community.clickhouse` Ansible collection.

## When to Invoke

TRIGGER when:
- A user asks to review a PR, patch, diff, or set of code changes
- Validating changes against project standards before merge

DO NOT TRIGGER when:
- Reviewing GitHub Issues (not PRs/code changes)
- General Q&A, documentation lookup, or debugging unrelated to a changeset

## Inputs

- `target` (optional): PR number, branch name, commit hash, or file path.
  - If omitted, review the current working changes via `git diff HEAD`.

## Approach

### Step 1 — Gather the changeset

Obtain the diff using the appropriate method:
- PR number provided → read changed files and their diffs
- Branch or commit reference → run `git diff <base>..<ref>` or `git show <ref>`
- File path provided → read the file and review it in full
- No target → run `git diff HEAD` to capture all current changes

Read every changed file completely before forming any judgment.

### Step 2 — Run all review checks in parallel

Execute all checks in the checklist below concurrently. Collect findings per category.

### Step 3 — Report

Produce the structured report described in the **Output Format** section.

---

## Review Checklist

### A. Collection Metadata

- [ ] `galaxy.yml`: `version`, `description`, `tags`, `dependencies` are accurate and up to date.
- [ ] `meta/runtime.yml`: `requires_ansible` minimum version reflects any new Ansible features used.
- [ ] New Python dependencies added to both `requirements.txt` and `meta/ee-requirements.txt`.

### B. Module Documentation

- [ ] Every public parameter has a `description`, `type`, and `required` or `default`.
- [ ] Every new module and every new parameter carries `version_added: 'x.y.z'` set to the next planned release version.
- [ ] The `EXAMPLES` block is present, valid YAML, and covers the primary use cases.
- [ ] The `RETURN` block accurately describes every key returned by the module.
- [ ] Connection parameters are **not** duplicated in individual modules — they are inherited via `extends_documentation_fragment: community.clickhouse.client_inst_opts`.
- [ ] Module short description (`short_description`) is concise and accurate.
- [ ] `author` field is present and correctly formatted.

### C. Naming and Style

- [ ] All variable and parameter names use `snake_case`.
- [ ] Module file names follow the `clickhouse_<noun>` pattern.
- [ ] No abbreviations that reduce readability.
- [ ] Code follows KISS, DRY, YAGNI, and Separation of Concerns.
- [ ] Functions are short, pure (no side effects where possible), and independently testable.
- [ ] Classes are used only when they naturally group tightly related state and behavior; simple functions are preferred.

### D. Architecture and Shared Code

- [ ] Connection logic uses `client_common_argument_spec()` from `plugins/module_utils/clickhouse.py` — never duplicated in a module.
- [ ] Shared utilities used by multiple modules live in `plugins/module_utils/clickhouse.py`.
- [ ] New connection parameters are added to `plugins/doc_fragments/client_inst_opts.py`, not to individual modules.
- [ ] Bootstrap pattern is followed: `argument_spec → AnsibleModule → check_clickhouse_driver → connect_to_db_via_client`.

### E. Idempotency

- [ ] State-management modules query the relevant `system.*` table first, compare current vs desired state, and execute DDL only when a change is needed.
- [ ] `result['changed']` is `False` when no real change is made.
- [ ] Repeated runs with the same arguments produce the same outcome with no spurious changes.

### F. check_mode Support

- [ ] All modules except `clickhouse_client` declare `supports_check_mode=True`.
- [ ] In check_mode, `executed_statements` is populated with what *would* run, but no DDL is actually executed.

### G. Sensitive Data

- [ ] All sensitive parameters (passwords, tokens, secrets) set `no_log=True`.
- [ ] No sensitive data appears in `executed_statements` or module return values in plaintext.

### H. Error Handling

- [ ] All errors call `module.fail_json(msg=...)` with a descriptive, actionable message — no bare `raise` or `sys.exit()`.
- [ ] `clickhouse_info` and similar read-only modules handle privilege errors gracefully (return partial results, not a failure).
- [ ] `execute_query()` from `module_utils/clickhouse.py` is used for all query execution.

### I. Type Conversion (`clickhouse_client`)

- [ ] Query results containing `uuid.UUID`, `decimal.Decimal`, `ipaddress.IPv4Address`, or `ipaddress.IPv6Address` are converted to `str` before returning (these types are not JSON-serializable by Ansible).

### J. Testing

- [ ] Sanity checks pass: `ansible-test sanity <changed_file> --docker -v`
- [ ] Unit tests are present for any new or modified logic in `module_utils` or non-trivial module functions. Located under `tests/unit/plugins/`.
- [ ] Integration tests are required for any non-refactoring, non-documentation code change. Located under `tests/integration/targets/<module_name>/`.
- [ ] Integration test pattern is followed:
  1. Call module → `register: result`
  2. `ansible.builtin.assert` on `result`
  3. Verify DB state using `community.clickhouse.clickhouse_client` → `register: result` → `ansible.builtin.assert`
- [ ] Each integration test target has `tests/integration/targets/<name>/meta/main.yml` declaring `setup_clickhouse` as a dependency.
- [ ] Tests cover both the happy path and idempotency (running the same task twice).
- [ ] Tests cover the `state: absent` path where applicable.

### K. Backwards Compatibility

- [ ] No existing parameters are removed or renamed without a deprecation notice.
- [ ] No existing return values are removed or their types changed.
- [ ] Breaking changes are flagged explicitly and justified.
- [ ] Deprecations use the Ansible deprecation mechanism (`module.deprecate()`).

### L. Changelog Fragment

- [ ] A fragment file exists under `changelogs/fragments/<something>.yaml` for any PR that changes module behavior, adds a feature, or fixes a bug.
- [ ] Documentation-only, test-only, and refactoring-only PRs are exempt.
- [ ] The fragment uses one of the valid sections: `major_changes`, `minor_changes`, `bugfixes`, `breaking_changes`, `deprecated_features`, `removed_features`, `security_fixes`, `known_issues`.
- [ ] Fragment content is concise, written in past tense, and references the module name.

### M. General Code Quality

- [ ] No dead code, commented-out blocks, or debug statements left in.
- [ ] No feature flags or backwards-compatibility shims for hypothetical future use.
- [ ] No premature abstractions (helpers/utilities created for a single use case).
- [ ] No security vulnerabilities: no shell injection, no unvalidated external input passed to queries, no hardcoded credentials.

---

## Output Format

Structure the review as follows:

```
## PR Review: <target or "Current Changes">

### Summary
<One-paragraph overall assessment: scope of the change, general quality, primary concerns.>

### Findings

#### Blockers (must fix before merge)
- [CATEGORY] <File>:<line> — <description of the issue>

#### Warnings (should fix, not strictly blocking)
- [CATEGORY] <File>:<line> — <description of the issue>

#### Suggestions (optional improvements)
- [CATEGORY] <File>:<line> — <description of the issue>

### Checklist Status
| Category | Status | Notes |
|---|---|---|
| A. Collection Metadata | PASS / FAIL / N/A | ... |
| B. Module Documentation | PASS / FAIL / N/A | ... |
| C. Naming and Style | PASS / FAIL / N/A | ... |
| D. Architecture | PASS / FAIL / N/A | ... |
| E. Idempotency | PASS / FAIL / N/A | ... |
| F. check_mode | PASS / FAIL / N/A | ... |
| G. Sensitive Data | PASS / FAIL / N/A | ... |
| H. Error Handling | PASS / FAIL / N/A | ... |
| I. Type Conversion | PASS / FAIL / N/A | ... |
| J. Testing | PASS / FAIL / N/A | ... |
| K. Backwards Compatibility | PASS / FAIL / N/A | ... |
| L. Changelog Fragment | PASS / FAIL / N/A | ... |
| M. Code Quality | PASS / FAIL / N/A | ... |

### Verdict
APPROVE / REQUEST CHANGES / COMMENT

<One sentence justifying the verdict.>
```

Use `N/A` for categories that do not apply to the changeset (e.g., type conversion for a docs-only PR). Be specific: always reference the file and line number when citing a finding.
