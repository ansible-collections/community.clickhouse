# AGENTS.md

This file is intended for AI coding agents. It is kept human-readable so contributors can also use it as a quick-reference guide.

When planning or reviewing changes, always check with `REVIEW_CHECKLIST.md` file.

When official documentation is not explicitly provided, delegate to the `docs-explorer` subagent (see `agents/docs-explorer.md`) to look up current documentation for the relevant libraries and technologies.

## What This Project Is

An Ansible collection (`community.clickhouse`) providing modules for managing ClickHouse databases. No roles exist — only modules and shared utilities. See `SPEC.md` for full technical reference.

## Development Environment

The collection must reside at `ansible_collections/community/clickhouse/` (relative to a directory on `ANSIBLE_COLLECTIONS_PATHS`) for imports to resolve correctly.

**Required Python package:**
```bash
pip install clickhouse-driver
```

**Optional packages** (needed only for `clickhouse_cfg_info`):
```bash
pip install pyyaml xmltodict
```

All three are listed in `requirements.txt`. The `meta/ee-requirements.txt` file tracks the same dependencies for Execution Environments.

## Test Commands

All testing uses `ansible-test`. The collection must be installed under the canonical Ansible collection path (`ansible_collections/community/clickhouse/`) for imports to resolve correctly.

```bash
# Sanity checks (style, docs, imports) run against a changed file
ansible-test sanity plugins/modules/clickhouse_db.py --docker -v

# Unit tests run against changed files
ansible-test units tests/unit/plugins/modules/test_clickhouse_client.py --docker -v
ansible-test units tests/unit/plugins/module_utils/test_clickhouse_grants.py --docker -v

# Integration tests (requires a running ClickHouse instance or Docker)
ansible-test integration clickhouse_db --docker default -v
ansible-test integration clickhouse_user --docker default -v
```

Integration tests depend on `setup_clickhouse` target (installs ClickHouse server). Each target in `tests/integration/targets/` has `meta/main.yml` declaring that dependency.

## Architecture

### Shared Foundation

Every module follows the same bootstrap pattern:

```python
argument_spec = client_common_argument_spec()   # from module_utils/clickhouse.py
argument_spec.update({...})                     # module-specific params
module = AnsibleModule(argument_spec=argument_spec, ...)
check_clickhouse_driver(module)
client = connect_to_db_via_client(module, ...)
```

Connection parameters (`login_host`, `login_port`, `login_user`, `login_password`, `login_db`, `client_kwargs`) are defined once in `plugins/doc_fragments/client_inst_opts.py` and shared via `extends_documentation_fragment`.

### Idempotency Pattern

State-management modules (db, user, role, quota, grants) all follow: query `system.*` table → compare current vs desired → execute DDL only if different → return `executed_statements`. The comparison logic lives in module-level classes (e.g., `ClickHouseDB`, `ClickHouseUser`).

### Error Handling

`PRIV_ERR_CODE = 497` (defined in `module_utils/clickhouse.py`) is returned instead of failing when the connected user lacks privileges to read a system table. `clickhouse_info` uses this to return partial results gracefully.

### check_mode

All modules support `check_mode` except `clickhouse_client` (which executes arbitrary SQL and cannot know whether it changes state). In check_mode, modules populate `executed_statements` with what *would* run.

### clickhouse_client Type Conversion

Query results containing `uuid.UUID`, `decimal.Decimal`, or `ipaddress.IPv4/IPv6Address` are converted to `str` before returning to Ansible (these types are not JSON-serializable by Ansible's output layer).

## Coding Guidelines

- Follow these software development principles: KISS (Keep It Simple, Stupid), DRY (Don't Repeat Yourself), YAGNI (You Aren't Gonna Need It), Separation of Concerns, Composition over Inheritance, and Convention Over Configuration.
- Prioritize code simplicity and readability over flexibility.
- Favor simple, short, and easily testable functions with no side effects over classes. Use classes only when they naturally fit the problem and help avoid boilerplate code while grouping tightly related functionality.
- Use `snake_case` for all variable and parameter names.
- Shared code used by multiple modules belongs in `plugins/module_utils/clickhouse.py` (DRY principle). Do not duplicate connection or utility logic in individual modules.
- Do not add connection parameters to individual modules. Extend the `client_inst_opts` doc fragment in `plugins/doc_fragments/client_inst_opts.py` instead.
- New modules and new parameters require `version_added: 'x.y.z'` in their DOCUMENTATION block, set to the next planned release version.
- All modules must pass sanity, unit, and integration tests before merging.

## Development Conventions

- Every new module parameter and new module requires `version_added: 'x.y.z'` in its DOCUMENTATION block, set to the next planned release version.
- Every PR that changes module behavior needs a changelog fragment in `changelogs/fragments/<something>.yaml`. Docs/tests/refactoring PRs are exempt. Valid fragment sections: `major_changes`, `minor_changes`, `bugfixes`, `breaking_changes`, `deprecated_features`, `removed_features`, `security_fixes`, `known_issues`. Fragments are consumed (deleted) at release time (`keep_fragments: false` in `changelogs/config.yaml`). To generate the changelog at release time, run:
  ```bash
  antsibull-changelog release -v
  ```
- Integration tests are required for any non-refactoring and non-documentation code change. The test pattern is: call module → `register: result` → `ansible.builtin.assert` → check database state by running `community.clickhouse.clickhouse_client` → `register: result` → `ansible.builtin.assert`.

## Subagents

Reusable subagent definitions live in the `agents/` directory. Each file describes a specialized agent: when to invoke it, how it should behave, and what format to return results in.

When a task matches a subagent's trigger conditions, delegate that part of the work to the subagent rather than handling it inline.

Available subagents:

- `agents/docs-explorer.md` — looks up documentation for any external library, framework, or tool
